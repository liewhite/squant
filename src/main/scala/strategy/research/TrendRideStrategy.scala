package strategy.research

import hft.strategy.{OutcomeEvent, Strategy}

import hft.domain.*
import hft.exchange.SubscriptionKind
import hft.indicator.{KlineSeries, TrendConviction}
import hft.messaging.{EventData, IncomeEvent, PendingOrder, StateManager}
import strategy.research.TrendRideLogic.{Desired, Exec, Params}

import scala.collection.mutable

/** 趋势骑乘策略 (trade-native, taker 强制风控)。
  *
  * 核心：**仓位 = 信念的连续表达**。多周期 drift 带噪比加权出信念 C∈[-1,1]，叠加短周期 stretch 的均值回归，
  * 经信念安全带钳住，得到连续目标净仓 (见 [[TrendRideLogic]])。每一拍把当前仓驱动向目标：
  *   - 顺势加仓 / 超买超卖部分止盈 → maker 分批 (耐心吃价差，挂被动价等回调/反弹成交)。
  *   - 逆势超限 / 趋势翻向 → taker 立即吃单拍平、必要时反手 —— 故**绝不攒出大规模逆势仓**。
  *
  * 撮合对齐：taker (市价) 需 L1 盘口，故回测须用 [[hft.backtest.TradeBboAugmentSource]] 在 trade 后补零价差
  * BBO；maker 限价由真实成交价穿越成交。系数由回测标定，见 app.backtest.TrendRideBacktest。
  */
final class TrendRideStrategy(
    exchange: Exchange,
    symbol: Symbol,
    barIntervalMs: Long = 3_600_000L,
    /** (周期 bars, 权重)：默认 1d/3d/7d, 长周期权重大 */
    horizonsBars: Seq[(Int, Double)] = Seq((24, 0.25), (72, 0.35), (168, 0.40)),
    volPeriodBars: Int = 72,
    anchorBars: Int = 6,
    kSnrP: Double = 1.5,
    mMax: Double = 12.0,
    rMax: Double = 3.0,
    stepQty: Quantity = 1.5,
    band: Double = 0.5,
    adverseCap: Double = 0.5,
    convDead: Double = 0.05,
    passiveOffset: Double = 0.0008,
    priceTol: Double = 0.0015,
    mrTrendDecay: Double = 0.0,
    trendEntryTaker: Boolean = false,
) extends Strategy:

  private val params =
    Params(mMax, rMax, stepQty, band, adverseCap, convDead, passiveOffset, priceTol, mrTrendDecay, trendEntryTaker)
  private val maxH = horizonsBars.map(_._1).max

  private val klines =
    new KlineSeries(barIntervalMs, maxH + volPeriodBars + 4) with TrendConviction:
      override protected def horizons: Seq[(Int, Double)] = horizonsBars
      override protected def volPeriod: Int = volPeriodBars
      override protected def anchorPeriod: Int = anchorBars
      override protected def kSnr: Double = kSnrP

  /** 已发撤单、等待 Cancelled 确认 (避免重复撤单) */
  private val cancelling = mutable.Set.empty[OrderId]

  override def publicStreams: Map[Exchange, Set[SubscriptionKind]] =
    Map(exchange -> Set(SubscriptionKind.Trade(symbol)))

  override def orderTimeoutMs: Long = 5000

  override def onEvent(event: IncomeEvent, state: StateManager): Vector[OutcomeEvent] =
    event.data match
      case EventData.MarketTradeUpdate(t) if t.exchange == exchange && t.symbol == symbol =>
        klines.update(t.timestamp, t.price, t.qty)
        reconcile(t.price, state)
      case _ => Vector.empty

  private def reconcile(price: Price, state: StateManager): Vector[OutcomeEvent] =
    state.symbolState(symbol) match
      case None => Vector.empty
      case Some(ss) =>
        cancelling.filterInPlace(id => ss.pendingOrders.exists(_.order.id == id))
        (klines.conviction, klines.sigmaPerBar, klines.anchor) match
          case (Some(c), Some(sigma), Some(anchor)) =>
            val pos = ss.positionSize(exchange)
            val z = TrendRideLogic.stretchZ(price, anchor, sigma, anchorBars)
            val tgt = TrendRideLogic.target(c, z, params)
            buildActions(TrendRideLogic.desired(pos, tgt, c, price, params), ss, pos, c, z, tgt)
          case _ => Vector.empty // 预热不足 → 不动作

  /** 把"期望单"与现有挂单对账, 产出本拍动作 (撤单 + 至多一张新单)。对账决策是纯函数 [[TrendRideLogic.plan]],
    * 本方法只负责 PendingOrder↔Resting 的映射、撤单去重标记、动作→事件的翻译。 */
  private def buildActions(
      want: Option[Desired],
      ss: hft.messaging.SymbolState,
      pos: Double,
      c: Double,
      z: Double,
      tgt: Double,
  ): Vector[OutcomeEvent] =
    val resting = ss.pendingOrders.toVector.map(toResting)
    val plan = TrendRideLogic.plan(want, resting, cancelling.contains, params.priceTol)
    plan.cancelIds.foreach(cancelling += _)
    val cancels = plan.cancelIds.map(id => OutcomeEvent.CancelOrder(exchange, symbol, id))
    cancels ++ plan.place.map(place(_, pos, c, z, tgt))

  private def toResting(p: PendingOrder): TrendRideLogic.Resting =
    val (isLimit, px) = p.order.orderType match
      case OrderType.Limit(price, _) => (true, price)
      case OrderType.Market          => (false, 0.0)
    TrendRideLogic.Resting(p.order.id, p.order.side, p.order.reduceOnly, isLimit, px, p.status.isConfirmed)

  private def place(d: Desired, pos: Double, c: Double, z: Double, tgt: Double): OutcomeEvent =
    val ot = d.exec match
      case Exec.Maker => OrderType.Limit(d.price, TimeInForce.GTC)
      case Exec.Taker => OrderType.Market
    val kind = if d.reduceOnly then "reduce" else "add"
    OutcomeEvent.PlaceOrders(
      Vector(mkOrder(d.side, ot, d.qty, d.reduceOnly)),
      f"trendride:${d.exec} $kind ${d.side} qty=${d.qty}%.3f pos=$pos%.3f tgt=$tgt%.3f C=$c%+.2f z=$z%+.2f",
    )

  private def mkOrder(side: Side, ot: OrderType, qty: Quantity, reduceOnly: Boolean): Order =
    Order(id = "", exchange = exchange, symbol = symbol, side = side, orderType = ot, quantity = qty, reduceOnly = reduceOnly, clientOrderId = "")
