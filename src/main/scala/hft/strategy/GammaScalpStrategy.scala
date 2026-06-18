package hft.strategy

import hft.domain.*
import hft.exchange.SubscriptionKind
import hft.indicator.KlineSeries
import hft.messaging.{EventData, IncomeEvent, PendingOrder, StateManager, SymbolState}

import scala.collection.mutable

/** gamma scalping 策略 (long-gamma 的 maker 对冲，带 K 线 MACD 方向性间距偏移)。
  *
  * **trade-native，不依赖盘口**：只订阅逐笔 [[SubscriptionKind.Trade]]，以**最新真实成交价**为基准挂单。
  * 持有期权 (希腊字母经 [[EventData.GreeksUpdate]] 推送：实盘 OKX 轮询 / 回测 BS 合成)，在永续上
  * **post-only 挂单**把账户净 delta 维持在对称容忍带 [-deltaBand, +deltaBand] 内：
  *   - 净 delta > band (偏多)：在最新价上方挂 PostOnly 卖单，价格继续上行 (真实成交越价) 时成交 (卖高)；
  *   - 净 delta < -band (偏空)：在最新价下方挂 PostOnly 买单，价格继续下行时成交 (买低)。
  *
  * 这即 long-gamma 的 maker 对冲：价格波动驱动期权 delta 漂移、越带触发逐次 rehedge，maker 成交
  * 赚取波动 (毛 gamma 收益)，成本为手续费 (期权 theta 由 demo 侧 BS 估值单独计入期权腿)。
  *
  * **方向性间距**：逐笔成交喂入 [[KlineSeries]] 维护 K 线与 MACD(12,26,9)，柱>0 看多 / <0 看空。
  * 对冲间距 = 基础 [[baseOffsetRatio]] ± [[dirSkewRatio]]：顺势侧挂更远 (让该方向 delta 多跑)、
  * 逆势侧挂更近 (尽快对冲)。轻量方向 overlay 的最小形态。
  *
  * 净 delta = 期权 delta (greeks，已含现货 cashBal 修正) + 永续持仓。对冲是**降低 |净 delta|** 的动作，
  * 永续仓位天然被期权 delta 量级界定，无 runaway，不设杠杆上限。同一时刻最多一张在途对冲单，
  * 挂单**不追价** (静置等成交以兑现固定对冲间距)，仅方向翻转/数量显著漂移/回带内时撤单。
  * onEvent 由框架保证单线程串行调用，内部可变状态无需同步。策略层 size 恒为币本位。
  */
final class GammaScalpStrategy(
    /** 永续对冲场所 + greeks 标记的交易所 (回测中二者一致) */
    exchange: Exchange,
    /** 永续 symbol, e.g. "ETHUSDT" */
    symbol: Symbol,
    /** greeks 币种, e.g. "ETH" */
    ccy: String,
    /** 净 delta 对称容忍带 (币本位)：|净 delta| 超过即对冲回中性 */
    deltaBand: Double,
    /** 基础对冲间距 (PostOnly 挂单距最新成交价的偏移)，0.002 = 0.2% */
    baseOffsetRatio: Double = 0.002,
    /** 方向性偏移幅度 (按 K 线 MACD 方向调整)，0.0005 = 0.05%。顺势侧挂更远、逆势侧挂更近 */
    dirSkewRatio: Double = 0.0005,
    /** 对冲数量相对目标漂移超该比例则撤单重挂 */
    qtyToleranceRatio: Double = 0.2,
    /** 最小对冲数量 (币本位)，低于此不挂单避免碎单 */
    minHedgeQty: Quantity = 0.001,
    /** K 线 MACD 参数 (由逐笔 trade 聚合)，用于方向偏移 */
    barIntervalMs: Long = 3_600_000,
    maxBars: Int = 200,
    macdFast: Int = 12,
    macdSlow: Int = 26,
    macdSignal: Int = 9,
) extends Strategy:

  /** 已发出撤单、尚未确认移除的订单，防止重复撤单 */
  private val cancelling = mutable.Set.empty[OrderId]

  /** 由逐笔 trade 聚合 K 线算 MACD，柱>0 看多 / <0 看空 -> 调整对冲间距方向偏移 */
  private val klines = KlineSeries(barIntervalMs, maxBars, macdFast, macdSlow, macdSignal)

  override def publicStreams: Map[Exchange, Set[SubscriptionKind]] =
    Map(exchange -> Set(SubscriptionKind.Trade(symbol)))
  // greeks 为广播事件 (账户级)，无需订阅，StateManager 自动维护，按 ccy 查询

  override def orderTimeoutMs: Long = 5000

  override def onEvent(event: IncomeEvent, state: StateManager): Vector[OutcomeEvent] =
    event.data match
      // 逐笔成交：喂 K 线 (方向信号) + 以最新成交价为基准评估对冲
      case EventData.MarketTradeUpdate(t) if t.exchange == exchange && t.symbol == symbol =>
        klines.update(t.timestamp, t.price, t.qty)
        hedge(t.price, state)
      // greeks 变化 (delta 漂移) 也即时触发，用 state 中最新成交价为基准
      case EventData.GreeksUpdate(g) if g.exchange == exchange && g.ccy == ccy =>
        state.symbolState(symbol).flatMap(_.lastTradePrice(exchange)).map(hedge(_, state)).getOrElse(Vector.empty)
      case _ => Vector.empty

  private def hedge(refPrice: Price, state: StateManager): Vector[OutcomeEvent] =
    (for
      symbolState <- state.symbolState(symbol)
      greeks <- state.greeks(exchange, ccy) // greeks 与 cashBal 均到达才动作 (delta 修正就绪)
    yield
      cancelling.filterInPlace(id => symbolState.pendingOrders.exists(_.order.id == id))
      val netDelta = greeks.delta + symbolState.positionSize(exchange)
      desiredHedge(refPrice, netDelta) match
        case None                   => withinBand(symbolState)
        case Some(side, qty, price) => maintain(symbolState, side, qty, price, netDelta)
    ).getOrElse(Vector.empty)

  /** 越带的目标对冲：把净 delta 拉回中性。偏多 -> 最新价上方挂卖；偏空 -> 最新价下方挂买。
    *
    * 对冲间距按 K 线 MACD 方向偏移：顺势侧挂更远、逆势侧挂更近。
    * dir=+1(看多): 卖间距 base+skew (更远)、买间距 base-skew (更近)；dir=-1(看空) 镜像。
    */
  private def desiredHedge(refPrice: Price, netDelta: Double): Option[(Side, Quantity, Price)] =
    val dir = klines.macdDirection
    // 偏移下限 0：dirSkewRatio > baseOffsetRatio 时不致挂到价格另一侧而立即 would-take
    val sellOffset = math.max(0.0, baseOffsetRatio + dir * dirSkewRatio)
    val buyOffset = math.max(0.0, baseOffsetRatio - dir * dirSkewRatio)
    if netDelta > deltaBand then Some((Side.Short, netDelta, refPrice * (1 + sellOffset)))
    else if netDelta < -deltaBand then Some((Side.Long, -netDelta, refPrice * (1 - buyOffset)))
    else None

  /** 在带内：无需对冲，撤掉残留的在途对冲单 */
  private def withinBand(symbolState: SymbolState): Vector[OutcomeEvent] =
    symbolState.pendingOrders.headOption.flatMap(cancelConfirmed).toVector

  /** 维护单张对冲单。**不追价**：挂单按固定间距静置等成交 (兑现"对冲间距=固定值"的语义，
    * 否则随最新价追价会让挂单在趋势里永远填不上)。仅在方向翻转或数量显著漂移时撤单重挂。
    */
  private def maintain(
      symbolState: SymbolState,
      side: Side,
      qty: Quantity,
      price: Price,
      netDelta: Double,
  ): Vector[OutcomeEvent] =
    symbolState.pendingOrders.headOption match
      case Some(pending) =>
        pending.order.orderType match
          case OrderType.Limit(_, _) =>
            val sideChanged = pending.order.side != side
            val qtyDrift = math.abs(pending.order.quantity - qty) / qty > qtyToleranceRatio
            if sideChanged || qtyDrift then cancelConfirmed(pending).toVector else Vector.empty
          case OrderType.Market => Vector.empty
      case None =>
        if qty < minHedgeQty then Vector.empty
        else
          Vector(
            OutcomeEvent.PlaceOrders(
              Vector(
                Order(
                  id = "",
                  exchange = exchange,
                  symbol = symbol,
                  side = side,
                  orderType = OrderType.Limit(price, TimeInForce.PostOnly),
                  quantity = qty,
                  reduceOnly = false,
                  clientOrderId = "",
                )
              ),
              f"gamma_hedge | $side%s netDelta=$netDelta%.4f qty=$qty%.4f px=$price%.4f dir=${klines.macdDirection}",
            )
          )

  /** 撤掉已确认挂单 (Created 无 orderId 不能撤，等确认)；防止重复撤单 */
  private def cancelConfirmed(pending: PendingOrder): Option[OutcomeEvent] =
    if pending.status.isConfirmed && !cancelling.contains(pending.order.id) then
      cancelling += pending.order.id
      Some(OutcomeEvent.CancelOrder(exchange, symbol, pending.order.id))
    else None
