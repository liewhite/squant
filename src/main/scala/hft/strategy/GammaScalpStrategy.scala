package hft.strategy

import hft.domain.*
import hft.exchange.SubscriptionKind
import hft.messaging.{EventData, IncomeEvent, PendingOrder, StateManager, SymbolState}

import scala.collection.mutable

/** 纯 gamma scalping 策略 (无方向/波动率观点的对称对冲)。
  *
  * 持有期权 (希腊字母经 [[EventData.GreeksUpdate]] 推送：实盘 OKX 轮询 / 回测 BS 合成)，通过在永续上
  * **post-only 挂单**把账户净 delta 维持在对称容忍带 [-deltaBand, +deltaBand] 内：
  *   - 净 delta > band (偏多)：在 ask 上方挂 PostOnly 卖单，价格继续上行时成交 (卖高)，把 delta 拉回；
  *   - 净 delta < -band (偏空)：在 bid 下方挂 PostOnly 买单，价格继续下行时成交 (买低)，把 delta 拉回。
  *
  * 这即 long-gamma 的 maker 对冲：价格波动驱动期权 delta 漂移、越带触发逐次 rehedge，maker 成交
  * 赚取波动 (毛 gamma 收益)，对冲成本为手续费 (撮合按 maker 费率计) —— 回测正是用来观察
  * "毛 scalp 收益 - 手续费" 的净期望 (期权 theta 为已知结构性成本，不在永续撮合 P&L 内建模)。
  *
  * 净 delta = 期权 delta (greeks，已含现货 cashBal 修正) + 永续持仓。对冲是**降低 |净 delta|** 的动作，
  * 永续仓位天然被期权 delta 量级界定 (对冲到中性即 perpPos ≈ -optionDelta)，无 runaway，故不设杠杆上限。
  *
  * 同一时刻最多一张在途对冲单 (节奏由订单生命周期限速)；目标价/数量漂移超容差即撤单重挂。
  * onEvent 由框架保证单线程串行调用，内部可变状态无需同步。
  *
  * 不变量：假设 contractSize 的张<->币换算由框架边界完成，策略层 size 恒为币本位。
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
    /** PostOnly 挂单距 BBO 的偏移比例 (保证只做 maker)，0.0002 = 0.02% */
    offsetRatio: Double = 0.0002,
    /** 挂单价偏离目标价超该比例则撤单重挂 */
    repriceToleranceRatio: Double = 0.0001,
    /** 对冲数量相对目标漂移超该比例则撤单重挂 */
    qtyToleranceRatio: Double = 0.2,
    /** 最小对冲数量 (币本位)，低于此不挂单避免碎单 */
    minHedgeQty: Quantity = 0.001,
) extends Strategy:

  /** 已发出撤单、尚未确认移除的订单，防止重复撤单 */
  private val cancelling = mutable.Set.empty[OrderId]

  override def publicStreams: Map[Exchange, Set[SubscriptionKind]] =
    Map(exchange -> Set(SubscriptionKind.BBO(symbol)))
  // greeks 为广播事件 (账户级)，无需订阅，StateManager 自动维护，按 ccy 查询

  override def orderTimeoutMs: Long = 5000

  override def onEvent(event: IncomeEvent, state: StateManager): Vector[OutcomeEvent] =
    event.data match
      case EventData.BboUpdate(bbo) if bbo.exchange == exchange && bbo.symbol == symbol =>
        hedge(bbo, state)
      // greeks 变化 (delta 漂移) 也即时触发对冲评估，用 state 中最新 BBO 报价
      case EventData.GreeksUpdate(g) if g.exchange == exchange && g.ccy == ccy =>
        state.symbolState(symbol).flatMap(_.bbo(exchange)).map(hedge(_, state)).getOrElse(Vector.empty)
      case _ => Vector.empty

  private def hedge(bbo: BBO, state: StateManager): Vector[OutcomeEvent] =
    (for
      symbolState <- state.symbolState(symbol)
      greeks <- state.greeks(exchange, ccy) // greeks 与 cashBal 均到达才动作 (delta 修正就绪)
    yield
      cancelling.filterInPlace(id => symbolState.pendingOrders.exists(_.order.id == id))
      val netDelta = greeks.delta + symbolState.positionSize(exchange)
      desiredHedge(bbo, netDelta) match
        case None              => withinBand(symbolState)
        case Some(side, qty, price) => maintain(symbolState, side, qty, price, netDelta)
    ).getOrElse(Vector.empty)

  /** 越带的目标对冲：把净 delta 拉回中性。偏多 -> ask 上方挂卖；偏空 -> bid 下方挂买 */
  private def desiredHedge(bbo: BBO, netDelta: Double): Option[(Side, Quantity, Price)] =
    if netDelta > deltaBand then Some((Side.Short, netDelta, bbo.askPrice * (1 + offsetRatio)))
    else if netDelta < -deltaBand then Some((Side.Long, -netDelta, bbo.bidPrice * (1 - offsetRatio)))
    else None

  /** 在带内：无需对冲，撤掉残留的在途对冲单 */
  private def withinBand(symbolState: SymbolState): Vector[OutcomeEvent] =
    symbolState.pendingOrders.headOption.flatMap(cancelConfirmed).toVector

  /** 维护单张对冲单：方向/价格/数量任一漂移即撤单重挂；无单则按风控挂新单 */
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
          case OrderType.Limit(restingPrice, _) =>
            val sideChanged = pending.order.side != side
            val priceDrift = math.abs(restingPrice - price) / price > repriceToleranceRatio
            val qtyDrift = math.abs(pending.order.quantity - qty) / qty > qtyToleranceRatio
            if sideChanged || priceDrift || qtyDrift then cancelConfirmed(pending).toVector else Vector.empty
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
              f"gamma_hedge | $side%s netDelta=$netDelta%.4f qty=$qty%.4f px=$price%.4f",
            )
          )

  /** 撤掉已确认挂单 (Created 无 orderId 不能撤，等确认)；防止重复撤单 */
  private def cancelConfirmed(pending: PendingOrder): Option[OutcomeEvent] =
    if pending.status.isConfirmed && !cancelling.contains(pending.order.id) then
      cancelling += pending.order.id
      Some(OutcomeEvent.CancelOrder(exchange, symbol, pending.order.id))
    else None
