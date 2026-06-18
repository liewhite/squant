package hft.sim

import hft.domain.*
import hft.messaging.{EventData, IncomeEvent}

/** 成交的流动性角色：maker (resting 单被越价成交) / taker (到达即吃单成交)，决定手续费率 */
enum Liquidity:
  case Maker
  case Taker

/** 挂单簿中的一张挂单 */
final case class RestingOrder(
    orderId: OrderId,
    clientOrderId: String,
    symbol: Symbol,
    side: Side,
    limitPrice: Price,
    quantity: Quantity,
)

/** 虚拟柜台的全部状态 (账本 + 挂单簿 + 最新行情)，不可变。
  *
  * 所有撮合逻辑都是 [[SimState]] 上的确定性转移 `(SimState, 命令) => (SimState, 回流事件)`
  * (仅回流事件的本地时间戳取自 nowMs，不影响撮合与状态)，脱离线程/锁/延迟即可同步单测。
  * [[SimulatedExchange]] 仅作为单 actor 的薄壳：把命令串行喂给这些转移函数、并把回流事件按延迟投递给策略。
  */
final case class SimState(
    ledger: Ledger,
    resting: Map[OrderId, RestingOrder],
    lastBbo: Map[Symbol, BBO],
    lastMark: Map[Symbol, Double],
    makerFeeRate: Double = 0.0,
    takerFeeRate: Double = 0.0,
):
  /** 估值价格：优先标记价格，退化为 BBO 中间价 */
  def markOf(symbol: Symbol): Double =
    lastMark.getOrElse(symbol, lastBbo.get(symbol).map(_.midPrice).getOrElse(0.0))

  // ==================== 上游行情到达 (实时, 用于撮合) ====================

  /** 行情到达交易所：更新行情 + 撮合越价挂单。返回 (新状态, 要回流策略的事件)。
    * 第一个事件即转发给策略的行情, 其后是本次行情触发的成交回报 (保证行情先于成交)。
    */
  def onMarket(exchange: Exchange, ev: IncomeEvent): (SimState, Vector[IncomeEvent]) =
    ev.data match
      case EventData.BboUpdate(bbo) =>
        val withBbo = copy(lastBbo = lastBbo.updated(bbo.symbol, bbo))
        val (next, fills) = withBbo.matchCrossing(exchange, bbo)
        (next, ev +: fills)
      case EventData.MarkPriceUpdate(mp) =>
        (copy(lastMark = lastMark.updated(mp.symbol, mp.price)), Vector(ev))
      case _ => (this, Vector(ev))

  /** BBO 越过挂单价的全部挂单成交 (maker 成交价取挂单价) */
  private def matchCrossing(exchange: Exchange, bbo: BBO): (SimState, Vector[IncomeEvent]) =
    val crossed = resting.values.filter(o => o.symbol == bbo.symbol && Matcher.crosses(o.side, o.limitPrice, bbo)).toVector
    crossed.foldLeft((this, Vector.empty[IncomeEvent])) { case ((st, evs), o) =>
      val (next, fillEvs) = st
        .copy(resting = st.resting - o.orderId)
        .fill(exchange, o.orderId, o.clientOrderId, o.symbol, o.side, o.limitPrice, o.quantity, bbo.timestamp, Liquidity.Maker)
      (next, evs ++ fillEvs)
    }

  // ==================== 下单到达撮合 ====================

  /** 订单到达撮合：按类型/TIF 决定 resting / 成交 / 拒单 */
  def onOrderArrived(exchange: Exchange, order: Order, orderId: OrderId): (SimState, Vector[IncomeEvent]) =
    val bboOpt = lastBbo.get(order.symbol)
    val ts = bboOpt.map(_.timestamp).getOrElse(nowMs)
    order.orderType match
      case OrderType.Market =>
        bboOpt match
          case Some(bbo) =>
            fill(exchange, orderId, order.clientOrderId, order.symbol, order.side, Matcher.touchPrice(order.side, bbo), order.quantity, ts, Liquidity.Taker)
          case None =>
            (this, Vector(statusEvent(exchange, order, orderId, OrderStatus.Rejected("no market data for market order"), 0.0, ts)))
      case OrderType.Limit(limit, tif) =>
        // 到达即可成交时的对手价 (None = 不可成交)。可成交性与 resting 越价用同一判定
        // (Matcher.crosses) 二者自洽；价与可成交性同源, 无需 .get
        val takerPrice: Option[Price] =
          bboOpt.filter(Matcher.crosses(order.side, limit, _)).map(Matcher.touchPrice(order.side, _))
        def takerFill(price: Price) = fill(exchange, orderId, order.clientOrderId, order.symbol, order.side, price, order.quantity, ts, Liquidity.Taker)
        tif match
          case TimeInForce.PostOnly =>
            takerPrice match
              case Some(_) => (this, Vector(statusEvent(exchange, order, orderId, OrderStatus.Rejected("post-only would take liquidity"), limit, ts)))
              case None    => rest(exchange, order, orderId, limit, ts)
          case TimeInForce.GTC =>
            takerPrice match
              case Some(p) => takerFill(p)
              case None    => rest(exchange, order, orderId, limit, ts)
          case TimeInForce.IOC | TimeInForce.FOK =>
            // 无深度模型, 可成交即全量成交, 否则整单取消 (不 resting)
            takerPrice match
              case Some(p) => takerFill(p)
              case None    => (this, Vector(statusEvent(exchange, order, orderId, OrderStatus.Cancelled, limit, ts)))

  /** 撤单到达撮合：仍在簿则移除并回报 Cancelled；已成交 (不在簿) 则无事发生 */
  def onCancelArrived(exchange: Exchange, orderId: OrderId): (SimState, Vector[IncomeEvent]) =
    resting.get(orderId) match
      case Some(o) =>
        val ev = IncomeEvent.at(
          nowMs,
          EventData.OrderUpdated(
            OrderUpdate(orderId, Some(o.clientOrderId), exchange, o.symbol, o.side, OrderStatus.Cancelled, o.limitPrice, o.quantity, 0.0, 0.0, nowMs)
          ),
        )
        (copy(resting = resting - orderId), Vector(ev))
      case None => (this, Vector.empty)

  // ==================== 私有构造 ====================

  private def rest(exchange: Exchange, order: Order, orderId: OrderId, limit: Price, ts: Timestamp): (SimState, Vector[IncomeEvent]) =
    val ro = RestingOrder(orderId, order.clientOrderId, order.symbol, order.side, limit, order.quantity)
    (copy(resting = resting.updated(orderId, ro)), Vector(statusEvent(exchange, order, orderId, OrderStatus.Pending, limit, ts)))

  private def fill(
      exchange: Exchange,
      orderId: OrderId,
      clientOrderId: String,
      symbol: Symbol,
      side: Side,
      fillPrice: Price,
      qty: Quantity,
      ts: Timestamp,
      liquidity: Liquidity,
  ): (SimState, Vector[IncomeEvent]) =
    val feeRate = liquidity match
      case Liquidity.Maker => makerFeeRate
      case Liquidity.Taker => takerFeeRate
    val fee = fillPrice * qty * feeRate
    val next = copy(ledger = ledger.applyFill(exchange, symbol, side, fillPrice, qty, fee))
    val update = OrderUpdate(orderId, Some(clientOrderId), exchange, symbol, side, OrderStatus.Filled, fillPrice, qty, qty, qty, ts)
    val f = Fill(exchange, symbol, side, fillPrice, qty, ts)
    (next, Vector(IncomeEvent.at(ts, EventData.OrderUpdated(update)), IncomeEvent.at(ts, EventData.FillUpdate(f))))

  private def statusEvent(exchange: Exchange, order: Order, orderId: OrderId, status: OrderStatus, price: Price, ts: Timestamp): IncomeEvent =
    IncomeEvent.at(
      ts,
      EventData.OrderUpdated(
        OrderUpdate(orderId, Some(order.clientOrderId), exchange, order.symbol, order.side, status, price, order.quantity, 0.0, 0.0, ts)
      ),
    )

object SimState:
  def empty(cash: Double, makerFeeRate: Double = 0.0, takerFeeRate: Double = 0.0): SimState =
    SimState(Ledger.empty(cash), Map.empty, Map.empty, Map.empty, makerFeeRate, takerFeeRate)
