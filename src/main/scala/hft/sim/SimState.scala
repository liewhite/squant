package hft.sim

import hft.domain.*
import hft.messaging.{EventData, IncomeEvent}

/** 成交的流动性角色：maker (resting 单被越价成交) / taker (到达即吃单成交)，决定手续费率 */
enum Liquidity:
  case Maker
  case Taker

/** 挂单簿中的一张挂单。
  *
  * @param seq 入簿到达序号 (单调递增)，用于撮合的时间优先排序 (FIFO)，杜绝按 HashMap 哈希序撮合。
  */
final case class RestingOrder(
    orderId: OrderId,
    clientOrderId: String,
    symbol: Symbol,
    side: Side,
    limitPrice: Price,
    quantity: Quantity,
    reduceOnly: Boolean,
    seq: Long,
):
  /** 价格优先级排序键 (越激进、越该先成交者越小)：买单出价越高越激进 -> -limitPrice；
    * 卖单要价越低越激进 -> +limitPrice。与 [[seq]] 组成 (价格, 时间) 优先级。 */
  def pricePriority: Double = side match
    case Side.Long  => -limitPrice
    case Side.Short => limitPrice

/** 虚拟柜台的全部状态 (账本 + 挂单簿 + 最新行情)，不可变。
  *
  * 所有撮合逻辑都是 [[SimState]] 上的确定性转移 `(SimState, 命令, now) => (SimState, 回流事件)`：
  * 当前时刻 `now` 由驱动层注入 (回测=虚拟时间, 实盘=墙钟)，回流事件的交易所/本地时间戳一律取
  * 该 `now`——转移内**不读墙钟**，故同一输入必得同一结果，脱离线程/锁/延迟即可同步单测。
  * [[SimulatedExchange]] 仅作为单 actor 的薄壳：把命令串行喂给这些转移函数、并把回流事件按延迟投递给策略。
  *
  * @param restingSeq 下一张入簿挂单的到达序号，撮合按 (价格, 到达序) 优先级排序 (FIFO)。
  */
final case class SimState(
    ledger: Ledger,
    resting: Map[OrderId, RestingOrder],
    lastBbo: Map[Symbol, BBO],
    lastMark: Map[Symbol, Double],
    lastTrade: Map[Symbol, Double] = Map.empty,
    makerFeeRate: Double = 0.0,
    takerFeeRate: Double = 0.0,
    restingSeq: Long = 0L,
):
  /** 估值价格：标记价 > BBO 中间价 > 最新成交价 (trade-only 行情用最新成交价估值) */
  def markOf(symbol: Symbol): Double =
    lastMark
      .get(symbol)
      .orElse(lastBbo.get(symbol).map(_.midPrice))
      .orElse(lastTrade.get(symbol))
      .getOrElse(0.0)

  // ==================== 上游行情到达 (实时, 用于撮合) ====================

  /** 行情到达交易所：更新行情 + 撮合越价挂单。返回 (新状态, 要回流策略的事件)。
    * 第一个事件即转发给策略的行情, 其后是本次行情触发的成交回报 (保证行情先于成交)。
    * `now` 为当前时刻 (回测虚拟时间 / 实盘墙钟)，成交回报的时间戳取自它。
    */
  def onMarket(exchange: Exchange, ev: IncomeEvent, now: Timestamp): (SimState, Vector[IncomeEvent]) =
    ev.data match
      case EventData.BboUpdate(bbo) =>
        val withBbo = copy(lastBbo = lastBbo.updated(bbo.symbol, bbo))
        val (next, fills) = withBbo.matchCrossing(exchange, bbo, now)
        (next, ev +: fills)
      case EventData.MarkPriceUpdate(mp) =>
        (copy(lastMark = lastMark.updated(mp.symbol, mp.price)), Vector(ev))
      case EventData.MarketTradeUpdate(t) =>
        // trade-print 撮合：真实成交价严格越过挂单价即成交 (无 bbo 行情时的撮合来源)
        val withTrade = copy(lastTrade = lastTrade.updated(t.symbol, t.price))
        val (next, fills) = withTrade.matchTrade(exchange, t, now)
        (next, ev +: fills)
      case _ => (this, Vector(ev))

  /** 多张挂单同刻越价时的成交顺序：价格-时间优先 (更激进者先成交, 同价按到达序 FIFO)，
    * 而非 HashMap 哈希序——影响多张 reduceOnly 竞争同一持仓时的成交分配。 */
  private def matchOrder(crossed: Iterable[RestingOrder]): Vector[RestingOrder] =
    crossed.toVector.sortBy(o => (o.pricePriority, o.seq))

  /** BBO 越过挂单价的全部挂单成交 (maker 成交价取挂单价) */
  private def matchCrossing(exchange: Exchange, bbo: BBO, now: Timestamp): (SimState, Vector[IncomeEvent]) =
    val crossed = matchOrder(resting.values.filter(o => o.symbol == bbo.symbol && Matcher.crosses(o.side, o.limitPrice, bbo)))
    crossed.foldLeft((this, Vector.empty[IncomeEvent])) { case ((st, evs), o) =>
      val (next, fillEvs) = st
        .copy(resting = st.resting - o.orderId)
        .fill(exchange, o.orderId, o.clientOrderId, o.symbol, o.side, o.limitPrice, o.quantity, now, Liquidity.Maker, o.reduceOnly)
      (next, evs ++ fillEvs)
    }

  /** 真实成交严格越过挂单价的全部挂单成交 (maker 成交价取挂单价) */
  private def matchTrade(exchange: Exchange, t: MarketTrade, now: Timestamp): (SimState, Vector[IncomeEvent]) =
    val crossed = matchOrder(resting.values.filter(o => o.symbol == t.symbol && Matcher.tradeCrosses(o.side, o.limitPrice, t.price)))
    crossed.foldLeft((this, Vector.empty[IncomeEvent])) { case ((st, evs), o) =>
      val (next, fillEvs) = st
        .copy(resting = st.resting - o.orderId)
        .fill(exchange, o.orderId, o.clientOrderId, o.symbol, o.side, o.limitPrice, o.quantity, now, Liquidity.Maker, o.reduceOnly)
      (next, evs ++ fillEvs)
    }

  // ==================== 下单到达撮合 ====================

  /** 订单到达撮合：按类型/TIF 决定 resting / 成交 / 拒单。`now` 为到达时刻 (回报时间戳取自它) */
  def onOrderArrived(exchange: Exchange, order: Order, orderId: OrderId, now: Timestamp): (SimState, Vector[IncomeEvent]) =
    val bboOpt = lastBbo.get(order.symbol)
    order.orderType match
      case OrderType.Market =>
        bboOpt match
          case Some(bbo) =>
            fill(exchange, orderId, order.clientOrderId, order.symbol, order.side, Matcher.touchPrice(order.side, bbo), order.quantity, now, Liquidity.Taker, order.reduceOnly)
          case None =>
            (this, Vector(statusEvent(exchange, order, orderId, OrderStatus.Rejected("no market data for market order"), 0.0, now)))
      case OrderType.Limit(limit, tif) =>
        // 到达即可成交时的对手价 (None = 不可成交)。可成交性与 resting 越价用同一判定
        // (Matcher.crosses) 二者自洽；价与可成交性同源, 无需 .get
        val takerPrice: Option[Price] =
          bboOpt.filter(Matcher.crosses(order.side, limit, _)).map(Matcher.touchPrice(order.side, _))
        def takerFill(price: Price) = fill(exchange, orderId, order.clientOrderId, order.symbol, order.side, price, order.quantity, now, Liquidity.Taker, order.reduceOnly)
        tif match
          case TimeInForce.PostOnly =>
            takerPrice match
              case Some(_) => (this, Vector(statusEvent(exchange, order, orderId, OrderStatus.Rejected("post-only would take liquidity"), limit, now)))
              case None    => rest(exchange, order, orderId, limit, now)
          case TimeInForce.GTC =>
            takerPrice match
              case Some(p) => takerFill(p)
              case None    => rest(exchange, order, orderId, limit, now)
          case TimeInForce.IOC | TimeInForce.FOK =>
            // 无深度模型, 可成交即全量成交, 否则整单取消 (不 resting)
            takerPrice match
              case Some(p) => takerFill(p)
              case None    => (this, Vector(statusEvent(exchange, order, orderId, OrderStatus.Cancelled, limit, now)))

  /** 撤单到达撮合：仍在簿则移除并回报 Cancelled；已成交 (不在簿) 则无事发生。`now` 为到达时刻 */
  def onCancelArrived(exchange: Exchange, orderId: OrderId, now: Timestamp): (SimState, Vector[IncomeEvent]) =
    resting.get(orderId) match
      case Some(o) =>
        val ev = IncomeEvent(
          now,
          now,
          EventData.OrderUpdated(
            OrderUpdate(orderId, Some(o.clientOrderId), exchange, o.symbol, o.side, OrderStatus.Cancelled, o.limitPrice, o.quantity, 0.0, 0.0, now)
          ),
        )
        (copy(resting = resting - orderId), Vector(ev))
      case None => (this, Vector.empty)

  // ==================== 私有构造 ====================

  private def rest(exchange: Exchange, order: Order, orderId: OrderId, limit: Price, now: Timestamp): (SimState, Vector[IncomeEvent]) =
    val ro = RestingOrder(orderId, order.clientOrderId, order.symbol, order.side, limit, order.quantity, order.reduceOnly, restingSeq)
    (copy(resting = resting.updated(orderId, ro), restingSeq = restingSeq + 1), Vector(statusEvent(exchange, order, orderId, OrderStatus.Pending, limit, now)))

  /** 成交落账。reduceOnly 单按当前持仓截断 (卖只平多、买只平空)：撮合层强制不反向开仓，
    * 与真实交易所一致——故策略对 reduceOnly 的"只减不增"可信赖，不必自行截断 qty (但仍可截断以省单)。
    * 无可平仓位时不成交，回 Cancelled。
    */
  private def fill(
      exchange: Exchange,
      orderId: OrderId,
      clientOrderId: String,
      symbol: Symbol,
      side: Side,
      fillPrice: Price,
      qty: Quantity,
      now: Timestamp,
      liquidity: Liquidity,
      reduceOnly: Boolean,
  ): (SimState, Vector[IncomeEvent]) =
    val effectiveQty =
      if !reduceOnly then qty
      else
        val posSize = ledger.positions.get(symbol).map(_.size).getOrElse(0.0)
        side match
          case Side.Short => math.min(qty, math.max(0.0, posSize))  // 卖平多: 至多平掉现有多头
          case Side.Long  => math.min(qty, math.max(0.0, -posSize)) // 买平空: 至多平掉现有空头
    if reduceOnly && effectiveQty <= Position.Epsilon then
      // reduceOnly 无可平仓位 -> 不成交，回 Cancelled (订单已被调用方移出簿 / 不入簿)
      val update = OrderUpdate(orderId, Some(clientOrderId), exchange, symbol, side, OrderStatus.Cancelled, fillPrice, qty, 0.0, 0.0, now)
      (this, Vector(IncomeEvent(now, now, EventData.OrderUpdated(update))))
    else
      val feeRate = liquidity match
        case Liquidity.Maker => makerFeeRate
        case Liquidity.Taker => takerFeeRate
      val fee = fillPrice * effectiveQty * feeRate
      val next = copy(ledger = ledger.applyFill(exchange, symbol, side, fillPrice, effectiveQty, fee))
      val update = OrderUpdate(orderId, Some(clientOrderId), exchange, symbol, side, OrderStatus.Filled, fillPrice, effectiveQty, effectiveQty, effectiveQty, now)
      val f = Fill(exchange, symbol, side, fillPrice, effectiveQty, now)
      (next, Vector(IncomeEvent(now, now, EventData.OrderUpdated(update)), IncomeEvent(now, now, EventData.FillUpdate(f))))

  private def statusEvent(exchange: Exchange, order: Order, orderId: OrderId, status: OrderStatus, price: Price, now: Timestamp): IncomeEvent =
    IncomeEvent(
      now,
      now,
      EventData.OrderUpdated(
        OrderUpdate(orderId, Some(order.clientOrderId), exchange, order.symbol, order.side, status, price, order.quantity, 0.0, 0.0, now)
      ),
    )

object SimState:
  def empty(cash: Double, makerFeeRate: Double = 0.0, takerFeeRate: Double = 0.0): SimState =
    SimState(Ledger.empty(cash), Map.empty, Map.empty, Map.empty, Map.empty, makerFeeRate, takerFeeRate)
