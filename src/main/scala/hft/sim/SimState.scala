package hft.sim

import hft.domain.*
import hft.event.{AnyEvent, Event, Topics}

import scala.collection.mutable

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
    quantity: Coin,
    reduceOnly: Boolean,
    seq: Long,
):
  /** 价格优先级排序键 (越激进、越该先成交者越小)：买单出价越高越激进 -> -limitPrice；
    * 卖单要价越低越激进 -> +limitPrice。与 [[seq]] 组成 (价格, 时间) 优先级。 */
  def pricePriority: Double = side match
    case Side.Long  => -limitPrice.value
    case Side.Short => limitPrice.value

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
    /** 本柜台服务的账户 —— 撮合产出的回报都标它。实盘替身标 Live，影子盘标 Paper(n) */
    account: AccountId,
    ledger: Ledger,
    resting: Map[OrderId, RestingOrder],
    lastBbo: Map[Symbol, BBO],
    lastMark: Map[Symbol, Price],
    lastTrade: Map[Symbol, Price] = Map.empty,
    makerFeeRate: Double = 0.0,
    takerFeeRate: Double = 0.0,
    restingSeq: Long = 0L,
):
  /** 估值价格：标记价 > BBO 中间价 > 最新成交价 (trade-only 行情用最新成交价估值) */
  def markOf(symbol: Symbol): Price =
    lastMark
      .get(symbol)
      .orElse(lastBbo.get(symbol).map(_.midPrice))
      .orElse(lastTrade.get(symbol))
      .getOrElse(Price.Zero)

  // ==================== 上游行情到达 (实时, 用于撮合) ====================

  /** 行情到达交易所：更新行情快照 + 撮合被穿越的挂单。返回 (新状态, **撮合产生的**回报)。
    *
    * **不回显行情本身**。"把行情转发给策略"是扮演网关那一方的职责，不是撮合的 ——
    * 实盘替身要转发 (策略的行情只有它这一个来源)，影子盘不能转发 (策略直接从总线读真实行情，
    * 转了就是重复投递)。此前行情混在回报里返回，于是影子盘不得不再把它滤掉，
    * 一个本不属于撮合的事实污染了撮合的输出形态。现在谁转发谁自己发 (见 [[Counter]])。
    *
    * `now` 为当前时刻 (回测虚拟时间 / 实盘墙钟)，成交回报的时间戳取自它。
    */
  def onMarket(exchange: Exchange, ev: AnyEvent, now: Timestamp): (SimState, Vector[AnyEvent]) =
    ev.as(Topics.Bbo).map { bbo =>
      copy(lastBbo = lastBbo.updated(bbo.symbol, bbo)).matchCrossing(exchange, bbo, now)
    }.orElse(ev.as(Topics.MarkPrice).map { mp =>
      (copy(lastMark = lastMark.updated(mp.symbol, mp.price)), Vector.empty[AnyEvent])
    }).orElse(ev.as(Topics.Trade).map { t =>
      // 逐笔撮合：真实成交价严格穿越挂单价即成交 (与 BBO 穿越同一 maker 口径, 见 Matcher)
      copy(lastTrade = lastTrade.updated(t.symbol, t.price)).matchTrade(exchange, t, now)
    }).getOrElse((this, Vector.empty[AnyEvent]))

  /** 本柜台账户的读数快照 (估值口径见 [[markOf]])。
    * 回测、影子盘、实盘替身三处都取这一个 —— 见 [[Ledger.accountInfo]]。 */
  def accountInfo(exchange: Exchange): AccountInfo = ledger.accountInfo(exchange, markOf)

  /** 收集越价挂单, 按 (价格, 到达序) 优先级排序 (更激进者先成交, 同价 FIFO)，杜绝 HashMap 哈希序。
    * 单趟扫描 valuesIterator；无匹配 (绝大多数行情) 零分配快速返回；单张免排序。 */
  private def crossingOrders(pred: RestingOrder => Boolean): Vector[RestingOrder] =
    var buf: mutable.ArrayBuffer[RestingOrder] = null
    val it = resting.valuesIterator
    while it.hasNext do
      val o = it.next()
      if pred(o) then
        if buf == null then buf = mutable.ArrayBuffer.empty
        buf += o
    if buf == null then Vector.empty
    else if buf.length == 1 then Vector(buf.head)
    else buf.sortInPlaceBy(o => (o.pricePriority, o.seq)).toVector

  /** 把按优先级排好序的越价挂单依次成交 (maker 成交价取挂单价)。 */
  private def fillCrossed(exchange: Exchange, crossed: Vector[RestingOrder], now: Timestamp): (SimState, Vector[AnyEvent]) =
    if crossed.isEmpty then (this, Vector.empty)
    else
      crossed.foldLeft((this, Vector.empty[AnyEvent])) { case ((st, evs), o) =>
        val (next, fillEvs) = st
          .copy(resting = st.resting - o.orderId)
          .fill(exchange, o.orderId, o.clientOrderId, o.symbol, o.side, o.limitPrice, o.quantity, now, Liquidity.Maker, o.reduceOnly)
        (next, evs ++ fillEvs)
      }

  /** BBO 严格穿越挂单价的全部挂单成交 (maker 悲观侧, 成交价取挂单价) */
  private def matchCrossing(exchange: Exchange, bbo: BBO, now: Timestamp): (SimState, Vector[AnyEvent]) =
    fillCrossed(exchange, crossingOrders(o => o.symbol == bbo.symbol && Matcher.crossedByBbo(o.side, o.limitPrice, bbo)), now)

  /** 真实成交严格穿越挂单价的全部挂单成交 (maker 悲观侧, 成交价取挂单价) */
  private def matchTrade(exchange: Exchange, t: MarketTrade, now: Timestamp): (SimState, Vector[AnyEvent]) =
    fillCrossed(exchange, crossingOrders(o => o.symbol == t.symbol && Matcher.crossedByTrade(o.side, o.limitPrice, t.price)), now)

  // ==================== 下单到达撮合 ====================

  /** 订单到达撮合：按类型/TIF 决定 resting / 成交 / 拒单。`now` 为到达时刻 (回报时间戳取自它) */
  def onOrderArrived(exchange: Exchange, order: Order, orderId: OrderId, now: Timestamp): (SimState, Vector[AnyEvent]) =
    // 进来的 [[Order]] 按类型即是币本位 —— 换算的职责在 exchange 边界 (虚拟柜台扮演交易所时
    // 收 ExchangeOrder 并自行换回)，撮合内部不再关心张数。
    val bboOpt = lastBbo.get(order.symbol)
    order.orderType match
      case OrderType.Market =>
        bboOpt match
          case Some(bbo) =>
            fill(exchange, orderId, order.clientOrderId, order.symbol, order.side, Matcher.touchPrice(order.side, bbo), order.quantity, now, Liquidity.Taker, order.reduceOnly)
          case None =>
            (this, Vector(statusEvent(exchange, order, orderId, OrderStatus.Rejected("no market data for market order"), Price.Zero, now)))
      case OrderType.Limit(limit, tif) =>
        // 到达即可成交时的对手价 (None = 不可成交)。用 taker 判定 (价格重合即成交, 乐观侧),
        // 与 resting 的严格穿越判定刻意不同 —— 见 [[Matcher]] 的"悲观间隙"说明。
        // 价与可成交性同源 (都出自这张 bbo), 无需 .get
        val takerPrice: Option[Price] =
          bboOpt.filter(Matcher.marketable(order.side, limit, _)).map(Matcher.touchPrice(order.side, _))
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
  def onCancelArrived(exchange: Exchange, ref: OrderRef, now: Timestamp): (SimState, Vector[AnyEvent]) =
    findResting(ref) match
      case Some((orderId, o)) =>
        val ev = Event.stamped(
          Topics.OrderUpdate,
          OrderUpdate(account, orderId, Some(o.clientOrderId), exchange, o.symbol, o.side, OrderStatus.Cancelled, o.limitPrice, o.quantity, Coin.Zero, now),
          now,
          now,
        )
        (copy(resting = resting - orderId), Vector(ev))
      case None => (this, Vector.empty)

  /** 按交易所 id 或 clientOrderId 找挂单 —— 与真实交易所的两种撤单指名方式一致 */
  def findResting(ref: OrderRef): Option[(OrderId, RestingOrder)] = ref match
    case OrderRef.ByExchangeId(id) => resting.get(id).map(id -> _)
    case OrderRef.ByClientId(cid)  => resting.find((_, o) => o.clientOrderId == cid)

  // ==================== 私有构造 ====================

  private def rest(exchange: Exchange, order: Order, orderId: OrderId, limit: Price, now: Timestamp): (SimState, Vector[AnyEvent]) =
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
      qty: Coin,
      now: Timestamp,
      liquidity: Liquidity,
      reduceOnly: Boolean,
  ): (SimState, Vector[AnyEvent]) =
    val effectiveQty =
      if !reduceOnly then qty
      else
        val posSize = ledger.positions.get(symbol).map(_.size).getOrElse(Coin.Zero)
        side match
          case Side.Short => qty.min(posSize.max(Coin.Zero))    // 卖平多: 至多平掉现有多头
          case Side.Long  => qty.min((-posSize).max(Coin.Zero)) // 买平空: 至多平掉现有空头
    if reduceOnly && effectiveQty.isZero then
      // reduceOnly 无可平仓位 -> 不成交，回 Cancelled (订单已被调用方移出簿 / 不入簿)
      val update = OrderUpdate(account, orderId, Some(clientOrderId), exchange, symbol, side, OrderStatus.Cancelled, fillPrice, qty, Coin.Zero, now)
      (this, Vector(Event.stamped(Topics.OrderUpdate, update, now, now)))
    else
      val feeRate = liquidity match
        case Liquidity.Maker => makerFeeRate
        case Liquidity.Taker => takerFeeRate
      val fee = effectiveQty.notional(fillPrice) * feeRate
      val next = copy(ledger = ledger.applyFill(exchange, symbol, side, fillPrice, effectiveQty, fee))
      val update = OrderUpdate(account, orderId, Some(clientOrderId), exchange, symbol, side, OrderStatus.Filled, fillPrice, effectiveQty, effectiveQty, now)
      val f = Fill(account, exchange, symbol, side, fillPrice, effectiveQty, now)
      // 顺序是这三条的全部意义, 见 hft.exchange.TradingGateway 的"回报有固定顺序":
      //   仓位快照 -> 成交 -> 订单终态
      (next, Vector(next.positionEvent(exchange, symbol, now), Event.stamped(Topics.Fill, f, now, now), Event.stamped(Topics.OrderUpdate, update, now, now)))

  /** 本账本当下这个标的的仓位快照 —— 构造与真实柜台同一份 */
  private[sim] def positionEvent(exchange: Exchange, symbol: Symbol, now: Timestamp): AnyEvent =
    hft.exchange.TradingGateway.positionEvent(
      Position(account, exchange, symbol, ledger.positions.get(symbol).fold(Coin.Zero)(_.size)),
      now,
    )

  private def statusEvent(exchange: Exchange, order: Order, orderId: OrderId, status: OrderStatus, price: Price, now: Timestamp): AnyEvent =
    Event.stamped(
      Topics.OrderUpdate,
      OrderUpdate(account, orderId, Some(order.clientOrderId), exchange, order.symbol, order.side, status, price, order.quantity, Coin.Zero, now),
      now,
      now,
    )

object SimState:
  def empty(
      account: AccountId,
      cash: Double,
      makerFeeRate: Double = 0.0,
      takerFeeRate: Double = 0.0,
  ): SimState =
    SimState(account, Ledger.empty(account, cash), Map.empty, Map.empty, Map.empty, Map.empty, makerFeeRate, takerFeeRate)
