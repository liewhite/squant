package hft.state

import hft.domain.*
import hft.event.{AnyEvent, Topics}
import org.slf4j.LoggerFactory

import scala.collection.mutable

/** 待处理订单信息 (完整订单 + 运行时状态) */
final case class PendingOrder(
    order: Order,
    status: OrderStatus,
    createdAt: Timestamp,
)

/** 单个交易对在所有交易所的聚合状态。
  *
  * 可变状态，仅在所属 Executor 的虚拟线程内访问，无需同步。
  */
final class SymbolState(val symbol: Symbol):
  private val logger = LoggerFactory.getLogger(classOf[SymbolState])

  val fundingRates: mutable.Map[Exchange, FundingRate] = mutable.Map.empty
  val bbos: mutable.Map[Exchange, BBO] = mutable.Map.empty
  /** 最新公共成交印记 (trade-only 行情下作价格基准) */
  val lastTrades: mutable.Map[Exchange, MarketTrade] = mutable.Map.empty
  val markPrices: mutable.Map[Exchange, MarkPrice] = mutable.Map.empty
  val indexPrices: mutable.Map[Exchange, IndexPrice] = mutable.Map.empty
  val positions: mutable.Map[Exchange, Position] = mutable.Map.empty
  /** 待处理订单 (以 clientOrderId 为 key) */
  private val _pendingOrders: mutable.Map[String, PendingOrder] = mutable.Map.empty

  // ==================== 查询 ====================

  def bbo(exchange: Exchange): Option[BBO] = bbos.get(exchange)
  def lastTrade(exchange: Exchange): Option[MarketTrade] = lastTrades.get(exchange)
  /** 最新成交价 (trade-only 行情下的价格基准)，无成交记录返回 None */
  def lastTradePrice(exchange: Exchange): Option[Price] = lastTrades.get(exchange).map(_.price)
  def markPrice(exchange: Exchange): Option[MarkPrice] = markPrices.get(exchange)
  def indexPrice(exchange: Exchange): Option[IndexPrice] = indexPrices.get(exchange)
  def fundingRate(exchange: Exchange): Option[FundingRate] = fundingRates.get(exchange)
  def position(exchange: Exchange): Option[Position] = positions.get(exchange)

  /** 仓位大小。无仓位记录等价于空仓 (size = 0)，策略启动初期确实没有仓位 */
  def positionSize(exchange: Exchange): Quantity =
    positions.get(exchange).map(_.size).getOrElse(0.0)

  def hasPositions: Boolean = positions.values.exists(p => !p.isEmpty)

  /** 多空仓位大小: (多头总量(正), 空头总量(负)) */
  def positionSizes: (Quantity, Quantity) =
    val sizes = positions.values.map(_.size)
    (sizes.filter(_ > 0).sum, sizes.filter(_ < 0).sum)

  def hasPendingOrders: Boolean = _pendingOrders.nonEmpty

  def hasPendingSide(side: Side): Boolean =
    _pendingOrders.values.exists(_.order.side == side)

  def pendingOrders: Iterable[PendingOrder] = _pendingOrders.values

  /** 统一时间基准: (所有交易所中最近的结算时间, 最新数据时间)，用于公平比较日化费率 */
  private def unifiedTimeBase: Option[(Timestamp, Timestamp)] =
    if fundingRates.isEmpty then None
    else
      val minSettle = fundingRates.values.map(_.nextSettleTime).min
      val current = fundingRates.values.map(_.timestamp).max
      Some((minSettle, current))

  /** 日化费率最高的交易所 (适合做空收资费) */
  def bestShortExchange: Option[(Exchange, FundingRate)] =
    unifiedTimeBase.map { (base, current) =>
      fundingRates.maxBy((_, r) => r.dailyRateWithBaseTime(base, current))
    }

  /** 日化费率最低的交易所 (适合做多付资费) */
  def bestLongExchange: Option[(Exchange, FundingRate)] =
    unifiedTimeBase.map { (base, current) =>
      fundingRates.minBy((_, r) => r.dailyRateWithBaseTime(base, current))
    }

  // ==================== 订单管理 ====================

  /** 添加待处理订单 (发送订单信号时调用) */
  def addPendingOrder(order: Order, createdAt: Timestamp): Unit =
    _pendingOrders(order.clientOrderId) = PendingOrder(order, OrderStatus.Created, createdAt)

  /** 移除指定的待处理订单 */
  def removePendingOrder(clientOrderId: String): Unit =
    _pendingOrders.remove(clientOrderId)

  /** 校验不存在超时未确认的订单，违反即抛错终止。
    *
    * Created 状态超过 timeoutMs：REST 已设置更短的超时，正常情况下下单要么明确成功
    * (私有流推送确认) 要么明确失败 (Error 事件清理 pending)，走到这里说明订单结果
    * **不确定**——清理后重下会造成敞口翻倍，唯一安全的做法是终止，由重启后的启动对齐恢复。
    * 已确认挂单 (Pending/PartiallyFilled) 由策略决定何时撤单，不参与校验。
    */
  def failOnTimedOutOrders(now: Timestamp, timeoutMs: Long): Unit =
    if timeoutMs > 0 then
      _pendingOrders.find((_, p) => p.status == OrderStatus.Created && now - p.createdAt > timeoutMs).foreach {
        (clientId, p) =>
          sys.error(
            s"[$symbol] order unconfirmed after ${now - p.createdAt}ms (timeout=${timeoutMs}ms), outcome UNKNOWN: " +
              s"clientOrderId=$clientId exchange=${p.order.exchange}"
          )
      }

  // ==================== 事件处理 ====================

  /** 按 topic 更新状态。
    *
    * 事件已由总线按标的精确投递、再由 [[StateManager]] 按 symbol 定位到本实例，
    * 故这里不再重复校验归属 —— 路由键就是从载荷派生的，不存在错配的可能。
    */
  def apply(event: AnyEvent): Unit =
    event.as(Topics.FundingRate).foreach(r => fundingRates(r.exchange) = r)
    event.as(Topics.Bbo).foreach(b => bbos(b.exchange) = b)
    event.as(Topics.Trade).foreach(t => lastTrades(t.exchange) = t)
    event.as(Topics.MarkPrice).foreach(m => markPrices(m.exchange) = m)
    event.as(Topics.IndexPrice).foreach(i => indexPrices(i.exchange) = i)
    event.as(Topics.Position).foreach(applyPosition)
    event.as(Topics.OrderUpdate).foreach(applyOrderUpdate)
    event.as(Topics.Fill).foreach(applyFill)

  /** 仅用于初始加载: 本地无仓位时写入，之后完全由 Fill 事件维护 */
  private def applyPosition(position: Position): Unit =
    if !positions.contains(position.exchange) then
      logger.info(s"[$symbol] position initialized from poll: exchange=${position.exchange} size=${position.size}")
      positions(position.exchange) = position

  private def applyOrderUpdate(update: OrderUpdate): Unit =
    logger.info(
      s"[$symbol] order status: exchange=${update.exchange} orderId=${update.orderId} " +
        s"clientOrderId=${update.clientOrderId} status=${update.status}"
    )
    // 用 clientOrderId 跟踪订单；没有 clientOrderId 说明不是我们发起的订单，忽略
    update.clientOrderId.foreach { clientId =>
      if update.status.isTerminal then _pendingOrders.remove(clientId)
      else if update.status.isConfirmed then
        _pendingOrders.get(clientId) match
          case Some(pending) =>
            // 交易所已确认，更新状态并回填 orderId
            val order =
              if pending.order.id.isEmpty then pending.order.copy(id = update.orderId)
              else pending.order
            _pendingOrders(clientId) = pending.copy(order = order, status = update.status)
          case None =>
            // 启动时同步的现有挂单，注册到 pendingOrders
            val order = Order(
              id = update.orderId,
              exchange = update.exchange,
              symbol = update.symbol,
              side = update.side,
              orderType = OrderType.Limit(update.price, TimeInForce.GTC),
              quantity = update.quantity,
              reduceOnly = false,
              clientOrderId = clientId,
            )
            _pendingOrders(clientId) = PendingOrder(order, update.status, update.timestamp)
    }

  /** Fill 事件即时更新仓位 (无论是策略订单还是手动订单) */
  private def applyFill(fill: Fill): Unit =
    val delta = fill.side match
      case Side.Long  => fill.size
      case Side.Short => -fill.size
    val pos = positions.getOrElseUpdate(
      fill.exchange,
      Position(fill.account, fill.exchange, symbol, 0.0, fill.price, 0.0),
    )
    val updated = pos.copy(size = pos.size + delta)
    positions(fill.exchange) = updated
    logger.info(
      s"[$symbol] position updated on fill: exchange=${fill.exchange} side=${fill.side} " +
        s"fillSize=${fill.size} fillPrice=${fill.price} newPositionSize=${updated.size}"
    )
