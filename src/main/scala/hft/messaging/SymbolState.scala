package hft.messaging

import hft.domain.*
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
  val markPrices: mutable.Map[Exchange, MarkPrice] = mutable.Map.empty
  val indexPrices: mutable.Map[Exchange, IndexPrice] = mutable.Map.empty
  val positions: mutable.Map[Exchange, Position] = mutable.Map.empty
  /** 待处理订单 (以 clientOrderId 为 key) */
  private val _pendingOrders: mutable.Map[String, PendingOrder] = mutable.Map.empty

  // ==================== 查询 ====================

  def bbo(exchange: Exchange): Option[BBO] = bbos.get(exchange)
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

  /** 清理超时订单，返回移除数量。
    *
    * 仅清理 Created 状态超过 timeoutMs 的订单 (交易所未确认，视为丢失)。
    * 已确认的挂单 (Pending/PartiallyFilled) 由策略决定何时撤单，不做超时清理。
    */
  def removeTimedOutOrders(now: Timestamp, timeoutMs: Long): Int =
    if timeoutMs <= 0 then 0
    else
      val expired = _pendingOrders.filter { (_, p) =>
        p.status == OrderStatus.Created && now - p.createdAt > timeoutMs
      }
      expired.foreach { (clientId, p) =>
        logger.warn(
          s"[$symbol] order timed out (no exchange confirmation), removing: " +
            s"clientOrderId=$clientId exchange=${p.order.exchange} elapsedMs=${now - p.createdAt}"
        )
        _pendingOrders.remove(clientId)
      }
      expired.size

  // ==================== 事件处理 ====================

  /** 更新状态。事件 symbol 与本 state 不一致时忽略 */
  def apply(event: IncomeEvent): Unit =
    event.symbol match
      case Some(s) if s != symbol =>
        logger.warn(s"Event symbol mismatch, ignoring: expected=$symbol actual=$s")
      case None => () // 账户级事件在 per-symbol 状态中不处理
      case _ =>
        event.data match
          case EventData.FundingRateUpdate(rate) => fundingRates(rate.exchange) = rate
          case EventData.BboUpdate(bbo)          => bbos(bbo.exchange) = bbo
          case EventData.MarkPriceUpdate(mp)     => markPrices(mp.exchange) = mp
          case EventData.IndexPriceUpdate(ip)    => indexPrices(ip.exchange) = ip
          case EventData.PositionUpdate(pos)     => applyPosition(pos)
          case EventData.OrderUpdated(update)    => applyOrderUpdate(update)
          case EventData.FillUpdate(fill)        => applyFill(fill)
          case _                                 => ()

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
      Position(fill.exchange, symbol, 0.0, fill.price, 0.0),
    )
    val updated = pos.copy(size = pos.size + delta)
    positions(fill.exchange) = updated
    logger.info(
      s"[$symbol] position updated on fill: exchange=${fill.exchange} side=${fill.side} " +
        s"fillSize=${fill.size} fillPrice=${fill.price} newPositionSize=${updated.size}"
    )
