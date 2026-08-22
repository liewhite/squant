package hft.engine

import hft.domain.*
import hft.exchange.ExchangeClient
import hft.event.{Event, EventBus, Interest, Topics}
import hft.strategy.{OrderIntent, OutcomeEvent}
import org.slf4j.LoggerFactory
import ox.{Ox, fork}

/** 信号处理器：消费策略信号，调用交易所 REST API 执行下单/撤单。
  *
  * 每个 REST 调用 fork 独立虚拟线程执行，互不阻塞。
  *
  * 错误处理 (fail-fast)：
  *   - 交易所明确拒绝 (HTTP 4xx，订单确定未成立) 是正常业务结果，
  *     以 OrderUpdate(Error/Rejected) 事件回流策略
  *   - 网络错误 / 超时 / 5xx —— 订单是否已到达交易所**不确定**，本地状态无法保证正确，
  *     直接抛错终止引擎 (重启后由启动对齐恢复一致)
  */
final class OutcomeProcessor(
    clients: Map[Exchange, ExchangeClient],
    bus: EventBus,
    dryRun: Boolean,
):
  private val logger = LoggerFactory.getLogger(classOf[OutcomeProcessor])

  /** 订阅下单意图并启动执行循环。
    *
    * 全量订阅 [[OrderIntent]] —— 它是唯一通往交易所的出口，没有"只执行一部分信号"的语义。
    */
  def run()(using Ox): Unit =
    if dryRun then logger.warn("OutcomeProcessor started in DRY-RUN mode (orders will NOT be placed)")
    else logger.info("OutcomeProcessor started")
    val signals = bus.subscribe(Set(Interest.All(OrderIntent)))
    fork {
      while true do
        val event = signals.receive()
        event.as(OrderIntent).foreach(handle)
    }
    ()

  private def handle(signal: OutcomeEvent)(using Ox): Unit = signal match
    case OutcomeEvent.PlaceOrders(orders, comment) =>
      // 关联订单独立并行下单：IOC 订单本身接受部分成交，敞口由策略层 rebalance 兜底
      orders.foreach(placeOrder(_, comment))
    case OutcomeEvent.CancelOrder(exchange, symbol, orderId) =>
      cancelOrder(exchange, symbol, orderId)

  private def placeOrder(order: Order, comment: String)(using Ox): Unit =
    if dryRun then
      logger.warn(s"[DRY-RUN] order NOT placed: ${describe(order)} signal=$comment")
      // dry-run 等价于确定性的"未下单"，以 Error 事件回流清理 pending
      publishOrderError(order, "dry-run: order not placed")
    else
      val client = requireClient(order.exchange)
      logger.info(s"Placing order: ${describe(order)} signal=$comment")
      fork {
        client.placeOrder(order) match
          case Right(orderId) =>
            // 订单确认 (Pending/Filled) 以私有流推送为准，这里只记录
            logger.info(
              s"Order accepted: ${order.exchange} ${order.symbol} orderId=$orderId clientOrderId=${order.clientOrderId}"
            )
          case Left(e @ ExchangeError.Http(status, _)) if status == 429 || status == 418 =>
            // 限频/封禁: 说明"订单生命周期自然限速"的假设已被打破，
            // 按拒单回流会形成"拒单->重挂->更多请求"的重试风暴，必须终止
            throw IllegalStateException(s"Rate limited by exchange, aborting: ${describe(order)} error=${e.message}")
          case Left(e @ ExchangeError.Http(status, _)) if status >= 400 && status < 500 =>
            // 交易所明确拒绝，订单确定未成立 -> 回流策略
            logger.warn(s"Order rejected: ${describe(order)} error=${e.message}")
            publishOrderError(order, e.message)
          case Left(e) =>
            // 网络/超时/5xx: 订单是否成立不确定，本地状态无法保证正确
            throw IllegalStateException(s"Place order outcome UNKNOWN, aborting: ${describe(order)} error=${e.message}")
      }
      ()

  private def cancelOrder(exchange: Exchange, symbol: Symbol, orderId: OrderId)(using Ox): Unit =
    if dryRun then logger.warn(s"[DRY-RUN] CancelOrder NOT sent: $exchange $symbol $orderId")
    else
      val client = requireClient(exchange)
      logger.info(s"Cancelling order: $exchange $symbol $orderId")
      fork {
        client.cancelOrder(symbol, orderId) match
          case Right(()) =>
            // 终态 (Cancelled) 以私有流推送为准
            logger.info(s"Cancel accepted: $exchange $symbol $orderId")
          case Left(ExchangeError.OrderNotFound(reason)) =>
            // 订单已成交/已撤销，终态同样由私有流推送，撤单失败非致命
            logger.info(s"Order already gone: $exchange $symbol $orderId ($reason)")
          case Left(e) =>
            throw IllegalStateException(s"Cancel outcome UNKNOWN, aborting: $exchange $symbol $orderId error=${e.message}")
      }
      ()

  /** 策略引用了未配置的交易所是装配错误，立即终止 */
  private def requireClient(exchange: Exchange): ExchangeClient =
    clients.getOrElse(exchange, throw IllegalStateException(s"No client configured for exchange $exchange"))

  /** 确定性的下单失败以 OrderUpdate(Error) 回流，驱动 pending order 清理 */
  private def publishOrderError(order: Order, reason: String): Unit =
    val update = OrderUpdate(
      orderId = "",
      clientOrderId = Some(order.clientOrderId),
      exchange = order.exchange,
      symbol = order.symbol,
      side = order.side,
      status = OrderStatus.Error(reason),
      price = 0.0,
      quantity = 0.0,
      filledQuantity = 0.0,
      fillSize = 0.0,
      timestamp = nowMs,
    )
    bus.publish(Event.local(Topics.OrderUpdate, update))

  private def describe(order: Order): String =
    s"${order.exchange} ${order.symbol} ${order.side} ${order.orderType} qty=${order.quantity} " +
      s"reduceOnly=${order.reduceOnly} clientOrderId=${order.clientOrderId}"
