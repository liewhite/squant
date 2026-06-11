package hft.engine

import hft.domain.*
import hft.exchange.ExchangeClient
import hft.messaging.{EventBus, EventData, IncomeEvent}
import hft.strategy.OutcomeEvent
import org.slf4j.LoggerFactory
import ox.{Ox, fork}
import ox.channels.Source

/** 信号处理器：消费策略信号，调用交易所 REST API 执行下单/撤单。
  *
  * 每个 REST 调用 fork 独立虚拟线程执行，互不阻塞；
  * 执行结果 (撤单确认、下单失败) 以 OrderUpdate 事件回流 income 总线。
  */
final class OutcomeProcessor(
    clients: Map[Exchange, ExchangeClient],
    incomeBus: EventBus[IncomeEvent],
    dryRun: Boolean,
):
  private val logger = LoggerFactory.getLogger(classOf[OutcomeProcessor])

  /** 启动信号消费循环 */
  def run(signals: Source[OutcomeEvent])(using Ox): Unit =
    if dryRun then logger.warn("OutcomeProcessor started in DRY-RUN mode (orders will NOT be placed)")
    else logger.info("OutcomeProcessor started")
    fork {
      while true do handle(signals.receive())
    }
    ()

  private def handle(signal: OutcomeEvent)(using Ox): Unit = signal match
    case OutcomeEvent.PlaceOrders(orders, comment) =>
      // 关联订单独立并行下单：IOC 订单本身接受部分成交，敞口由策略层 rebalance 兜底
      orders.foreach(placeOrder(_, comment))
    case OutcomeEvent.CancelOrder(exchange, symbol, orderId, clientOrderId) =>
      cancelOrder(exchange, symbol, orderId, clientOrderId)

  private def placeOrder(order: Order, comment: String)(using Ox): Unit =
    clients.get(order.exchange) match
      case None =>
        val reason = s"No client found for exchange ${order.exchange}"
        logger.error(reason)
        publishOrderError(order, reason)
      case Some(client) =>
        if dryRun then
          logger.warn(s"[DRY-RUN] order NOT placed: ${describe(order)} signal=$comment")
        else
          logger.info(s"Placing order: ${describe(order)} signal=$comment")
          fork {
            client.placeOrder(order) match
              case Right(orderId) =>
                logger.info(
                  s"Order placed: ${order.exchange} ${order.symbol} orderId=$orderId clientOrderId=${order.clientOrderId}"
                )
              case Left(e) =>
                logger.error(s"Failed to place order: ${describe(order)} error=${e.message}")
                publishOrderError(order, e.message)
          }
          ()

  private def cancelOrder(exchange: Exchange, symbol: Symbol, orderId: OrderId, clientOrderId: String)(using
      Ox
  ): Unit =
    clients.get(exchange) match
      case None => logger.error(s"No client found for cancel_order: $exchange")
      case Some(client) =>
        if dryRun then logger.warn(s"[DRY-RUN] CancelOrder NOT sent: $exchange $symbol $orderId")
        else
          logger.info(s"Cancelling order: $exchange $symbol $orderId")
          fork {
            client.cancelOrder(symbol, orderId) match
              case Right(()) =>
                // 撤单成功，反馈 Cancelled 状态让 SymbolState 移除 pending order
                val update = OrderUpdate(
                  orderId = orderId,
                  clientOrderId = Some(clientOrderId),
                  exchange = exchange,
                  symbol = symbol,
                  side = Side.Long, // 撤单事件中 side 无实际意义
                  status = OrderStatus.Cancelled,
                  price = 0.0,
                  quantity = 0.0,
                  filledQuantity = 0.0,
                  fillSize = 0.0,
                  timestamp = nowMs,
                )
                incomeBus.publish(IncomeEvent.local(EventData.OrderUpdated(update)))
              case Left(e) =>
                logger.error(s"Failed to cancel order: $exchange $symbol $orderId error=${e.message}")
          }
          ()

  /** 下单失败以 OrderUpdate(Error) 回流，驱动 pending order 清理 */
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
    incomeBus.publish(IncomeEvent.local(EventData.OrderUpdated(update)))

  private def describe(order: Order): String =
    s"${order.exchange} ${order.symbol} ${order.side} ${order.orderType} qty=${order.quantity} " +
      s"reduceOnly=${order.reduceOnly} clientOrderId=${order.clientOrderId}"
