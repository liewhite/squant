package hft.engine

import hft.domain.*
import hft.messaging.{EventBus, IncomeEvent, StateManager}
import hft.strategy.{OutcomeEvent, Strategy}
import org.slf4j.LoggerFactory
import ox.{Ox, fork}
import ox.channels.Source

/** 策略执行器：每个策略一个 Executor，独占一个虚拟线程串行消费事件。
  *
  * 职责：
  *   - 按策略订阅范围过滤事件 (全局事件广播，symbol 事件定向)
  *   - 维护策略独享的 StateManager
  *   - 将策略产出的订单分配 clientOrderId、登记 pending、按交易所精度转换后发布
  */
final class Executor(
    strategy: Strategy,
    symbolMetas: Map[(Exchange, Symbol), SymbolMeta],
    outcomeBus: EventBus[OutcomeEvent],
):
  private val logger = LoggerFactory.getLogger(classOf[Executor])

  /** 策略订阅的 (exchange, symbol) 集合，用于事件过滤 */
  private val subscriptions: Set[(Exchange, Symbol)] =
    strategy.publicStreams.toSet.flatMap { (exchange, kinds) =>
      kinds.map(k => (exchange, k.subscribedSymbol))
    }

  private val state = StateManager(subscriptions.map(_._2), strategy.orderTimeoutMs)

  /** 启动执行循环 */
  def run(events: Source[IncomeEvent])(using Ox): Unit =
    fork {
      logger.info(s"Executor started: subscriptions=$subscriptions")
      while true do
        val event = events.receive()
        if accepts(event) then handle(event)
    }
    ()

  /** 全局事件 (无路由键) 广播；symbol 事件仅接收订阅范围内的 */
  private def accepts(event: IncomeEvent): Boolean =
    event.routing.forall(subscriptions.contains)

  private def handle(event: IncomeEvent): Unit =
    state.apply(event)
    strategy.onEvent(event, state).foreach {
      case OutcomeEvent.PlaceOrders(orders, comment) =>
        val converted = orders.map { order =>
          val withId = order.copy(clientOrderId = order.exchange.newClientOrderId)
          // 原始订单 (币本位) 登记到 pending，策略端统一看到币的数量
          state.addPendingOrder(withId)
          // 转换为交易所格式 (合约张数 + 价格/数量取整)
          convertOrder(withId)
        }
        outcomeBus.publish(OutcomeEvent.PlaceOrders(converted, comment))
      case cancel: OutcomeEvent.CancelOrder =>
        outcomeBus.publish(cancel)
    }

  /** 币本位数量 -> 合约张数，价格/数量按交易所精度取整。
    * 缺少 SymbolMeta 说明策略交易了未预加载的 symbol，是配置错误，立即终止
    */
  private def convertOrder(order: Order): Order =
    val meta = symbolMetas.getOrElse(
      (order.exchange, order.symbol),
      sys.error(s"SymbolMeta not found for ${order.exchange} ${order.symbol}, cannot convert order"),
    )
    val quantity = meta.roundSizeDown(meta.coinToQty(order.quantity))
    val orderType = order.orderType match
      case OrderType.Market            => OrderType.Market
      case OrderType.Limit(price, tif) => OrderType.Limit(meta.roundPrice(price), tif)
    order.copy(quantity = quantity, orderType = orderType)
