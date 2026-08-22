package hft.engine

import hft.domain.*
import hft.event.{AnyEvent, Event, EventBus, Interest, Topics}
import hft.strategy.{OrderIntent, OutcomeEvent, Strategy}
import ox.supervised
import hft.state.{StateManager}

/** Executor 集成测试: 通过 EventBus 驱动完整的 事件 -> 策略 -> 信号 链路 */
class ExecutorFlowSpec extends munit.FunSuite:
  private val meta = SymbolMeta(
    exchange = Exchange.Binance,
    symbol = "BTCUSDT",
    tickSize = 0.1,
    sizeStep = 0.001,
    minOrderSize = 0.001,
    contractSize = 1.0,
  )

  /** Clock 触发即下单的 stub 策略 */
  private class ClockOrderStrategy extends Strategy:
    override def interests: Set[Interest] =
      Set(Interest.Keyed(Topics.Bbo, Set(Instrument(Exchange.Binance, "BTCUSDT"))))
    override def orderTimeoutMs: Long = 5000
    override def onEvent(event: AnyEvent, state: StateManager): Vector[OutcomeEvent] =
      event.as(Topics.Clock).map { _ =>
        val order = Order(
          id = "",
          exchange = Exchange.Binance,
          symbol = "BTCUSDT",
          side = Side.Long,
          orderType = OrderType.Limit(62761.333, TimeInForce.GTC),
          quantity = 0.0015,
          reduceOnly = false,
          clientOrderId = "",
        )
        OutcomeEvent.PlaceOrders(Vector(order), "test")
      }.toVector

  test("事件驱动策略产出信号: clientOrderId 生成 + 精度转换"):
    supervised:
      val bus = EventBus()
      val outcomes = bus.subscribe(Set(Interest.All(OrderIntent)))

      Executor(ClockOrderStrategy(), Map((Exchange.Binance, "BTCUSDT") -> meta), bus).run()
      bus.publish(Event.local(Topics.Clock, ()))

      outcomes.receive().as(OrderIntent).get match
        case OutcomeEvent.PlaceOrders(orders, comment) =>
          assertEquals(comment, "test")
          assertEquals(orders.size, 1)
          val order = orders.head
          assert(order.clientOrderId.nonEmpty, "Executor 应生成 clientOrderId")
          assertEquals(order.quantity, 0.001) // 0.0015 向下取整到 step
          assertEquals(order.orderType, OrderType.Limit(62761.3, TimeInForce.GTC)) // 价格取整到 tick
        case other => fail(s"unexpected signal: $other")

  test("订阅范围外的 symbol 事件被过滤，不触达策略"):
    supervised:
      val bus = EventBus()
      val outcomes = bus.subscribe(Set(Interest.All(OrderIntent)))

      Executor(ClockOrderStrategy(), Map((Exchange.Binance, "BTCUSDT") -> meta), bus).run()

      // 范围外 symbol 事件 (若未被过滤会触发 StateManager 路由 sys.error 使作用域崩溃)
      val other = BBO(Exchange.Binance, "DOGEUSDT", 0.1, 1.0, 0.2, 1.0, 0L)
      bus.publish(Event.at(Topics.Bbo, other, 0L))
      // Clock 紧随其后仍能正常产出信号，证明 executor 没有被范围外事件破坏
      bus.publish(Event.local(Topics.Clock, ()))

      assert(outcomes.receive().as(OrderIntent).exists(_.isInstanceOf[OutcomeEvent.PlaceOrders]))

  test("策略交易的 symbol 缺少 SymbolMeta -> 配置错误，作用域终止"):
    intercept[RuntimeException] {
      supervised:
        val bus = EventBus()
        val outcomes = bus.subscribe(Set(Interest.All(OrderIntent)))

        // 空 metas: 策略下单时 convertOrder 抛错 -> executor fork 失败 -> 作用域级联终止
        Executor(ClockOrderStrategy(), Map.empty, bus).run()
        bus.publish(Event.local(Topics.Clock, ()))
        outcomes.receive() // 阻塞至 fork 失败取消作用域
    }
