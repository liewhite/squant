package hft.engine

import hft.actor.ActorSystem
import hft.domain.*
import hft.event.{AnyEvent, Event, EventBus, Interest, Topics}
import hft.state.StateManager
import hft.event.Commands.{AccountOutcome, OrderIntent, OutcomeEvent}
import hft.strategy.{Strategy, StrategyHandlers}
import ox.supervised
import hft.TestCommandSink
import hft.TestUnits.given

/** Executor 集成测试: 通过 EventBus + ActorSystem 驱动完整的 事件 -> 策略 -> 信号 链路 */
class ExecutorFlowSpec extends munit.FunSuite:
  /** Clock 触发即下单的 stub 策略 */
  private class ClockOrderStrategy extends Strategy:
    override def handlers: StrategyHandlers = StrategyHandlers.empty
      // 声明标的以获得订阅范围与 SymbolMeta 校验；下单由时钟触发
      .market(Topics.Bbo, Instrument(Exchange.Binance, "BTCUSDT")) { (_, _, _) => Vector.empty }
      .onClock { (ctx, _) =>
        ctx.place(
          Order(
            id = "",
            exchange = Exchange.Binance,
            symbol = "BTCUSDT",
            side = Side.Long,
            orderType = OrderType.Limit(62761.333, TimeInForce.GTC),
            quantity = 0.0015,
            reduceOnly = false,
            clientOrderId = "",
          ),
          "test",
        )
      }

  test("事件驱动策略产出信号: 生成 clientOrderId, 数量与价格原样交给柜台"):
    supervised:
      val bus = EventBus()
      val outcomes = bus.subscribe(Set(Interest.Keyed(OrderIntent, Set(AccountExchange(AccountId.Live, Exchange.Binance)))))
      val system = ActorSystem(bus)
      system.spawn(TestCommandSink("order-sink", OrderIntent, Set(AccountExchange(AccountId.Live, Exchange.Binance))))

      system.spawn(Executor.readyToTrade(ClockOrderStrategy(), AccountId.Live))
      bus.publish(Event.local(Topics.Clock, ()))

      outcomes.events.receive().as(OrderIntent).get.outcome match
        case OutcomeEvent.PlaceOrders(orders, comment) =>
          assertEquals(comment, "test")
          assertEquals(orders.size, 1)
          val order = orders.head
          assert(order.clientOrderId.nonEmpty, "Executor 应生成 clientOrderId")
          // 精度对齐归柜台 (见 TradingGatewaySpec) —— 策略这一侧从头到尾只有币本位的意图,
          // 不该在这里被改写。原先它在此处取整, 于是 tick/最小下单量泄漏进了策略侧。
          assertEquals(order.quantity.value, 0.0015)
          assertEquals(order.orderType, OrderType.Limit(62761.333, TimeInForce.GTC))
        case other => fail(s"unexpected signal: $other")

  test("订阅范围外的 symbol 事件被过滤，不触达策略"):
    supervised:
      val bus = EventBus()
      val outcomes = bus.subscribe(Set(Interest.Keyed(OrderIntent, Set(AccountExchange(AccountId.Live, Exchange.Binance)))))
      val system = ActorSystem(bus)
      system.spawn(TestCommandSink("order-sink", OrderIntent, Set(AccountExchange(AccountId.Live, Exchange.Binance))))

      system.spawn(Executor.readyToTrade(ClockOrderStrategy(), AccountId.Live))

      // 范围外 symbol 事件 (若未被过滤会触发 StateManager 路由 sys.error 使作用域崩溃)
      val other = BBO(Exchange.Binance, "DOGEUSDT", 0.1, Coin(1.0), 0.2, Coin(1.0), 0L)
      bus.publish(Event.at(Topics.Bbo, other, 0L))
      // Clock 紧随其后仍能正常产出信号，证明 executor 没有被范围外事件破坏
      bus.publish(Event.local(Topics.Clock, ()))

      assert(outcomes.events.receive().as(OrderIntent).exists(_.outcome.isInstanceOf[OutcomeEvent.PlaceOrders]))

  // "标的缺少合约规格" 已不是这一层的事: 规格归柜台, 缺了它由柜台在装配期或发单时报错
  // (见 TradingGatewaySpec)。策略这一侧根本不接触精度。
