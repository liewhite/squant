package hft.engine

import hft.actor.ActorSystem
import hft.domain.*
import hft.event.{AnyEvent, Event, EventBus, Interest, Topics}
import hft.state.StateManager
import hft.event.Commands.{AccountOutcome, OrderIntent, OutcomeEvent}
import hft.strategy.{Strategy, StrategyHandlers}
import ox.supervised
import hft.TestUnits.given

/** 撤下一个策略实例时的收尾语义。 */
class ExecutorLifecycleSpec extends munit.FunSuite:
  private val ex = Exchange.Binance
  private val sym = "BTCUSDT"
  /** 收到首个 BBO 就挂一张限价单 */
  private class OneShotMaker extends Strategy:
    private var placed = false
    def orderTimeoutMs: Long = 0L
    def handlers = StrategyHandlers.empty.market(Topics.Bbo, Instrument(ex, sym)) { (b, ctx, _) =>
      if placed then Vector.empty
      else
        placed = true
        ctx.place(
          Order("", ex, sym, Side.Long, OrderType.Limit(b.bidPrice, TimeInForce.GTC), 0.01, reduceOnly = false, clientOrderId = ""),
          "maker",
        )
    }

  test("撤下策略时先撤掉它挂在交易所的单, 且不平仓"):
    supervised:
      val bus = EventBus()
      val system = ActorSystem(bus)
      val intents = bus.subscribe(Set(Interest.All(OrderIntent)))
      val h = system.spawn(Executor.readyToTrade(OneShotMaker(), AccountId.Live))

      bus.publish(Event.at(Topics.Bbo, BBO(ex, sym, 100.0, Coin(1.0), 100.1, Coin(1.0), 0L), 0L))
      val placed = intents.events.receive().as(OrderIntent).get.outcome match
        case OutcomeEvent.PlaceOrders(orders, _) => orders.head
        case other                               => fail(s"expected PlaceOrders, got $other")

      // 交易所确认挂单 (给它一个 orderId) —— 只有已确认的单才撤得掉
      bus.publish(Event.local(
        Topics.OrderUpdate,
        OrderUpdate(AccountId.Live, "EX-1", Some(placed.clientOrderId), ex, sym, Side.Long, OrderStatus.Pending, 100.0, Coin(0.01), Coin(0.0), 0L),
      ))

      system.stop(h)
      assertEquals(firstCancel(bus, intents), OutcomeEvent.CancelOrder(ex, sym, OrderRef.ByExchangeId("EX-1")), "收尾必须撤掉已确认的挂单")

  test("在途单 (交易所尚未确认) 按 clientOrderId 撤 —— 否则会留下无主挂单"):
    // 策略撤下后没有"下一次收尾"(它已退订), 超时检测也随它停了。
    // 跳过在途单等于把一张 GTC 单留在交易所无人跟踪。
    supervised:
      val bus = EventBus()
      val system = ActorSystem(bus)
      val intents = bus.subscribe(Set(Interest.All(OrderIntent)))
      val h = system.spawn(Executor.readyToTrade(OneShotMaker(), AccountId.Live))

      bus.publish(Event.at(Topics.Bbo, BBO(ex, sym, 100.0, Coin(1.0), 100.1, Coin(1.0), 0L), 0L))
      val placed = intents.events.receive().as(OrderIntent).get.outcome match
        case OutcomeEvent.PlaceOrders(orders, _) => orders.head
        case other                               => fail(s"expected PlaceOrders, got $other")
      // 不推 OrderUpdate: 订单停留在 Created, 本地没有交易所 id

      system.stop(h)
      assertEquals(
        firstCancel(bus, intents),
        OutcomeEvent.CancelOrder(ex, sym, OrderRef.ByClientId(placed.clientOrderId)),
        "在途单必须按 clientOrderId 撤",
      )

  private val sentinel = OutcomeEvent.CancelOrder(ex, "SENTINEL", OrderRef.ByClientId("s"))

  /** 取下一条撤单信号。先发哨兵作栅栏 —— 撤单若没发出，读到的是哨兵而不是永久挂死测试进程 */
  private def firstCancel(bus: EventBus, intents: EventBus.Mailbox): OutcomeEvent.CancelOrder =
    bus.publish(Event.local(OrderIntent, AccountOutcome(AccountId.Live, sentinel)))
    val got = Iterator
      .continually(intents.events.receive().as(OrderIntent).map(_.outcome))
      .collect { case Some(c: OutcomeEvent.CancelOrder) => c }
      .next()
    assertNotEquals(got, sentinel, "没有收到撤单信号 (先读到了哨兵)")
    got

  test("没有挂单时撤下策略不产出任何信号"):
    supervised:
      val bus = EventBus()
      val system = ActorSystem(bus)
      val intents = bus.subscribe(Set(Interest.All(OrderIntent)))
      val h = system.spawn(Executor.readyToTrade(OneShotMaker(), AccountId.Live))
      system.stop(h)
      // 哨兵作栅栏: 若收尾误发了信号, 先读到的会是它而不是哨兵
      bus.publish(Event.local(OrderIntent, AccountOutcome(AccountId.Live, sentinel)))
      assertEquals(intents.events.receive().as(OrderIntent).map(_.outcome), Some(sentinel))
