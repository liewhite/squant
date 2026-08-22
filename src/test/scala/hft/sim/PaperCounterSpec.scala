package hft.sim

import hft.actor.ActorSystem
import hft.domain.*
import hft.event.{AnyEvent, Event, EventBus, Interest, Topics}
import hft.strategy.{AccountOutcome, OrderIntent, OutcomeEvent}
import ox.supervised

import java.util.concurrent.ConcurrentLinkedQueue
import scala.jdk.CollectionConverters.*

/** 虚拟柜台：与实盘并行的影子账户。 */
class PaperCounterSpec extends munit.FunSuite:
  private val ex = Exchange.Binance
  private val sym = "BTCUSDT"
  private val paper = AccountId.Paper(1)
  /** 无延迟配置：断言不必等时钟 (延迟本身另有用例) */
  private val instant = SimConfig(exchangeToStrategyDelayMs = 0, orderToExchangeDelayMs = 0, initialBalanceUsdt = 10_000.0)

  private def bbo(bid: Double, ask: Double, ts: Long = 0L) = BBO(ex, sym, bid, 1.0, ask, 1.0, ts)

  private def buyLimit(px: Double, qty: Double, cid: String) =
    OutcomeEvent.PlaceOrders(
      Vector(Order("", ex, sym, Side.Long, OrderType.Limit(px, TimeInForce.GTC), qty, reduceOnly = false, clientOrderId = cid)),
      "test",
    )

  /** 等到队列里出现满足条件的元素 (柜台经定时器发布，非同步) */
  private def eventually[T](q: ConcurrentLinkedQueue[T])(p: T => Boolean): T =
    val deadline = System.nanoTime() + 3_000_000_000L
    while System.nanoTime() < deadline do
      q.asScala.find(p) match
        case Some(v) => return v
        case None    => Thread.sleep(5)
    fail(s"等待超时，队列内容: ${q.asScala.toVector}")

  private def collect(bus: EventBus, interests: Set[Interest])(using ox.Ox): ConcurrentLinkedQueue[AnyEvent] =
    val q = ConcurrentLinkedQueue[AnyEvent]()
    val mailbox = bus.subscribe(interests)
    ox.forkDiscard { while true do q.add(mailbox.events.receive()) }
    q

  test("影子账户撮合自己的单, 回报标自己的账户"):
    supervised:
      val bus = EventBus()
      val system = ActorSystem(bus)
      val fills = collect(bus, Set(Interest.All(Topics.Fill)))
      system.spawn(PaperCounter(paper, ex, instant))

      // 挂一张买单在 bid 下方, 随后行情下穿 -> 成交
      bus.publish(Event.local(OrderIntent, AccountOutcome(paper, buyLimit(99.0, 0.5, "c1"))))
      bus.publish(Event.at(Topics.Bbo, bbo(100.0, 100.1), 1L))
      bus.publish(Event.at(Topics.Bbo, bbo(98.0, 98.1), 2L))

      val fill = eventually(fills)(_.is(Topics.Fill)).as(Topics.Fill).get
      assertEquals(fill.account, paper, "回报必须标影子账户")
      assertEquals(fill.symbol, sym)
      assertEqualsDouble(fill.size, 0.5, 1e-12)

  test("不接实盘账户的下单意图"):
    supervised:
      val bus = EventBus()
      val system = ActorSystem(bus)
      val fills = collect(bus, Set(Interest.All(Topics.Fill)))
      system.spawn(PaperCounter(paper, ex, instant))

      // 实盘意图: 柜台不该撮合它
      bus.publish(Event.local(OrderIntent, AccountOutcome(AccountId.Live, buyLimit(99.0, 0.5, "live1"))))
      bus.publish(Event.at(Topics.Bbo, bbo(98.0, 98.1), 1L))
      // 影子意图作栅栏: 它成交了就说明前面那条确实被忽略而不是还没处理
      bus.publish(Event.local(OrderIntent, AccountOutcome(paper, buyLimit(99.0, 0.25, "p1"))))
      bus.publish(Event.at(Topics.Bbo, bbo(97.0, 97.1), 2L))

      val fill = eventually(fills)(_.is(Topics.Fill)).as(Topics.Fill).get
      assertEqualsDouble(fill.size, 0.25, 1e-12, "只该成交影子盘那张单")
      assertEquals(fills.asScala.size, 1)

  test("净值随成交变化, 按本账户发布"):
    supervised:
      val bus = EventBus()
      val system = ActorSystem(bus)
      val infos = collect(bus, Set(Interest.All(Topics.AccountInfo)))
      val counter = PaperCounter(paper, ex, instant, equityRefreshMs = 0)
      system.spawn(counter)

      bus.publish(Topics.clockAt(1L))
      val first = eventually(infos)(_.is(Topics.AccountInfo)).as(Topics.AccountInfo).get
      assertEquals(first.account, paper)
      assertEqualsDouble(first.equity, 10_000.0, 1e-9, "起始净值 = 初始资金")

  test("拒绝占用实盘账户"):
    intercept[IllegalArgumentException](PaperCounter(AccountId.Live, ex, instant))

  test("下单在途与回报回传都有延迟 —— 否则影子盘系统性偏乐观"):
    supervised:
      val bus = EventBus()
      val system = ActorSystem(bus)
      val fills = collect(bus, Set(Interest.All(Topics.Fill)))
      val delayed = SimConfig(exchangeToStrategyDelayMs = 120, orderToExchangeDelayMs = 120, initialBalanceUsdt = 10_000.0)
      system.spawn(PaperCounter(paper, ex, delayed))

      bus.publish(Event.local(OrderIntent, AccountOutcome(paper, buyLimit(99.0, 0.5, "c1"))))
      // 订单还在途 (120ms 未到)，此刻的下穿行情不该让它成交
      bus.publish(Event.at(Topics.Bbo, bbo(98.0, 98.1), 1L))
      Thread.sleep(60)
      assert(fills.isEmpty, "订单在途期间不该成交")

      // 订单到达后再来一次下穿行情才成交
      Thread.sleep(120)
      bus.publish(Event.at(Topics.Bbo, bbo(97.0, 97.1), 2L))
      val fill = eventually(fills)(_.is(Topics.Fill)).as(Topics.Fill).get
      assertEqualsDouble(fill.size, 0.5, 1e-12)
