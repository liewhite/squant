package hft.sim

import hft.actor.ActorSystem
import hft.domain.*
import hft.event.{AnyEvent, Event, EventBus, Interest, Topics}
import hft.event.Commands.{AccountOutcome, OrderIntent, OutcomeEvent}
import ox.supervised

import java.util.concurrent.ConcurrentLinkedQueue
import scala.jdk.CollectionConverters.*
import hft.TestUnits.given

/** 虚拟柜台：与实盘并行的影子账户。 */
class PaperCounterSpec extends munit.FunSuite:
  private val ex = Exchange.Binance
  private val sym = "BTCUSDT"
  private val paper: AccountId.Paper = AccountId.Paper(1)
  /** 无延迟配置：断言不必等时钟 (延迟本身另有用例) */
  private val instant = SimConfig(exchangeToStrategyDelayMs = 0, orderToExchangeDelayMs = 0, initialBalanceUsdt = 10_000.0)
  /** contractSize = 1 的常规标的。影子柜台也按交易所精度对齐 —— 它存在的理由就是预测实盘 */
  private val metas = Map[Symbol, SymbolMeta](sym -> SymbolMeta(ex, sym, tickSize = 0.1, sizeStep = 0.001, minOrderSize = 0.001, contractSize = 1.0))

  private def bbo(bid: Double, ask: Double, ts: Long = 0L) = BBO(ex, sym, bid, Coin(1.0), ask, Coin(1.0), ts)

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
      system.spawn(PaperCounter(paper, ex, instant, metas))

      // 挂一张买单在 bid 下方, 随后行情下穿 -> 成交
      bus.publish(Event.local(OrderIntent, AccountOutcome(paper, buyLimit(99.0, 0.5, "c1"))))
      bus.publish(Event.at(Topics.Bbo, bbo(100.0, 100.1), 1L))
      bus.publish(Event.at(Topics.Bbo, bbo(98.0, 98.1), 2L))

      val fill = eventually(fills)(_.is(Topics.Fill)).as(Topics.Fill).get
      assertEquals(fill.account, paper, "回报必须标影子账户")
      assertEquals(fill.symbol, sym)
      assertEqualsDouble(fill.size.value, 0.5, 1e-12)

  test("不接实盘账户的下单意图"):
    supervised:
      val bus = EventBus()
      val system = ActorSystem(bus)
      val fills = collect(bus, Set(Interest.All(Topics.Fill)))
      system.spawn(PaperCounter(paper, ex, instant, metas))

      // 实盘意图: 柜台不该撮合它
      bus.publish(Event.local(OrderIntent, AccountOutcome(AccountId.Live, buyLimit(99.0, 0.5, "live1"))))
      bus.publish(Event.at(Topics.Bbo, bbo(98.0, 98.1), 1L))
      // 影子意图作栅栏: 它成交了就说明前面那条确实被忽略而不是还没处理
      bus.publish(Event.local(OrderIntent, AccountOutcome(paper, buyLimit(99.0, 0.25, "p1"))))
      bus.publish(Event.at(Topics.Bbo, bbo(97.0, 97.1), 2L))

      val fill = eventually(fills)(_.is(Topics.Fill)).as(Topics.Fill).get
      assertEqualsDouble(fill.size.value, 0.25, 1e-12, "只该成交影子盘那张单")
      assertEquals(fills.asScala.size, 1)

  test("净值随成交变化, 按本账户发布"):
    supervised:
      val bus = EventBus()
      val system = ActorSystem(bus)
      val infos = collect(bus, Set(Interest.All(Topics.AccountInfo)))
      val counter = PaperCounter(paper, ex, instant, metas, equityRefreshMs = 0)
      system.spawn(counter)

      bus.publish(Topics.clockAt(1L))
      val first = eventually(infos)(_.is(Topics.AccountInfo)).as(Topics.AccountInfo).get
      assertEquals(first.account, paper)
      assertEqualsDouble(first.equity, 10_000.0, 1e-9, "起始净值 = 初始资金")

  test("contractSize != 1 也不影响柜台 —— 下单意图与回报都是币本位"):
    // 单位换算的职责在 exchange 边界: 下单意图 (OrderIntent) 按类型即是币本位,
    // 柜台不碰合约张数, 所以 contractSize 取多少都不改变它的成交量。
    // 从前柜台直接消费"已换算成张数"的订单, 在 contractSize != 1 的交易所上
    // 影子盘的成交量/盈亏/仓位整体差一个倍数, 而 Binance contractSize = 1 掩盖着它;
    // 现在这类错配由 Coin / Contracts 两个类型在编译期挡住。
    supervised:
      val bus = EventBus()
      val system = ActorSystem(bus)
      val fills = collect(bus, Set(Interest.All(Topics.Fill)))
      val metas = Map[Symbol, SymbolMeta](sym -> SymbolMeta(ex, sym, tickSize = 0.1, sizeStep = 1.0, minOrderSize = 1.0, contractSize = 0.01))
      system.spawn(PaperCounter(paper, ex, instant, metas))

      bus.publish(Event.local(OrderIntent, AccountOutcome(paper, buyLimit(99.0, 0.03, "c1"))))
      bus.publish(Event.at(Topics.Bbo, bbo(98.0, 98.1), 1L))

      val fill = eventually(fills)(_.is(Topics.Fill)).as(Topics.Fill).get
      assertEqualsDouble(fill.size.value, 0.03, 1e-12, "币进币出, contractSize 不参与")

  test("虚拟柜台占用实盘账户 —— 编译期就写不出来 (不再靠运行时 require)"):
    // AccountId.Paper 在 Scala 3 里本身就是一个类型 (带参数的 enum case 会生成类),
    // 所以约束落在签名上而不是构造函数体里的一句 require。
    assert(compileErrors("PaperCounter(AccountId.Live, ex, instant)").nonEmpty)

  test("下单在途与回报回传都有延迟 —— 否则影子盘系统性偏乐观"):
    supervised:
      val bus = EventBus()
      val system = ActorSystem(bus)
      val fills = collect(bus, Set(Interest.All(Topics.Fill)))
      val delayed = SimConfig(exchangeToStrategyDelayMs = 120, orderToExchangeDelayMs = 120, initialBalanceUsdt = 10_000.0)
      system.spawn(PaperCounter(paper, ex, delayed, metas))

      bus.publish(Event.local(OrderIntent, AccountOutcome(paper, buyLimit(99.0, 0.5, "c1"))))
      // 订单还在途 (120ms 未到)，此刻的下穿行情不该让它成交
      bus.publish(Event.at(Topics.Bbo, bbo(98.0, 98.1), 1L))
      Thread.sleep(60)
      assert(fills.isEmpty, "订单在途期间不该成交")

      // 订单到达后再来一次下穿行情才成交
      Thread.sleep(120)
      bus.publish(Event.at(Topics.Bbo, bbo(97.0, 97.1), 2L))
      val fill = eventually(fills)(_.is(Topics.Fill)).as(Topics.Fill).get
      assertEqualsDouble(fill.size.value, 0.5, 1e-12)
