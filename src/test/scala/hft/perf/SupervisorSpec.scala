package hft.perf

import hft.actor.{Actor, ActorHandle, ActorSystem}
import hft.domain.*
import hft.event.{AnyEvent, Event, EventBus, Interest, Topics}
import hft.state.StateManager
import hft.event.Commands.{OrderIntent, OutcomeEvent}
import hft.strategy.{Strategy, StrategyHandlers}
import ox.supervised

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicReference
import scala.jdk.CollectionConverters.*
import hft.TestUnits.given
import hft.TestCommandSink

/** 监督者：按影子盘战绩起停实盘实例。 */
class SupervisorSpec extends munit.FunSuite:
  private val ex = Exchange.Binance
  private val sym = "BTCUSDT"
  private val inst = Instrument(ex, sym)
  private val meta = SymbolMeta(ex, sym, tickSize = 0.1, sizeStep = 0.001, minOrderSize = 0.001, contractSize = 1.0)
  private val metas = Map((ex, sym) -> meta)
  private val paper = AccountId.Paper(1)

  private class Noop extends Strategy:
    def orderTimeoutMs: Long = 60_000L // 走实盘装配路径, 0 (关闭校验) 只允许在回测/单测里
    def handlers = StrategyHandlers.empty.market(Topics.Bbo, inst) { (_, _, _) => Vector.empty }

  private class Idle extends Actor:
    override def name = "idle"

  private def perf(account: AccountId, pnl: Double, trips: Int, position: Double = 0.0) =
    Performance(account, inst, pnl, 0.0, trips * 2, trips, position, 0L, 0L)

  private def always(d: Decision): PromotionPolicy = _ => d

  private def eventually(cond: => Boolean, what: String): Unit =
    val deadline = System.nanoTime() + 3_000_000_000L
    while System.nanoTime() < deadline && !cond do Thread.sleep(5)
    assert(cond, s"等待超时: $what")

  /** 装配一个监督者，返回 (总线, 已晋升记录, 已撤下记录) */
  private def fixture(policy: PromotionPolicy)(using ox.Ox) =
    val bus = EventBus()
    val system = ActorSystem(bus)
    val promoted = ConcurrentLinkedQueue[Instrument]()
    val demoted = ConcurrentLinkedQueue[ActorHandle]()
    val sup = Supervisor(
      instruments = Seq(inst),
      paperAccount = paper,
      strategyFactory = _ => Noop(),
      policy = policy,
      promoteLive = (i, _) => { promoted.add(i); system.spawn(Idle()) },
      demoteLive = h => demoted.add(h): Unit,
      decideIntervalMs = 0,
    )
    system.spawn(sup)
    (bus, promoted, demoted)

  test("判据说晋升就拉起实盘实例"):
    supervised:
      val (bus, promoted, _) = fixture(always(Decision.Promote))
      bus.publish(Event.local(Performances, perf(paper, pnl = 100.0, trips = 50)))
      bus.publish(Topics.clockAt(1L))
      eventually(promoted.size == 1, "应晋升一次")
      assertEquals(promoted.peek(), inst)

  test("没有影子战绩时不问判据 —— 不拿空数据做决定"):
    supervised:
      val (bus, promoted, _) = fixture(always(Decision.Promote))
      // 只有实盘战绩、没有影子战绩
      bus.publish(Event.local(Performances, perf(AccountId.Live, pnl = 100.0, trips = 50)))
      bus.publish(Topics.clockAt(1L))
      Thread.sleep(80)
      assertEquals(promoted.size, 0)

  test("空窗期不重复晋升 —— 实盘刚拉起、还没有任何成交时"):
    // 防重的判据必须是"句柄在不在", 不能是"有没有实盘战绩": 跟踪器只在第一笔成交后才建账,
    // 晋升到首笔成交之间 (可能几分钟) 战绩一直为空。用战绩判定的话每个节拍都会再拉起一个,
    // 旧句柄被覆盖 —— 那个实例没人能停, 还在真金白银地交易。
    supervised:
      val (bus, promoted, _) = fixture(always(Decision.Promote))
      bus.publish(Event.local(Performances, perf(paper, 100.0, 50)))
      bus.publish(Topics.clockAt(1L))
      eventually(promoted.size == 1, "首次晋升")

      // 关键: 不注入任何实盘战绩 (真实系统在首笔成交前就是这样)
      bus.publish(Topics.clockAt(2L))
      bus.publish(Topics.clockAt(3L))
      Thread.sleep(80)
      assertEquals(promoted.size, 1, "空窗期不该重复晋升")

  test("有实盘战绩后同样不重复晋升"):
    supervised:
      val (bus, promoted, _) = fixture(always(Decision.Promote))
      bus.publish(Event.local(Performances, perf(paper, 100.0, 50)))
      bus.publish(Topics.clockAt(1L))
      eventually(promoted.size == 1, "首次晋升")
      bus.publish(Event.local(Performances, perf(AccountId.Live, 10.0, 5)))
      bus.publish(Topics.clockAt(2L))
      Thread.sleep(80)
      assertEquals(promoted.size, 1, "已在实盘不该再拉起一个")

  test("降级: 撤下实例并平掉残留敞口"):
    supervised:
      val bus = EventBus()
      val system = ActorSystem(bus)
      val promoted = ConcurrentLinkedQueue[Instrument]()
      val demoted = ConcurrentLinkedQueue[ActorHandle]()
      val intents = ConcurrentLinkedQueue[OutcomeEvent]()
      val mailbox = bus.subscribe(Set(Interest.Keyed(OrderIntent, Set(AccountExchange(AccountId.Live, ex)))))
      system.spawn(TestCommandSink("order-sink", OrderIntent, Set(AccountExchange(AccountId.Live, ex))))
      ox.forkDiscard { while true do mailbox.events.receive().as(OrderIntent).foreach(i => intents.add(i.outcome)) }

      // 先晋升, 再降级
      val decision = AtomicReference[Decision](Decision.Promote)
      val sup = Supervisor(
        Seq(inst), paper, _ => Noop(), _ => decision.get(),
        (i, _) => { promoted.add(i); system.spawn(Idle()) },
        h => demoted.add(h): Unit,
        decideIntervalMs = 0,
      )
      system.spawn(sup)

      bus.publish(Event.local(Performances, perf(paper, 100.0, 50)))
      bus.publish(Topics.clockAt(1L))
      eventually(promoted.size == 1, "先晋升")

      // 实盘持有 2 个多头, 降级时要被平掉
      bus.publish(Event.local(Performances, perf(AccountId.Live, -50.0, 10, position = 2.0)))
      decision.set(Decision.Demote)
      bus.publish(Topics.clockAt(2L))

      eventually(demoted.size == 1, "应撤下实盘实例")
      eventually(intents.asScala.exists(_.isInstanceOf[OutcomeEvent.PlaceOrders]), "应发出平仓单")
      val flatten = intents.asScala.collectFirst { case p: OutcomeEvent.PlaceOrders => p.orders.head }.get
      assertEquals(flatten.side, Side.Short, "多头要用卖单平")
      assertEqualsDouble(flatten.quantity.value, 2.0, 1e-9)
      assert(flatten.reduceOnly, "平仓单必须 reduce-only —— 撮合层据此保证只减不增")
      assert(flatten.clientOrderId.nonEmpty, "平仓单也要有 clientOrderId, 否则回报无从关联")

  test("无敞口时降级不发平仓单"):
    supervised:
      val bus = EventBus()
      val system = ActorSystem(bus)
      val intents = ConcurrentLinkedQueue[OutcomeEvent]()
      val mailbox = bus.subscribe(Set(Interest.Keyed(OrderIntent, Set(AccountExchange(AccountId.Live, ex)))))
      system.spawn(TestCommandSink("order-sink", OrderIntent, Set(AccountExchange(AccountId.Live, ex))))
      ox.forkDiscard { while true do mailbox.events.receive().as(OrderIntent).foreach(i => intents.add(i.outcome)) }
      val demoted = ConcurrentLinkedQueue[ActorHandle]()
      val promoted = ConcurrentLinkedQueue[Instrument]()
      val decision = AtomicReference[Decision](Decision.Promote)
      system.spawn(Supervisor(
        Seq(inst), paper, _ => Noop(), _ => decision.get(),
        (i, _) => { promoted.add(i); system.spawn(Idle()) }, h => demoted.add(h): Unit, decideIntervalMs = 0,
      ))

      bus.publish(Event.local(Performances, perf(paper, 100.0, 50)))
      bus.publish(Topics.clockAt(1L))
      eventually(promoted.size == 1, "先晋升") // 必须等晋升落地再改判据, 否则降级无从谈起

      bus.publish(Event.local(Performances, perf(AccountId.Live, -50.0, 10, position = 0.0)))
      decision.set(Decision.Demote)
      bus.publish(Topics.clockAt(2L))

      eventually(demoted.size == 1, "应撤下")
      Thread.sleep(80)
      assertEquals(intents.size, 0, "没有敞口就不该发单")

  test("默认判据永不晋升 —— 框架不替使用者决定何时上真钱"):
    supervised:
      val (bus, promoted, _) = fixture(NeverPromote)
      bus.publish(Event.local(Performances, perf(paper, 1_000_000.0, 9999)))
      bus.publish(Topics.clockAt(1L))
      Thread.sleep(80)
      assertEquals(promoted.size, 0)

  test("降级后跟踪器仍在发布该标的的历史战绩, 不能让实盘状态起死回生"):
    // 跟踪器的账本跨轮存续、按节拍一直发布。若无条件收下这些战绩,
    // "实盘在跑"会恒为真而句柄早已不存在 —— 该标的从此永远无法再晋升,
    // 且判据每说一次 Demote 就空跑一遍降级。
    supervised:
      val bus = EventBus()
      val system = ActorSystem(bus)
      val promoted = ConcurrentLinkedQueue[Instrument]()
      val demoted = ConcurrentLinkedQueue[ActorHandle]()
      val decision = AtomicReference[Decision](Decision.Promote)
      system.spawn(Supervisor(
        Seq(inst), paper, _ => Noop(), _ => decision.get(),
        (i, _) => { promoted.add(i); system.spawn(Idle()) },
        h => demoted.add(h): Unit, decideIntervalMs = 0,
      ))

      bus.publish(Event.local(Performances, perf(paper, 100.0, 50)))
      bus.publish(Topics.clockAt(1L))
      eventually(promoted.size == 1, "第一轮晋升")

      bus.publish(Event.local(Performances, perf(AccountId.Live, -50.0, 10)))
      decision.set(Decision.Demote)
      bus.publish(Topics.clockAt(2L))
      eventually(demoted.size == 1, "降级")

      // 跟踪器继续发布该键的历史战绩 (真实系统就是这样)
      bus.publish(Event.local(Performances, perf(AccountId.Live, -50.0, 10)))
      bus.publish(Topics.clockAt(3L))
      Thread.sleep(50)
      assertEquals(demoted.size, 1, "不该反复空降级")

      // 判据重新说晋升时, 必须还能晋升
      decision.set(Decision.Promote)
      bus.publish(Event.local(Performances, perf(paper, 200.0, 80)))
      bus.publish(Topics.clockAt(4L))
      eventually(promoted.size == 2, "降级后应能再次晋升")

  test("判据看到的是本轮实盘战绩, 不含上一轮"):
    supervised:
      val bus = EventBus()
      val system = ActorSystem(bus)
      val seen = ConcurrentLinkedQueue[Option[Double]]()
      val promoted = ConcurrentLinkedQueue[Instrument]()
      val demoted = ConcurrentLinkedQueue[ActorHandle]()
      val decision = AtomicReference[Decision](Decision.Promote)
      val policy: PromotionPolicy = view =>
        seen.add(view.live.map(_.realizedPnl))
        decision.get()
      system.spawn(Supervisor(
        Seq(inst), paper, _ => Noop(), policy,
        (i, _) => { promoted.add(i); system.spawn(Idle()) },
        h => demoted.add(h): Unit, decideIntervalMs = 0,
      ))

      // 第一轮: 实盘累计到 -30, 然后降级
      // (每次改判据前都要等上一个决定落地 —— publish 是异步的, 否则会改在 actor 读之前)
      bus.publish(Event.local(Performances, perf(paper, 100.0, 50)))
      bus.publish(Topics.clockAt(1L))
      eventually(promoted.size == 1, "第一轮晋升")

      bus.publish(Event.local(Performances, perf(AccountId.Live, -30.0, 10)))
      decision.set(Decision.Demote)
      bus.publish(Topics.clockAt(2L))
      eventually(demoted.size == 1, "第一轮降级")

      // 第二轮: 跟踪器累计到 -20, 本轮实际 +10
      decision.set(Decision.Promote)
      bus.publish(Topics.clockAt(3L))
      eventually(promoted.size == 2, "第二轮晋升")

      decision.set(Decision.Hold)
      bus.publish(Event.local(Performances, perf(AccountId.Live, -20.0, 12)))
      bus.publish(Topics.clockAt(4L))

      eventually(seen.asScala.exists(_.exists(v => math.abs(v - 10.0) < 1e-9)),
        s"本轮实盘战绩应为 +10 (累计 -20 减去晋升基线 -30), 实际看到 ${seen.asScala.toVector}")
