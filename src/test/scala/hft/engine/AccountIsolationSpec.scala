package hft.engine

import hft.actor.ActorSystem
import hft.domain.*
import hft.event.{AnyEvent, Event, EventBus, Interest, Topics}
import hft.state.StateManager
import hft.strategy.{AccountOutcome, OrderIntent, OutcomeEvent, Strategy, StrategyHandlers}
import ox.supervised

import java.util.concurrent.ConcurrentLinkedQueue
import scala.jdk.CollectionConverters.*

/** 账户隔离：同一份策略逻辑跑在实盘与影子账户上，两个实例互不串味。
  *
  * 这是"按影子盘表现起停实盘策略"的地基 —— 两边必须是同一份逻辑、同一组参数、同一份行情，
  * 只有账户不同；而私有回报绝不能串。
  */
class AccountIsolationSpec extends munit.FunSuite:
  private val ex = Exchange.Binance
  private val sym = "BTCUSDT"
  private val inst = Instrument(ex, sym)
  private val meta = SymbolMeta(ex, sym, tickSize = 0.1, sizeStep = 0.001, minOrderSize = 0.001, contractSize = 1.0)
  private val metas = Map((ex, sym) -> meta)
  private val paper = AccountId.Paper(1)

  /** 记录自己看到的成交与仓位；两个实例共用同一份逻辑 */
  private class Recorder(seen: ConcurrentLinkedQueue[String], tag: String) extends Strategy:
    def orderTimeoutMs: Long = 0L
    def handlers = StrategyHandlers.empty
      .market(Topics.Bbo, inst) { (_, _, _) => seen.add(s"$tag:bbo"); Vector.empty }
      .own(Topics.Fill) { (f, _, _) => seen.add(s"$tag:fill:${f.size}"); Vector.empty }

  private def fill(account: AccountId, size: Double) =
    Fill(account, ex, sym, Side.Long, 100.0, size, 0L)

  test("行情两边都收到, 成交只回各自账户"):
    supervised:
      val bus = EventBus()
      val system = ActorSystem(bus)
      val seen = ConcurrentLinkedQueue[String]()
      system.spawn(Executor(Recorder(seen, "live"), metas, AccountId.Live))
      system.spawn(Executor(Recorder(seen, "paper"), metas, paper))

      bus.publish(Event.at(Topics.Bbo, BBO(ex, sym, 100.0, 1.0, 100.1, 1.0, 0L), 0L))
      bus.publish(Event.local(Topics.Fill, fill(AccountId.Live, 1.0)))
      bus.publish(Event.local(Topics.Fill, fill(paper, 2.0)))
      // 用一条两边都收的行情作栅栏，确保前面的都已处理
      bus.publish(Event.at(Topics.Bbo, BBO(ex, sym, 101.0, 1.0, 101.1, 1.0, 1L), 1L))
      while seen.asScala.count(_.endsWith(":bbo")) < 4 do Thread.sleep(5)

      val got = seen.asScala.toVector
      assertEquals(got.count(_ == "live:fill:1.0"), 1, "实盘实例应收到自己的成交")
      assertEquals(got.count(_ == "paper:fill:2.0"), 1, "影子实例应收到自己的成交")
      assertEquals(got.count(_ == "live:fill:2.0"), 0, "实盘不该收到影子盘的成交")
      assertEquals(got.count(_ == "paper:fill:1.0"), 0, "影子盘不该收到实盘的成交")
      assertEquals(got.count(_ == "live:bbo"), 2, "行情无账户归属，一份服务所有账户")
      assertEquals(got.count(_ == "paper:bbo"), 2)

  test("下单意图按账户路由到各自出口"):
    supervised:
      val bus = EventBus()
      val live = bus.subscribe(Set(Interest.Keyed(OrderIntent, Set(AccountId.Live))))
      val shadow = bus.subscribe(Set(Interest.Keyed(OrderIntent, Set(paper))))

      val order = OutcomeEvent.CancelOrder(ex, sym, OrderRef.ByClientId("c1"))
      bus.publish(Event.local(OrderIntent, AccountOutcome(paper, order)))
      bus.publish(Event.local(OrderIntent, AccountOutcome(AccountId.Live, order)))

      // 实盘出口先读到的必须是实盘那条 —— 影子盘那条根本不该进它的邮箱
      assertEquals(live.events.receive().as(OrderIntent).map(_.account), Some(AccountId.Live))
      assertEquals(shadow.events.receive().as(OrderIntent).map(_.account), Some(paper))

  test("同账户同标的不能有两个策略实例"):
    val claims = InstrumentClaims[String]()
    val key = AccountInstrument(AccountId.Live, inst)
    claims.claimAll(Seq(("h1", "strategyA", Set(key))))
    val e = intercept[IllegalStateException](claims.checkAll(Seq(("strategyB", Set(key)))))
    assert(e.getMessage.contains("strategyA"), e.getMessage)

  test("不同账户可以跑同一标的 —— 实盘与影子盘并行的前提"):
    val claims = InstrumentClaims[String]()
    claims.claimAll(Seq(("h1", "live", Set(AccountInstrument(AccountId.Live, inst)))))
    claims.claimAll(Seq(("h2", "shadow", Set(AccountInstrument(paper, inst)))))
    assertEquals(claims.size, 2)

  test("同一批里两个策略互撞也算冲突, 且整批拒绝"):
    val claims = InstrumentClaims[String]()
    val key = AccountInstrument(AccountId.Live, inst)
    intercept[IllegalStateException] {
      claims.claimAll(Seq(("h1", "a", Set(key)), ("h2", "b", Set(key))))
    }
    assertEquals(claims.size, 0, "任一冲突即整批拒绝，不留半登记状态")

  test("撤下后释放占用, 同一标的可以被接管"):
    val claims = InstrumentClaims[String]()
    val key = AccountInstrument(AccountId.Live, inst)
    claims.claimAll(Seq(("h1", "old", Set(key))))
    claims.release("h1")
    claims.release("h1") // 幂等
    claims.claimAll(Seq(("h2", "new", Set(key))))
    assertEquals(claims.ownerOf(key), Some("new"))
