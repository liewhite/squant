package hft.perf

import hft.actor.ActorSystem
import hft.domain.*
import hft.event.{AnyEvent, Event, EventBus, Interest, Topics}
import ox.supervised

import java.util.concurrent.ConcurrentLinkedQueue
import scala.jdk.CollectionConverters.*
import hft.TestUnits.given

/** 绩效重建：实盘与影子走同一段代码，数字因此可比。 */
class PerformanceTrackerSpec extends munit.FunSuite:
  private val ex = Exchange.Binance
  private val sym = "BTCUSDT"
  private val inst = Instrument.perp(ex, sym)
  private val paper = AccountId.Paper(1)

  private def fill(account: AccountId, side: Side, px: Double, qty: Double, ts: Long = 0L) =
    Fill(account, ex, sym, side, px, qty, ts)

  private def feed(t: PerformanceTracker, fills: Fill*): Unit =
    supervised:
      val bus = EventBus()
      ActorSystem(bus).spawn(t)
      fills.foreach(f => bus.publish(Event.local(Topics.Fill, f)))
      // 用一条 Clock 作栅栏并触发发布
      val done = bus.subscribe(Set(Interest.All(Performances)))
      bus.publish(Topics.clockAt(10_000L))
      done.events.receive(): Unit

  test("从成交流重建已实现盈亏, 扣掉按名义费率估算的手续费"):
    val t = PerformanceTracker(feeRate = 0.001, publishIntervalMs = 0)
    feed(t, fill(AccountId.Live, Side.Long, 100.0, 1.0), fill(AccountId.Live, Side.Short, 110.0, 1.0))
    val p = t.snapshot(AccountInstrument(AccountId.Live, inst)).get
    // 毛利 10, 手续费 100*1*0.001 + 110*1*0.001 = 0.21
    assertEqualsDouble(p.realizedPnl, 10.0 - 0.21, 1e-9)
    assertEqualsDouble(p.fees, 0.21, 1e-9)
    assertEquals(p.fills, 2)

  test("往返计数看的是仓位回到零, 不是成交笔数"):
    val t = PerformanceTracker(feeRate = 0.0, publishIntervalMs = 0)
    feed(
      t,
      fill(AccountId.Live, Side.Long, 100.0, 1.0),  // 开
      fill(AccountId.Live, Side.Long, 100.0, 1.0),  // 加仓, 还没回到零
      fill(AccountId.Live, Side.Short, 110.0, 2.0), // 平掉 -> 一次往返
    )
    val p = t.snapshot(AccountInstrument(AccountId.Live, inst)).get
    assertEquals(p.fills, 3)
    assertEquals(p.roundTrips, 1, "三笔成交只构成一次完整的下注结果")
    assertEqualsDouble(p.position.value, 0.0, 1e-12)

  test("实盘与影子各记各的账"):
    val t = PerformanceTracker(feeRate = 0.0, publishIntervalMs = 0)
    feed(
      t,
      fill(AccountId.Live, Side.Long, 100.0, 1.0),
      fill(AccountId.Live, Side.Short, 110.0, 1.0), // 实盘赚 10
      fill(paper, Side.Long, 100.0, 1.0),
      fill(paper, Side.Short, 90.0, 1.0), // 影子亏 10
    )
    assertEqualsDouble(t.snapshot(AccountInstrument(AccountId.Live, inst)).get.realizedPnl, 10.0, 1e-9)
    assertEqualsDouble(t.snapshot(AccountInstrument(paper, inst)).get.realizedPnl, -10.0, 1e-9)

  test("周期发布快照, 按 (账户, 标的) 路由"):
    supervised:
      val bus = EventBus()
      val system = ActorSystem(bus)
      val got = ConcurrentLinkedQueue[Performance]()
      val mailbox = bus.subscribe(Set(Interest.Keyed(Performances, Set(AccountInstrument(paper, inst)))))
      ox.forkDiscard { while true do mailbox.events.receive().as(Performances).foreach(got.add) }

      system.spawn(PerformanceTracker(feeRate = 0.0, publishIntervalMs = 0))
      bus.publish(Event.local(Topics.Fill, fill(paper, Side.Long, 100.0, 1.0)))
      bus.publish(Topics.clockAt(1L))

      val deadline = System.nanoTime() + 3_000_000_000L
      while System.nanoTime() < deadline && got.isEmpty do Thread.sleep(5)
      assertEquals(got.asScala.toVector.map(_.account), Vector(paper))

  test("反手也算一次往返 —— 否则从不落平的策略永远攒不够样本"):
    val t = PerformanceTracker(feeRate = 0.0, publishIntervalMs = 0)
    feed(
      t,
      fill(AccountId.Live, Side.Long, 100.0, 1.0),   // 开多
      fill(AccountId.Live, Side.Short, 110.0, 2.0),  // 反手: 平掉多头(实现 +10) 并开空
      fill(AccountId.Live, Side.Long, 105.0, 1.0),   // 平掉空头
    )
    val p = t.snapshot(AccountInstrument(AccountId.Live, inst)).get
    assertEquals(p.roundTrips, 2, "反手一次 + 归零一次")
