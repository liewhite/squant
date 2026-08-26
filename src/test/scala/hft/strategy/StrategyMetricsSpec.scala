package hft.strategy

import hft.TestUnits.given
import hft.actor.ActorSystem
import hft.domain.*
import hft.engine.Executor
import hft.event.{Event, EventBus, Interest, Topic, Topics}
import ox.supervised

import java.util.concurrent.ConcurrentLinkedQueue
import scala.jdk.CollectionConverters.*

/** 策略对外输出自定义指标，外部订阅后即可消费。
  *
  * 这是"框架不需要知情"的扩展：指标的 topic、载荷类型、路由键全由使用者定义，
  * `hft.event` / `hft.engine` 一行不改。
  */
class StrategyMetricsSpec extends munit.FunSuite:
  private val ex = Exchange.Binance
  private val sym = "BTCUSDT"
  private val inst = Instrument(ex, sym)
  private val meta = SymbolMeta(ex, sym, 0.1, 0.001, 0.001, 1.0)
  private val metas = Map((ex, sym) -> meta)

  /** 用户域的指标事件 —— 框架对它一无所知 */
  final case class Spread(instrument: Instrument, bps: Double)
  object SpreadMetric extends Topic[Instrument, Spread]("metric.spread"):
    def keyOf(p: Spread): Instrument = p.instrument

  /** 一边看行情一边把自己的价差指标发出去 */
  private class Quoting extends Strategy:
    def orderTimeoutMs: Long = 0L
    def handlers: StrategyHandlers = StrategyHandlers.empty
      .market(Topics.Bbo, inst) { (b, ctx, _) =>
        val bps = (b.askPrice - b.bidPrice).value / b.midPrice.value * 10_000
        Vector(ctx.emit(SpreadMetric, Spread(inst, bps)))
      }

  private def await(cond: => Boolean, what: String): Unit =
    val deadline = System.nanoTime() + 3_000_000_000L
    while System.nanoTime() < deadline && !cond do Thread.sleep(5)
    assert(cond, s"等待超时: $what")

  test("策略发出的自定义指标，外部订阅得到"):
    supervised:
      val bus = EventBus()
      val system = ActorSystem(bus)
      val got = ConcurrentLinkedQueue[Spread]()
      val mailbox = bus.subscribe(Set(Interest.Keyed(SpreadMetric, Set(inst))))
      ox.forkDiscard { while true do mailbox.events.receive().as(SpreadMetric).foreach(got.add) }

      system.spawn(Executor(Quoting(), AccountId.Live))
      bus.publish(Event.at(Topics.Bbo, BBO(ex, sym, 100.0, Coin(1.0), 100.1, Coin(1.0), 0L), 0L))

      await(!got.isEmpty, "应收到指标")
      assertEquals(got.asScala.toVector.map(_.instrument), Vector(inst))
      assert(math.abs(got.peek().bps - 9.995) < 0.01, s"价差 bps=${got.peek().bps}")

  test("另一个策略可以订阅这条指标 —— 策略间经事件通信"):
    supervised:
      val bus = EventBus()
      val system = ActorSystem(bus)
      val seen = ConcurrentLinkedQueue[Double]()

      class Consumer extends Strategy:
        def orderTimeoutMs: Long = 0L
        def handlers: StrategyHandlers = StrategyHandlers.empty
          .custom(SpreadMetric, Set(inst)) { (s, _, _) => seen.add(s.bps); Vector.empty }

      system.spawn(Executor(Quoting(), AccountId.Live))
      // 消费方绑在另一个账户上：自定义事件不带账户维度，两边都收得到
      system.spawn(Executor(Consumer(), AccountId.Paper(1)))
      bus.publish(Event.at(Topics.Bbo, BBO(ex, sym, 100.0, Coin(1.0), 100.1, Coin(1.0), 0L), 0L))

      await(!seen.isEmpty, "另一个策略应收到指标")
      assertEquals(seen.asScala.size, 1)
