package strategy.scan

import hft.TestUnits.given
import hft.actor.ActorSystem
import hft.domain.*
import hft.event.{AnyEvent, Event, EventBus, Interest, Topics}
import ox.supervised

import scala.collection.mutable

/** 扫描器行为单测：核心是**横截面**那一步 —— 大盘同向不报，单独异动才报。 */
class FlowScannerSpec extends munit.FunSuite:
  private val ex = Exchange.Binance

  private def tradeEv(symbol: Symbol, price: Double, qty: Double, takerBuy: Boolean, ts: Timestamp): AnyEvent =
    Event.stamped(Topics.Trade, MarketTrade(ex, symbol, price, Coin(qty), isBuyerMaker = !takerBuy, ts), ts, ts)

  private def clockEv(ts: Timestamp): AnyEvent = Event.stamped(Topics.Clock, (), ts, ts)

  /** 直接驱动 actor 的 onEvent（不起总线），收集它产出的异动。
    * FlowScanner 的产出是 onEvent 的返回值，故无需并发设施即可断言。 */
  private class Harness(config: FlowScanConfig, r: AnomalyRule = rule):
    val scanner = FlowScanner(ex, config, r)
    private val out = mutable.ArrayBuffer.empty[FlowAnomaly]
    def feed(ev: AnyEvent, now: Timestamp): Unit =
      out ++= scanner.onEvent(ev, now).flatMap(_.as(FlowAnomalies))
    def anomalies: Vector[FlowAnomaly] = out.toVector

  private val cfg = FlowScanConfig(bucketMs = 1000, windowBuckets = 3, baselineSamples = 20, cooldownMs = 10_000)
  private val rule = CrossSectionalMedianRule(residualZ = 4.0, minWindowNotional = 1_000.0, minSymbols = 5)

  private val universe = (1 to 10).map(i => s"SYM${i}USDT").toVector

  /** 喂 n 个桶的平稳双向流量给全部标的，建立基线 */
  private def warmup(h: Harness, buckets: Int, startTs: Timestamp = 0L): Timestamp =
    var t = startTs
    (0 until buckets).foreach { i =>
      universe.foreach { s =>
        h.feed(tradeEv(s, 100.0, 10.0, takerBuy = true, t), t)
        h.feed(tradeEv(s, 100.0, if i % 2 == 0 then 9.8 else 10.2, takerBuy = false, t), t)
      }
      h.feed(clockEv(t), t)
      t += 1000
    }
    t

  test("平稳市场 -> 不报任何异动"):
    val h = Harness(cfg)
    val t = warmup(h, 30)
    h.feed(clockEv(t), t)
    assertEquals(h.anomalies, Vector.empty)

  test("全市场同向爆买 -> 不报 (这是大盘, 不是某个标的独立异动)"):
    val h = Harness(cfg)
    var t = warmup(h, 30)
    // 每个标的都来一记同等规模的主动买
    universe.foreach(s => h.feed(tradeEv(s, 100.0, 5000.0, takerBuy = true, t), t))
    h.feed(clockEv(t), t)
    assertEquals(h.anomalies, Vector.empty, s"大盘齐涨不该报: ${h.anomalies.map(_.symbol)}")

  test("单个标的独立爆买 -> 只报它一个, 方向为 Long"):
    val h = Harness(cfg)
    var t = warmup(h, 30)
    h.feed(tradeEv("SYM3USDT", 100.0, 5000.0, takerBuy = true, t), t)
    h.feed(clockEv(t), t)
    assertEquals(h.anomalies.map(_.symbol), Vector("SYM3USDT"))
    assertEquals(h.anomalies.head.side, Side.Long)
    assert(h.anomalies.head.residualZ > rule.residualZ)

  test("单个标的独立爆卖 -> 方向为 Short"):
    val h = Harness(cfg)
    val t = warmup(h, 30)
    h.feed(tradeEv("SYM7USDT", 100.0, 5000.0, takerBuy = false, t), t)
    h.feed(clockEv(t), t)
    assertEquals(h.anomalies.map(_.symbol), Vector("SYM7USDT"))
    assertEquals(h.anomalies.head.side, Side.Short)

  test("大盘齐涨 + 某标的涨得更凶 -> 只报那个更凶的"):
    val h = Harness(cfg)
    val t = warmup(h, 30)
    universe.foreach(s => h.feed(tradeEv(s, 100.0, 1000.0, takerBuy = true, t), t))
    h.feed(tradeEv("SYM5USDT", 100.0, 20_000.0, takerBuy = true, t), t) // 额外的独立部分
    h.feed(clockEv(t), t)
    assertEquals(h.anomalies.map(_.symbol), Vector("SYM5USDT"))

  test("名义额下限: 枯水标的 z 分再高也不报"):
    val h = Harness(cfg, rule.copy(minWindowNotional = 1_000_000.0))
    val t = warmup(h, 30)
    h.feed(tradeEv("SYM3USDT", 100.0, 5000.0, takerBuy = true, t), t) // 名义额 50 万 < 100 万
    h.feed(clockEv(t), t)
    assertEquals(h.anomalies, Vector.empty)

  test("冷却期内同一标的不重复报"):
    val h = Harness(cfg)
    var t = warmup(h, 30)
    h.feed(tradeEv("SYM3USDT", 100.0, 5000.0, takerBuy = true, t), t)
    h.feed(clockEv(t), t)
    assertEquals(h.anomalies.size, 1)
    // 异动持续, 下一个节拍仍在窗口内, 但处于冷却期
    t += 1000
    h.feed(clockEv(t), t)
    assertEquals(h.anomalies.size, 1, "冷却期内不该重复报同一件事")

  test("标的数不足 -> 不做横截面, 一律不报 (中位数无意义)"):
    val h = Harness(cfg, rule.copy(minSymbols = 50))
    val t = warmup(h, 30)
    h.feed(tradeEv("SYM3USDT", 100.0, 5000.0, takerBuy = true, t), t)
    h.feed(clockEv(t), t)
    assertEquals(h.anomalies, Vector.empty)

  test("只消费本交易所的成交"):
    val h = Harness(cfg)
    val other = Event.stamped(Topics.Trade, MarketTrade(Exchange.Okx, "SYM1USDT", 100.0, Coin(1.0), false, 0L), 0L, 0L)
    h.feed(other, 0L)
    assertEquals(h.scanner.stats.tradesSeen, 0L)
    assertEquals(h.scanner.stats.symbolsTracked, 0)

  test("扫描器不占用任何标的 —— Interest.All 不产生标的归属"):
    val sub = hft.event.Subscription(FlowScanner(ex).interests)
    assertEquals(sub.instruments, Set.empty[Instrument])

  test("挂上真实总线能收到全市场行情 (Interest.All 的投递路径)"):
    supervised {
      val bus = EventBus()
      val system = ActorSystem(bus)
      val scanner = FlowScanner(ex, cfg)
      system.spawn(scanner)
      (1 to 5).foreach(i => bus.publish(tradeEv(s"SYM${i}USDT", 100.0, 1.0, takerBuy = true, 0L)))
      eventually(scanner.stats.tradesSeen == 5L, s"应收到 5 条, 实收 ${scanner.stats.tradesSeen}")
      assertEquals(scanner.stats.symbolsTracked, 5)
    }

  private def eventually(cond: => Boolean, clue: String, timeoutMs: Long = 2000): Unit =
    val deadline = System.currentTimeMillis() + timeoutMs
    while !cond && System.currentTimeMillis() < deadline do Thread.sleep(10)
    assert(cond, clue)
