package strategy.scan

import hft.TestSim

import hft.TestUnits.given
import hft.backtest.{BacktestEngine, MarketDataSource}
import hft.domain.*
import hft.engine.StrategyRunner
import hft.event.{AnyEvent, Event, Topics}
import hft.sim.SimConfig
import hft.event.Commands.OrderIntent
import hft.strategy.StrategyContext

import scala.collection.mutable

/** 分解出来的三条缝各自能单独测 —— 缝是不是真的存在，靠这个证明。 */
class FlowAnomalyStrategySpec extends munit.FunSuite:
  private val ex = Exchange.Binance
  private val universe = (1 to 10).map(i => s"SYM${i}USDT").toSet
  private val metas: Map[(Exchange, Symbol), SymbolMeta] =
    universe.map(s => (ex, s) -> SymbolMeta(ex, s, tickSize = 0.01, sizeStep = 0.001, minOrderSize = 0.001, contractSize = 1.0)).toMap

  private val cfg = FlowScanConfig(bucketMs = 1000, windowBuckets = 3, baselineSamples = 20, cooldownMs = 10_000)
  private val rule = CrossSectionalMedianRule(residualZ = 4.0, minWindowNotional = 1_000.0, minSymbols = 5)

  private def tradeEv(symbol: Symbol, qty: Double, takerBuy: Boolean, ts: Timestamp): AnyEvent =
    Event.stamped(Topics.Trade, MarketTrade(ex, symbol, 100.0, Coin(qty), isBuyerMaker = !takerBuy, ts), ts, ts)

  // ==================== 缝一：判定规则可脱离一切单独测 ====================

  test("规则是纯函数: 给读数就给结论, 不需要 actor / 引擎 / 时钟"):
    val obs = (1 to 10).map(i => FlowObservation(s"S$i", ownZ = 0.5, windowFlow = 100.0, windowNotional = 10_000.0)).toVector
    assertEquals(rule.detect(obs), Vector.empty) // 全市场一致 -> 无残差
    val withOutlier = obs.updated(3, FlowObservation("S4", ownZ = 20.0, windowFlow = 9e5, windowNotional = 9e5))
    val verdicts = rule.detect(withOutlier)
    assertEquals(verdicts.map(_.symbol), Vector("S4"))
    assertEquals(verdicts.head.side, Side.Long)
    assertEqualsDouble(verdicts.head.marketZ, 0.5, 1e-9)

  test("规则可替换: 换一个实现就换了判法, 不碰观测层与驱动层"):
    // 一个"谁净流向为正就报"的玩具规则 —— 证明 AnomalyRule 这个插座是通的
    val naive = new AnomalyRule:
      def detect(obs: Vector[FlowObservation]): Vector[FlowVerdict] =
        obs.filter(_.windowFlow > 0).map(o => FlowVerdict(o.symbol, Side.Long, o.ownZ, o.ownZ, 0.0))
    val obs = Vector(
      FlowObservation("A", 0.1, windowFlow = 5.0, windowNotional = 1.0),
      FlowObservation("B", 0.1, windowFlow = -5.0, windowNotional = 1.0),
    )
    assertEquals(naive.detect(obs).map(_.symbol), Vector("A"))

  test("横截面: 全市场同向不报, 单独异动才报 (与驱动无关, 纯规则层)"):
    val common = (1 to 9).map(i => FlowObservation(s"S$i", ownZ = 8.0, windowFlow = 1e5, windowNotional = 1e5)).toVector
    assertEquals(rule.detect(common), Vector.empty, "全市场齐涨: 残差为 0")
    val plusOne = common :+ FlowObservation("HOT", ownZ = 30.0, windowFlow = 9e5, windowNotional = 9e5)
    assertEquals(rule.detect(plusOne).map(_.symbol), Vector("HOT"))

  // ==================== 缝二：检测器可脱离框架单独测 ====================

  test("检测器不依赖框架: 直接喂成交与时间即可出异动"):
    val d = TakerFlowDetector(ex, cfg, rule)
    var t = 0L
    (0 until 30).foreach { i =>
      universe.foreach { s =>
        d.onTrade(MarketTrade(ex, s, 100.0, Coin(10.0), isBuyerMaker = false, t))
        d.onTrade(MarketTrade(ex, s, 100.0, Coin(if i % 2 == 0 then 9.8 else 10.2), isBuyerMaker = true, t))
      }
      d.evaluate(t)
      t += 1000
    }
    d.onTrade(MarketTrade(ex, "SYM3USDT", 100.0, Coin(5000.0), isBuyerMaker = false, t))
    assertEquals(d.evaluate(t).map(_.symbol), Vector("SYM3USDT"))

  // ==================== 缝三：响应可替换 ====================

  test("默认响应只出信号不下单"):
    val (anomalies, intents) = runStrategy(AnomalyResponse.SignalOnly)
    assert(anomalies.nonEmpty, "应有异动信号")
    assertEquals(intents, Vector.empty, "SignalOnly 不该产生任何下单意图")

  test("换一个响应就会下单 —— 检测不变, 交易行为变"):
    val buyOnAnomaly = new AnomalyResponse:
      def onAnomaly(a: FlowAnomaly, ctx: StrategyContext, now: Timestamp): Vector[AnyEvent] =
        ctx.place(
          Order("", a.exchange, a.symbol, a.side, OrderType.Market, Coin(0.01), reduceOnly = false, clientOrderId = ""),
          "flow-anomaly",
        )
    val (anomalies, intents) = runStrategy(buyOnAnomaly)
    assertEquals(anomalies.size, 1)
    assertEquals(intents.size, 1, "同一批信号, 换个响应就下单了")

  /** 跑一遍策略, 分别收集它 emit 的异动与产出的下单意图 */
  private def runStrategy(response: AnomalyResponse): (Vector[FlowAnomaly], Vector[AnyEvent]) =
    val strategy = FlowAnomalyStrategy(ex, universe, cfg, rule, response)
    val runner = StrategyRunner.backtest(strategy)
    val anomalies = mutable.ArrayBuffer.empty[FlowAnomaly]
    val intents = mutable.ArrayBuffer.empty[AnyEvent]
    def feed(ev: AnyEvent, now: Timestamp): Unit =
      runner.onEvent(ev, now).foreach { out =>
        out.as(FlowAnomalies).foreach(anomalies += _)
        out.as(OrderIntent).foreach(_ => intents += out)
      }
    var t = 0L
    (0 until 30).foreach { i =>
      universe.foreach { s =>
        feed(tradeEv(s, 10.0, takerBuy = true, t), t)
        feed(tradeEv(s, if i % 2 == 0 then 9.8 else 10.2, takerBuy = false, t), t)
      }
      feed(Event.stamped(Topics.Clock, (), t, t), t)
      t += 1000
    }
    feed(tradeEv("SYM3USDT", 5000.0, takerBuy = true, t), t)
    feed(Event.stamped(Topics.Clock, (), t, t), t)
    (anomalies.toVector, intents.toVector)

  // ==================== 装配契约 ====================

  test("策略形态诚实声明它可能交易的标的 (不是绕过独占登记)"):
    val strategy = FlowAnomalyStrategy(ex, universe, cfg, rule)
    val sub = StrategyRunner.backtest(strategy).subscription
    assertEquals(sub.instruments, universe.map(Instrument(ex, _)))

  test("空宇宙在装配期即被拒"):
    intercept[IllegalArgumentException](FlowAnomalyStrategy(ex, Set.empty, cfg, rule))

  test("这个形态能进回测 —— 固定一组 runner, BacktestEngine 直接收"):
    val strategy = FlowAnomalyStrategy(ex, universe, cfg, rule)
    val runner = StrategyRunner.backtest(strategy)
    // 造一段全市场平稳 + SYM3 独立爆买的行情
    val events = mutable.ArrayBuffer.empty[AnyEvent]
    var t = 1_000L
    (0 until 40).foreach { i =>
      universe.toVector.sorted.foreach { s =>
        events += tradeEv(s, 10.0, takerBuy = true, t)
        events += tradeEv(s, if i % 2 == 0 then 9.8 else 10.2, takerBuy = false, t)
      }
      t += 1000
    }
    events += tradeEv("SYM3USDT", 5000.0, takerBuy = true, t)
    val feed = events.toVector
    val source = new MarketDataSource:
      def events(): Iterator[AnyEvent] = feed.iterator

    val seen = mutable.ArrayBuffer.empty[FlowAnomaly]
    val result = BacktestEngine(
      exchange = ex,
      source = source,
      runners = Seq(runner),
      config = TestSim.noFees.copy(exchangeToStrategyDelayMs = 0, orderToExchangeDelayMs = 0),
      symbolMetas = metas,
      observers = Seq(ev => ev.as(FlowAnomalies).foreach(seen += _)),
    ).run()

    assert(result.marketEvents > 0, "回测应消费到行情")
    assertEquals(seen.map(_.symbol).toVector, Vector("SYM3USDT"), s"回测里应只报 SYM3USDT, 实得 ${seen.map(_.symbol)}")
