package strategy.strategies.crossspread

import hft.TestUnits.given
import hft.domain.*
import strategy.strategies.crossspread.logic.*

/** 检测器的核心承诺：**持续存在的价差不是信号，突然的偏离才是**。
  *
  * 全部用例都脱离引擎、脱离时钟同步跑 —— 逻辑层不碰框架，这就是证明。
  */
class CrossSpreadDetectorSpec extends munit.FunSuite:
  private val bn = Instrument(Exchange.Binance, "AAPLUSDT")
  private val hl = Instrument(Exchange.Hyperliquid, "AAPL")
  private val pair = VenuePair.of("AAPL", bn, hl)

  private val config = CrossSpreadConfig(
    sampleMs = 1000,
    windowSamples = 60,
    minSamples = 10,
    maxQuoteAgeMs = 5_000,
    resetGapMs = 10_000,
    cooldownMs = 30_000,
  )
  private val rule = ZScoreRule(minZ = 4.0, minDeviationBps = 10.0, minSigmaBps = 0.5)

  private def detector(cfg: CrossSpreadConfig = config, r: DislocationRule = rule) =
    CrossSpreadDetector(Vector(pair), cfg, r)

  private def bbo(instrument: Instrument, mid: Double, ts: Timestamp, halfSpread: Double = 0.01): BBO =
    BBO(instrument.exchange, instrument.symbol, mid - halfSpread, 100.0, mid + halfSpread, 100.0, ts)

  /** 喂 n 个采样点，两所价格按给定的函数走。返回全过程产出的异动。 */
  private def feed(
      d: CrossSpreadDetector,
      ticks: Int,
      startTs: Timestamp = 1_000_000L,
  )(priceAt: Int => (Double, Double)): Vector[SpreadDislocation] =
    val out = Vector.newBuilder[SpreadDislocation]
    (0 until ticks).foreach { i =>
      val ts = startTs + i * config.sampleMs
      val (pb, ph) = priceAt(i)
      d.onQuote(bbo(bn, pb, ts))
      d.onQuote(bbo(hl, ph, ts))
      out ++= d.evaluate(ts).dislocations
    }
    out.result()

  // ==================== 核心语义 ====================

  test("持续存在的价差不报警: 30bp 的固定基差稳定存在两百个采样点, 一条信号都没有"):
    val d = detector()
    // 币安恒比 HL 贵 30bp —— 这正是结构性价差的样子
    val alerts = feed(d, 200)(_ => (100.3, 100.0))
    assertEquals(alerts, Vector.empty)

  test("突然大幅偏离才报警: 同样的基差, 跳到 90bp 时报出偏离约 60bp"):
    val d = detector()
    val alerts = feed(d, 60)(i => if i < 50 then (100.3, 100.0) else (100.9, 100.0))
    assertEquals(alerts.size, 1)
    val a = alerts.head
    assertEquals(a.ticker, "AAPL")
    assertEquals(a.rich, bn) // 币安那边被推贵了
    assertEquals(a.cheap, hl)
    assert(a.deviationBps > 50 && a.deviationBps < 70, s"偏离应在 60bp 上下, 实为 ${a.deviationBps}")
    assert(a.z >= 4.0, s"z 应达阈值, 实为 ${a.z}")

  test("方向由谁变贵决定: 便宜的一边突然被推高时, rich/cheap 对调且偏离仍为正"):
    val d = detector()
    val alerts = feed(d, 60)(i => if i < 50 then (100.3, 100.0) else (100.3, 100.9))
    assertEquals(alerts.size, 1)
    val a = alerts.head
    assertEquals(a.rich, hl)
    assertEquals(a.cheap, bn)
    assert(a.deviationBps > 0, s"偏离对外恒为正 (方向已由 rich/cheap 表达), 实为 ${a.deviationBps}")

  test("预热不足不报: 中枢只有几个样本时, 再大的跳变也不下结论"):
    val d = detector()
    val alerts = feed(d, config.minSamples - 1)(i => if i == config.minSamples - 2 then (105.0, 100.0) else (100.3, 100.0))
    assertEquals(alerts, Vector.empty)

  test("当前样本不参与自己的中枢: 否则窗口越短, 异动越大越检测不出"):
    // 窗口恰好等于预热样本数, 跳变样本若入窗再判, 会把中枢拉向自己
    val tight = config.copy(windowSamples = 12, minSamples = 10)
    val d = detector(tight)
    val alerts = feed(d, 12)(i => if i == 11 then (100.9, 100.0) else (100.3, 100.0))
    assertEquals(alerts.size, 1)

  // ==================== 数据质量防线 ====================

  test("一腿报价停更即整对跳过: 不拿陈旧报价配实时报价算价差"):
    // 新鲜度上限设为 2 个采样点, 于是"停更后还能采几拍"是个明确的数字而不是巧合
    val d = detector(config.copy(maxQuoteAgeMs = 2 * config.sampleMs))
    val out = Vector.newBuilder[SpreadDislocation]
    (0 until 60).foreach { i =>
      val ts = 1_000_000L + i * config.sampleMs
      // 币安一路走高, HL 在第 5 个点之后不再报价
      d.onQuote(bbo(bn, 100.3 + i * 0.05, ts))
      if i < 5 then d.onQuote(bbo(hl, 100.0, ts))
      out ++= d.evaluate(ts).dislocations
    }
    assertEquals(out.result(), Vector.empty, "陈旧的一腿配实时的一腿, 会把对方的正常波动算成价差异动")
    assertEquals(d.stats.pairsReady, 0, "只在新鲜度容忍期内多采了两拍, 之后一直没再采样")

  test("采样断档超过阈值即重置窗口: 断档前的中枢不代表断档后的当下"):
    val d = detector()
    // 先喂满窗口, 再跳过一段长于 resetGapMs 的时间, 然后立刻大幅变动
    feed(d, 30)(_ => (100.3, 100.0))
    val resumeTs = 1_000_000L + 30 * config.sampleMs + config.resetGapMs * 3
    d.onQuote(bbo(bn, 100.9, resumeTs))
    d.onQuote(bbo(hl, 100.0, resumeTs))
    assertEquals(d.evaluate(resumeTs).dislocations, Vector.empty, "断档后第一条样本必然'大幅偏离', 不该据此报警")
    assertEquals(d.stats.pairsReady, 0, "窗口已重置, 需要重新预热")

  test("持续差出一个数量级的一对被剔除, 且只报一次"):
    val d = detector()
    // 一张对一股 vs 一张对十股: 每一条样本的价差都约 ln(10) ≈ 23000bp
    val results = (0 until config.minSamples).map { i =>
      val ts = 1_000_000L + i * config.sampleMs
      d.onQuote(bbo(bn, 1000.0, ts))
      d.onQuote(bbo(hl, 100.0, ts))
      d.evaluate(ts)
    }
    assert(results.init.forall(_.rejections.isEmpty), "证据没攒够之前不下结论")
    val rejection = results.last.rejections
    assertEquals(rejection.size, 1)
    assertEquals(rejection.head.pair, pair)
    assertEquals(rejection.head.samples, config.minSamples)
    assertEquals(d.stats.pairsRejected, 1)

    val nextTs = 1_000_000L + config.minSamples * config.sampleMs
    d.onQuote(bbo(bn, 1000.0, nextTs))
    d.onQuote(bbo(hl, 100.0, nextTs))
    assert(d.evaluate(nextTs).isEmpty, "已剔除的对不再参与, 也不重复报")

  test("单个离群样本既不剔除该对, 也不污染中枢"):
    // 稀薄品种偶尔报一次极宽的盘口。拿它做不可逆的剔除, 等于一次坏报价永久丢掉一个价差对;
    // 让它入窗, 则 23000bp 摊进 60 个样本的窗口会把中枢整个拖走, 之后每一条正常样本都"大幅偏离"
    val d = detector()
    val out = Vector.newBuilder[SpreadDislocation]
    (0 until 60).foreach { i =>
      val ts = 1_000_000L + i * config.sampleMs
      val (pb, ph) = if i == 20 then (1000.0, 100.0) else (100.3, 100.0)
      d.onQuote(bbo(bn, pb, ts))
      d.onQuote(bbo(hl, ph, ts))
      out ++= d.evaluate(ts).dislocations
    }
    assertEquals(d.stats.pairsRejected, 0, "一条坏报价不足以给一对下结构性结论")
    assertEquals(out.result(), Vector.empty, "离群样本没进窗口, 中枢仍是那 30bp")
    assertEquals(d.stats.pairsReady, 1, "其余样本照常入窗, 预热正常完成")

  test("一条在范围内的样本清零超限计数: 判据是**连续**超限"):
    val d = detector()
    // 差一条就够: 中间插一条正常样本, 计数归零, 于是永远攒不够
    (0 until config.minSamples * 3).foreach { i =>
      val ts = 1_000_000L + i * config.sampleMs
      val (pb, ph) = if i % (config.minSamples - 1) == 0 then (100.3, 100.0) else (1000.0, 100.0)
      d.onQuote(bbo(bn, pb, ts))
      d.onQuote(bbo(hl, ph, ts))
      d.evaluate(ts): Unit
    }
    assertEquals(d.stats.pairsRejected, 0)

  test("冷却期内不重复报同一件事: 一次异动会持续若干个采样点"):
    val d = detector()
    val alerts = feed(d, 80)(i => if i < 50 then (100.3, 100.0) else (100.9, 100.0))
    assertEquals(alerts.size, 1, s"冷却 ${config.cooldownMs}ms 内只应报一次, 实报 ${alerts.size} 次")

  test("采样节拍由 sampleMs 定义, 不由评估频率定义"):
    // 5 秒内以十倍于 sampleMs 的频率评估 50 次。若每次评估都采样, 窗口早就有 50 个样本、
    // 远超预热所需的 10 个 —— 而均线的时间刻度也就跟着调用频率漂移了。
    val d = detector()
    (0 until 50).foreach { i =>
      val ts = 1_000_000L + i * (config.sampleMs / 10)
      d.onQuote(bbo(bn, 100.3, ts))
      d.onQuote(bbo(hl, 100.0, ts))
      d.evaluate(ts): Unit
    }
    assertEquals(d.stats.pairsReady, 0, "5 秒只该采到 5 个样本, 不足预热的 10 个")

    // 同样是 5 秒, 按节拍再走 5 秒就够了 —— 证明决定权在时长而不在调用次数
    (5 until 10).foreach { i =>
      val ts = 1_000_000L + i * config.sampleMs
      d.onQuote(bbo(bn, 100.3, ts))
      d.onQuote(bbo(hl, 100.0, ts))
      d.evaluate(ts): Unit
    }
    assertEquals(d.stats.pairsReady, 1)

  // ==================== 判定规则可替换 ====================

  test("规则是纯函数: 给读数就给结论, 不需要检测器/引擎/时钟"):
    val baseline = SpreadBaseline(meanBps = 30.0, sigmaBps = 2.0, samples = 100)
    assertEquals(rule.detect(SpreadObservation(pair, 32.0, baseline)), None, "1σ 不是异动")
    val verdict = rule.detect(SpreadObservation(pair, 60.0, baseline))
    assert(verdict.isDefined)
    assertEqualsDouble(verdict.get.deviationBps, 30.0, 1e-9)
    assertEqualsDouble(verdict.get.z, 15.0, 1e-9)

  test("σ 有下限: 报价钉死不动的清淡时段, 一次正常换价不该打出无穷个 σ"):
    val flat = SpreadBaseline(meanBps = 0.0, sigmaBps = 0.0, samples = 100)
    val verdict = rule.detect(SpreadObservation(pair, 5.0, flat))
    assertEquals(verdict, None, "偏离 5bp 达不到 minDeviationBps, 无论 z 有多大")
    assert(rule.detect(SpreadObservation(pair, 100.0, flat)).isDefined, "真正大的偏离仍要报")

  test("规则可替换: 换一个实现就换了判法, 观测层与驱动层一行不动"):
    val everything = new DislocationRule:
      def detect(o: SpreadObservation): Option[SpreadVerdict] = Some(SpreadVerdict(o.spreadBps - o.baseline.meanBps, 0.0))
    val d = detector(config, everything)
    val alerts = feed(d, 30)(_ => (100.3, 100.0))
    assert(alerts.nonEmpty, "换成'什么都报'的规则就该报出来")

  // ==================== 配对与顺序 ====================

  test("价差对的两腿顺序固定: 同一对无论以什么顺序构造都相等"):
    assertEquals(VenuePair.of("AAPL", bn, hl), VenuePair.of("AAPL", hl, bn))

  test("同所两腿不构成价差对"):
    intercept[IllegalArgumentException](VenuePair.of("AAPL", bn, Instrument(Exchange.Binance, "AAPLUSDT2")))

  test("配对只在至少两家上市时产生, 三家上市则给出全部三组两两组合"):
    val listings = Map(
      Exchange.Binance -> Map("AAPL" -> "AAPLUSDT", "NVDA" -> "NVDAUSDT"),
      Exchange.Okx -> Map("AAPL" -> "AAPL"),
      Exchange.Hyperliquid -> Map("AAPL" -> "AAPL"),
    )
    val pairs = CrossVenueUniverse.pairs(Set("AAPL", "NVDA", "MSFT"), listings)
    assertEquals(pairs.map(_.ticker).distinct, Vector("AAPL"), "NVDA 只在一家, MSFT 一家都没有")
    assertEquals(pairs.size, 3)
    assertEquals(pairs, pairs.distinct)

  test("配对结果顺序确定: 同一份清单必得同一批对, 启动日志才可对比"):
    val listings = Map(
      Exchange.Binance -> Map("AAPL" -> "AAPLUSDT"),
      Exchange.Okx -> Map("AAPL" -> "AAPL"),
      Exchange.Hyperliquid -> Map("AAPL" -> "AAPL"),
    )
    assertEquals(CrossVenueUniverse.pairs(Set("AAPL"), listings), CrossVenueUniverse.pairs(Set("AAPL"), listings))
