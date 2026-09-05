package strategy.strategies.crossspread.logic

import hft.domain.{BBO, Instrument, Price, Timestamp}

import scala.collection.mutable

/** 观测参数 —— **怎么看**：采样节拍、窗口长度、报价新鲜度。**怎么判**在 [[DislocationRule]] 里。
  *
  * @param sampleMs       采样间隔。均线的时间刻度由它定义 (窗口时长 = sampleMs × windowSamples)
  * @param windowSamples  均线窗口的样本数
  * @param minSamples     预热样本数：不足则一条都不报 —— 拿几个样本估出来的中枢去判"偏离"，
  *                       报出来的是噪声不是信号
  * @param maxQuoteAgeMs  报价新鲜度上限。超过它的一腿视为停更，本轮不采样也不判定；
  *                       否则一边的陈旧报价配上另一边的实时报价，会把**对方的正常波动**
  *                       算成价差异动
  * @param resetGapMs     采样断档超过它就丢弃整个窗口重新预热。断档期间价差可能已经整体
  *                       移位，拿断档前的中枢判断当下，第一条样本几乎必然"大幅偏离"
  * @param cooldownMs     同一对两次报警的最小间隔。一次异动会持续若干个采样点，
  *                       不设冷却就会每个节拍重复报同一件事
  * @param maxSpreadBps   单条样本的价差绝对值上限。超出它的样本**不是一次可用的测量**，
  *                       不入窗；连续 [[minSamples]] 条都超出才认定这一对配错了，
  *                       见 [[PairRejection]]
  */
final case class CrossSpreadConfig(
    sampleMs: Long = 1000,
    windowSamples: Int = 900,
    minSamples: Int = 300,
    maxQuoteAgeMs: Long = 10_000,
    resetGapMs: Long = 30_000,
    cooldownMs: Long = 60_000,
    maxSpreadBps: Double = 2000.0,
):
  require(sampleMs > 0, s"sampleMs 须为正, 实为 $sampleMs")
  require(windowSamples > 1, s"windowSamples 须 > 1, 实为 $windowSamples")
  require(minSamples >= 2 && minSamples <= windowSamples, s"minSamples 须在 [2, $windowSamples] 内, 实为 $minSamples")
  require(maxQuoteAgeMs > 0, s"maxQuoteAgeMs 须为正, 实为 $maxQuoteAgeMs")
  require(resetGapMs >= sampleMs, s"resetGapMs 须 >= sampleMs, 实为 $resetGapMs")
  require(cooldownMs >= 0, s"cooldownMs 不可为负, 实为 $cooldownMs")
  require(maxSpreadBps > 0, s"maxSpreadBps 须为正, 实为 $maxSpreadBps")

  /** 预热完成所需时长 —— 装配方用它告诉人"什么时候开始该信这些信号" */
  def warmupMs: Long = sampleMs * minSamples

/** 一个价差对被判定为**配错了** —— 两条腿的价格持续差出一个数量级意义上的距离。
  *
  * 跨所配对靠标的代码相同，而代码会撞：某所的加密永续与另一所的股票永续可能同名，
  * 同一个股票在两所的合约乘数也可能不同 (一张对一股 vs 对十股)。这两种情况下"价差"
  * 不是价差，是两个无关数字相减，其偏离量同样会突然变化 —— 报出来全是假信号。
  *
  * 判据放在价格上而不是命名规则上：命名规则各所各改各的，价格骗不了人。
  *
  * ## 为什么要连续 `minSamples` 条而不是一条
  *
  * "这两个合约是不是同一个标的"是**结构性事实**，那就得用结构性证据去判 ——
  * 一条瞬时报价不是。稀薄品种偶尔报一次极宽的盘口 (Hyperliquid 的股票永续尤其常见)，
  * 拿它做一次**不可逆**的剔除，等于一次坏报价就永久丢掉一个价差对，而且只留一行日志。
  *
  * 反过来，真配错的一对**每一条**样本都超限，攒够 `minSamples` 条只要一个预热的时间，
  * 而那段时间里本来就不出信号 —— 认定晚一点不损失任何东西。
  *
  * `minSamples` 一处定义两处使用，因为它表达的是同一件事：**下结论之前要多少证据**。
  *
  * @param spreadBps 认定时那一条样本的价差
  * @param samples   连续超限的样本数 (= 认定所需的证据量)
  */
final case class PairRejection(pair: VenuePair, spreadBps: Double, samples: Int)

/** 一轮采样的产出：异动信号，以及本轮新认定配错的对 (只在认定的那一次给出)。 */
final case class ScanResult(dislocations: Vector[SpreadDislocation], rejections: Vector[PairRejection]):
  def isEmpty: Boolean = dislocations.isEmpty && rejections.isEmpty

/** 跨所价差异动检测器 —— **纯逻辑**：无框架依赖、无 IO、不读墙钟。
  *
  * 时间一律由调用方传入，因此可脱离引擎逐条喂报价做同步单测，也因此实盘与回放共用同一份
  * 语义。职责边界：本类管**观测与编排** (采样、推进时间、预热、断档、冷却)，
  * 判定是 [[DislocationRule]] 的事。
  *
  * ## 价差为什么取对数比
  *
  * `spread = 1e4 × ln(pa / pb)`。相对量而不是绝对价差，于是不同价位的标的可比；
  * 而且合约乘数差异 (一张对一股还是十股) 在对数下只是一个**常数偏置**，会被均线整个吸收，
  * 偏离量因此与乘数无关。取差再除以某一边则要先决定除以谁，两个方向给出的数还不一样。
  */
final class CrossSpreadDetector(
    val pairs: Vector[VenuePair],
    val config: CrossSpreadConfig = CrossSpreadConfig(),
    rule: DislocationRule = ZScoreRule(),
):
  require(pairs.nonEmpty, "至少要有一个价差对")
  require(pairs.distinct.sizeIs == pairs.size, "价差对不可重复")

  /** 本检测器需要的报价 —— 驱动层据此声明订阅范围 */
  val instruments: Set[Instrument] = pairs.iterator.flatMap(p => Iterator(p.a, p.b)).toSet

  private final class PairState:
    val window: SpreadWindow = SpreadWindow(config.windowSamples)
    var lastSampleAt: Timestamp = Long.MinValue
    var lastAlertAt: Timestamp = Long.MinValue
    var rejected: Boolean = false
    /** 连续超出 [[CrossSpreadConfig.maxSpreadBps]] 的样本数。
      *
      * 只由一条**在范围内**的样本清零，不由采样断档清零：断档不削弱
      * "至今为止每一条样本都超限"这个证据，而报价稀疏的标的本就要攒很久。 */
    var outOfRange: Int = 0

  private val states: Map[VenuePair, PairState] = pairs.map(_ -> PairState()).toMap
  private val quotes = mutable.Map.empty[Instrument, VenueQuote]
  private var lastEvalAt: Timestamp = Long.MinValue
  private var quotesSeen: Long = 0L
  private var alertsEmitted: Long = 0L

  /** 记一条盘口。不在 [[instruments]] 内的直接忽略 —— 驱动层可能订得比这里宽 */
  def onQuote(bbo: BBO): Unit =
    val instrument = Instrument(bbo.exchange, bbo.symbol)
    if instruments.contains(instrument) then
      quotesSeen += 1
      quotes(instrument) = VenueQuote(bbo.bidPrice, bbo.askPrice, bbo.timestamp)

  /** 一个节拍：按固定间隔采样全部价差对，产出异动与新认定的配错对。
    *
    * 采样与判定同拍进行，且**先判定后入窗**：当前这一条样本不参与它自己的中枢，
    * 否则窗口越短、这条样本把中枢往自己身上拉得越多，异动越大越检测不出来。
    */
  def evaluate(now: Timestamp): ScanResult =
    if lastEvalAt != Long.MinValue && now - lastEvalAt < config.sampleMs then ScanResult(Vector.empty, Vector.empty)
    else
      lastEvalAt = now
      val dislocations = Vector.newBuilder[SpreadDislocation]
      val rejections = Vector.newBuilder[PairRejection]
      pairs.foreach { pair =>
        val state = states(pair)
        if !state.rejected then
          freshQuotes(pair, now).foreach { (quoteA, quoteB) =>
            val spreadBps = CrossSpreadDetector.logRatioBps(quoteA.mid, quoteB.mid)
            if math.abs(spreadBps) > config.maxSpreadBps then
              // 超限的样本不入窗: 它不是一次可用的测量, 放进去会把中枢整个拖走。
              // 攒够连续 minSamples 条才认定配错 —— 理由见 PairRejection
              state.outOfRange += 1
              if state.outOfRange >= config.minSamples then
                state.rejected = true
                rejections += PairRejection(pair, spreadBps, state.outOfRange)
            else
              state.outOfRange = 0
              if state.lastSampleAt != Long.MinValue && now - state.lastSampleAt > config.resetGapMs then
                state.window.reset()
              judge(pair, state, quoteA, quoteB, spreadBps, now).foreach { d =>
                state.lastAlertAt = now
                alertsEmitted += 1
                dislocations += d
              }
              state.window.push(spreadBps)
              state.lastSampleAt = now
          }
      }
      ScanResult(dislocations.result(), rejections.result())

  /** 两腿都在且都新鲜才采样。缺一条就整对跳过 —— 半边报价推不出价差 */
  private def freshQuotes(pair: VenuePair, now: Timestamp): Option[(VenueQuote, VenueQuote)] =
    for
      quoteA <- quotes.get(pair.a) if now - quoteA.timestamp <= config.maxQuoteAgeMs
      quoteB <- quotes.get(pair.b) if now - quoteB.timestamp <= config.maxQuoteAgeMs
    yield (quoteA, quoteB)

  private def judge(
      pair: VenuePair,
      state: PairState,
      quoteA: VenueQuote,
      quoteB: VenueQuote,
      spreadBps: Double,
      now: Timestamp,
  ): Option[SpreadDislocation] =
    if state.lastAlertAt != Long.MinValue && now - state.lastAlertAt < config.cooldownMs then None
    else
      state.window.baseline
        .filter(_.samples >= config.minSamples)
        .flatMap { baseline =>
          rule
            .detect(SpreadObservation(pair, spreadBps, baseline))
            .map(verdict => describe(pair, quoteA, quoteB, spreadBps, baseline, verdict, now))
        }

  /** 把带符号的判定摆成"谁贵谁便宜"：偏离为正即 [[VenuePair.a]] 相对变贵。
    * 方向一旦落到 rich/cheap 上，价差、中枢、偏离都随之取同一个方向，读的人不必再对符号。 */
  private def describe(
      pair: VenuePair,
      quoteA: VenueQuote,
      quoteB: VenueQuote,
      spreadBps: Double,
      baseline: SpreadBaseline,
      verdict: SpreadVerdict,
      now: Timestamp,
  ): SpreadDislocation =
    val aIsRich = verdict.deviationBps > 0
    val (rich, cheap) = if aIsRich then (pair.a, pair.b) else (pair.b, pair.a)
    val (richQuote, cheapQuote) = if aIsRich then (quoteA, quoteB) else (quoteB, quoteA)
    val sign = if aIsRich then 1.0 else -1.0
    SpreadDislocation(
      ticker = pair.ticker,
      rich = rich,
      cheap = cheap,
      spreadBps = sign * spreadBps,
      meanBps = sign * baseline.meanBps,
      sigmaBps = baseline.sigmaBps,
      deviationBps = sign * verdict.deviationBps,
      z = sign * verdict.z,
      // 卖贵的一边成交在它的买一, 买便宜的一边成交在它的卖一
      crossEdgeBps = CrossSpreadDetector.logRatioBps(richQuote.bid, cheapQuote.ask),
      richMid = richQuote.mid,
      cheapMid = cheapQuote.mid,
      samples = baseline.samples,
      quoteAgeMs = math.max(now - richQuote.timestamp, now - cheapQuote.timestamp),
      timestamp = now,
    )

  /** 观测用快照：报价条数、跟踪/就绪/被拒的对数、累计报警数。
    * 就绪数用来区分"还没到点"与"接线错了" —— 预热未完成前一条都不会报。 */
  def stats: CrossSpreadDetector.Stats =
    CrossSpreadDetector.Stats(
      quotesSeen = quotesSeen,
      pairsTracked = pairs.size,
      pairsReady = states.count((_, s) => !s.rejected && s.window.size >= config.minSamples),
      pairsRejected = states.count((_, s) => s.rejected),
      alertsEmitted = alertsEmitted,
    )

object CrossSpreadDetector:
  /** 价差的**唯一算法**: 1e4 × ln(a / b)。
    *
    * 取对数比而不是 (a−b)/b: 对数比是**反对称**的 (ln(a/b) = −ln(b/a)), 于是"A 贵于 B"
    * 与"B 便宜于 A"给出同一个绝对值 —— 而百分比差不是, 它随谁做分母而变。价差要进均线与
    * 标准差, 分母一变整个中枢就偏了。
    *
    * `private[crossspread]` 而不是私有: 对敲策略要用**此刻**的盘口重算这个量
    * (见 `ArbPlan.edgeBps`), 而两处各写一遍的结果是同一段行情算出两个价差, 且不会有编译错误。 */
  private[crossspread] def logRatioBps(numerator: Price, denominator: Price): Double =
    1e4 * math.log(numerator.ratioTo(denominator))

  final case class Stats(quotesSeen: Long, pairsTracked: Int, pairsReady: Int, pairsRejected: Int, alertsEmitted: Long)
