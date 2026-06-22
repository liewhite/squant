package hft.indicator

/** 趋势信念测量原语 (无状态纯函数) —— 把价格统计量翻译成 [-1,1] 的方向分 / 信念。
  *
  * 设计 (第一性原理)：市面上各类趋势/震荡指标本质都在测两件事——方向 与 质量。最朴素、且自归一化
  * (跨品种跨波动率可比) 的度量是"带噪比"：净位移 / 该尺度下的随机游走波动。
  */
object TrendConviction:
  /** 单周期方向分 ∈ (-1,1)：净 log 位移 / (σ·√h) 再 tanh 压缩。
    * 走直线 (位移远超随机游走尺度) → ±1；来回折返 → 0。kSnr 越大越钝感。 */
  def driftScore(logRet: Double, sigmaPerBar: Double, horizonBars: Int, kSnr: Double): Double =
    if sigmaPerBar <= 0.0 || horizonBars <= 0 || kSnr <= 0.0 then 0.0
    else math.tanh(logRet / (kSnr * sigmaPerBar * math.sqrt(horizonBars.toDouble)))

  /** 多周期方向分加权平均 → 信念 C ∈ [-1,1]。weights 非负, 全 0 → 0。 */
  def convictionOf(weightedScores: Seq[(Double, Double)]): Double =
    val wsum = weightedScores.iterator.map(_._1).sum
    if wsum <= 0.0 then 0.0
    else math.max(-1.0, math.min(1.0, weightedScores.iterator.map((w, s) => w * s).sum / wsum))

/** 趋势信念指标 —— 可叠加 trait (stackable)，混入 [[KlineSeries]]。
  *
  * 收盘 ([[onBarClosed]]) 冻结三样**收盘量**：每根 log 收益的 EWMA 波动 σ、各周期 drift 方向分加权出的
  * 信念 C、短锚 EMA。盘中的 stretch (超买超卖) 由策略用**现价 + 冻结的 σ/anchor** 现算 —— 本 trait
  * 只负责收盘固定量, 与库内其他指标 (Macd/Sma) 的"收盘冻结 + 盘中动态"一致。
  */
trait TrendConviction extends KlineSeries:
  /** (周期 bars, 权重) 列表；长周期权重大 = 定战略偏向，短周期 = 定战术节奏 */
  protected def horizons: Seq[(Int, Double)]
  /** 每根 log 收益 EWMA 波动的周期 (alpha = 2/(n+1)) */
  protected def volPeriod: Int = 72
  /** 短锚 EMA 周期 (stretch 用) */
  protected def anchorPeriod: Int = 6
  /** drift 带噪比钝感系数 */
  protected def kSnr: Double = 1.5

  private var prevClose = 0.0
  private var emaVar = 0.0 // 每根 log 收益平方的 EWMA (= σ²)
  private var anchorEma = 0.0
  private var conv = 0.0
  private var closedCount = 0
  private var warm = false

  private lazy val maxHorizon: Int = if horizons.isEmpty then 0 else horizons.map(_._1).max

  abstract override protected def onBarClosed(bar: Candle): Unit =
    super.onBarClosed(bar)
    val c = bar.close
    if closedCount == 0 then
      anchorEma = c
      emaVar = 0.0
    else
      val r = math.log(c / prevClose)
      val aVol = 2.0 / (volPeriod + 1)
      emaVar = aVol * (r * r) + (1 - aVol) * emaVar
      val aAnc = 2.0 / (anchorPeriod + 1)
      anchorEma = aAnc * c + (1 - aAnc) * anchorEma
    prevClose = c
    closedCount += 1
    // 预热：需足够历史算最长周期 drift，且 σ 已成形
    if closedCount > maxHorizon + 1 && emaVar > 0.0 then
      warm = true
      val sigma = math.sqrt(emaVar)
      val bs = bars // 已收盘 K (最旧->最新)，deque 本体, O(1) 索引, 不复制
      val n = bs.length
      val cNow = bs(n - 1).close
      val ws = horizons.map { (h, w) =>
        val logRet = math.log(cNow / bs(n - 1 - h).close)
        (w, TrendConviction.driftScore(logRet, sigma, h, kSnr))
      }
      conv = TrendConviction.convictionOf(ws)

  /** 信念 C ∈ [-1,1]，预热不足 → None */
  def conviction: Option[Double] = Option.when(warm)(conv)

  /** 每根收益 σ (波动率估计)，预热不足 → None */
  def sigmaPerBar: Option[Double] = Option.when(warm && emaVar > 0.0)(math.sqrt(emaVar))

  /** 短锚 EMA (stretch 基准)，无收盘根 → None */
  def anchor: Option[Double] = Option.when(closedCount > 0)(anchorEma)
