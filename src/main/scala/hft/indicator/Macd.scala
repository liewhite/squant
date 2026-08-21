package hft.indicator

/** MACD 指标 —— 混入 [[KlineSeries]] 的可叠加 trait。
  *
  * MACD = EMA(fast) − EMA(slow)，signal = EMA(MACD, signalPeriod)，**柱 = MACD − signal**。
  * 收盘根 ([[onBarClosed]]) 冻结 EMA 基线；盘中根 ([[onBarUpdated]]) 用冻结基线 + 当前盘中收盘价
  * 动态推算。因 freeze 中 fEmaFast/fEmaSlow 先更新再相减，冻结 MACD 与盘中 dMacd 在同一收盘价处
  * 完全一致 (KlineSeriesSpec 有对照参考实现的回归断言)。预热不足 (已收盘 bar < slow+signal) 时
  * [[macdDirection]] 返回 0。
  *
  * 周期默认 12/26/9，需自定义时混入处覆写：`new KlineSeries(..) with Macd { override protected def macdFast = 5 }`
  */
trait Macd extends KlineSeries:
  protected def macdFast: Int = 12
  protected def macdSlow: Int = 26
  protected def macdSignalPeriod: Int = 9

  private var fEmaFast = 0.0
  private var fEmaSlow = 0.0
  private var fEmaSignal = 0.0
  private var closedCount = 0
  private var dMacd = 0.0
  private var dSignal = 0.0
  private var dHist = 0.0

  // 逐根已收盘的 DIF(line)/DEA(signal)/柱(hist) 历史，供回看与单调性判定
  private val lineHistory = RingSeries(maxBars)
  private val signalHistory = RingSeries(maxBars)
  private val histHistory = RingSeries(maxBars)

  abstract override protected def onBarClosed(bar: Candle): Unit =
    super.onBarClosed(bar)
    val c = bar.close
    if closedCount == 0 then
      fEmaFast = c
      fEmaSlow = c
      fEmaSignal = 0.0 // macd@首根 = 0
    else
      fEmaFast = Macd.ema(fEmaFast, c, macdFast)
      fEmaSlow = Macd.ema(fEmaSlow, c, macdSlow)
      fEmaSignal = Macd.ema(fEmaSignal, fEmaFast - fEmaSlow, macdSignalPeriod)
    closedCount += 1
    val line = fEmaFast - fEmaSlow
    lineHistory.push(line)
    signalHistory.push(fEmaSignal)
    histHistory.push(line - fEmaSignal)

  abstract override protected def onBarUpdated(bar: Candle): Unit =
    super.onBarUpdated(bar)
    if closedCount == 0 then
      dMacd = 0.0; dSignal = 0.0; dHist = 0.0
    else
      val df = Macd.ema(fEmaFast, bar.close, macdFast)
      val ds = Macd.ema(fEmaSlow, bar.close, macdSlow)
      dMacd = df - ds
      dSignal = Macd.ema(fEmaSignal, dMacd, macdSignalPeriod)
      dHist = dMacd - dSignal

  // 当前 (含盘中根) 动态值
  def macdLine: Double = dMacd
  def macdSignalLine: Double = dSignal
  def macdHistogram: Double = dHist

  // 逐根已收盘历史 (最旧->最新)，用于回看："macd 柱连续 2 周期递减" = macdHistSeries.falling(2)
  def macdLineSeries: RingSeries = lineHistory
  def macdSignalSeries: RingSeries = signalHistory
  def macdHistSeries: RingSeries = histHistory

  /** 预热是否就绪 (已收盘 bar ≥ slow+signal, EMA 基线可信)。未就绪时方向/柱/DEA 不应被用于决策。 */
  def macdReady: Boolean = closedCount >= macdSlow + macdSignalPeriod

  /** 方向：+1 看多 (柱>0)、-1 看空 (柱<0)、0 预热不足或持平 */
  def macdDirection: Int =
    if closedCount < macdSlow + macdSignalPeriod then 0
    else if dHist > 0 then 1
    else if dHist < 0 then -1
    else 0

  /** 柱的方向强度 = **颜色 (符号) × 趋势 (连续升/降)** -> {-2,-1,0,1,2}：
    *   - 水上 (柱>0) 且连续上升 = +2 (最强多)，水上但回落 = +1；
    *   - 水下 (柱<0) 且连续下降 = -2 (最强空)，水下但反弹 = -1；
    *   - 持平/预热不足 = 0。
    * 颜色取当前 (含盘中) 柱，趋势取最近 [[trendBars]] 根**已收盘**柱 (确认值)。 */
  def histBias(trendBars: Int): Int =
    if closedCount < macdSlow + macdSignalPeriod then 0
    else if dHist > 0 then (if histHistory.rising(trendBars) then 2 else 1)
    else if dHist < 0 then (if histHistory.falling(trendBars) then -2 else -1)
    else 0

object Macd:
  private def ema(prev: Double, x: Double, period: Int): Double =
    val k = 2.0 / (period + 1)
    x * k + prev * (1 - k)
