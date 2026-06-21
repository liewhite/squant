package hft.indicator

import hft.option.BlackScholes

/** 实现波动 (Realized Volatility) —— 混入 [[KlineSeries]] 的可叠加 trait。
  *
  * 维护逐根**已收盘** bar 的对数收益环形序列，按短/长两个窗口给出年化实现波动，并以
  * `volRatio = rvShort / rvLong` 度量近端波动是否**放大**(>1 放大、<1 平静)。供对冲带按
  * 波动体制收放 (放大时收窄、更频繁对冲；平静时放宽、减少摩擦)。
  *
  * 年化口径与 [[hft.backtest.BsGreeksSource]]/Black-Scholes 一致 (同一 [[BlackScholes.MillisPerYear]]
  * 年基准)，故指标 RV 与定价 IV 可直接比较。预热不足 (收益数 < 对应窗口) 时返回 None。
  */
trait RealizedVol extends KlineSeries:
  /** 近端窗口 (根)：默认 24 (1h bar -> 1 天) */
  protected def rvShortBars: Int = 24
  /** 基线窗口 (根)：默认 168 (1h bar -> 1 周) */
  protected def rvLongBars: Int = 168

  private val returns = RingSeries(math.max(rvShortBars, rvLongBars))
  private var prevClose = Double.NaN
  private val barsPerYear = BlackScholes.MillisPerYear / periodMs

  abstract override protected def onBarClosed(bar: Candle): Unit =
    super.onBarClosed(bar)
    if !prevClose.isNaN && prevClose > 0.0 && bar.close > 0.0 then
      returns.push(math.log(bar.close / prevClose))
    prevClose = bar.close

  /** 近端年化实现波动 (收益数 < rvShortBars -> None) */
  def rvShort: Option[Double] =
    if returns.size < rvShortBars then None
    else Some(RealizedVol.annualized(returns.recent(rvShortBars), barsPerYear))

  /** 基线年化实现波动 (收益数 < rvLongBars -> None) */
  def rvLong: Option[Double] =
    if returns.size < rvLongBars then None
    else Some(RealizedVol.annualized(returns.recent(rvLongBars), barsPerYear))

  /** 波动放大比 = rvShort / rvLong (二者均就绪且 rvLong>0 时；否则 None) */
  def volRatio: Option[Double] =
    for s <- rvShort; l <- rvLong if l > 0.0 yield s / l

object RealizedVol:
  /** 年化实现波动 = sqrt( Σ rᵢ² / tYears )，r=对数收益，tYears = n / barsPerYear。
    * 采用"对零求和"(高频均值≈0) 口径，与 demo 预扫的 RV 公式一致。样本 < 2 返回 0。 */
  def annualized(logReturns: collection.Seq[Double], barsPerYear: Double): Double =
    if logReturns.sizeIs < 2 || barsPerYear <= 0.0 then 0.0
    else
      val sumSq = logReturns.iterator.map(r => r * r).sum
      val tYears = logReturns.size.toDouble / barsPerYear
      if tYears <= 0.0 then 0.0 else math.sqrt(sumSq / tYears)
