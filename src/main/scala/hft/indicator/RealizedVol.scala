package hft.indicator

import hft.option.BlackScholes

/** 实现波动 (Realized Volatility) —— 混入 [[KlineSeries]] 的可叠加 trait。
  *
  * 维护逐根**已收盘** bar 的对数收益环形序列，按短/长两个窗口给出年化实现波动，并以
  * `volRatio = rvShort / rvLong` 度量近端波动是否**放大**(>1 放大、<1 平静)。供对冲带按
  * 波动体制收放 (放大时收窄、更频繁对冲；平静时放宽、减少摩擦)。
  *
  * 年化口径与 [[hft.backtest.BsGreeksSource]]/Black-Scholes 一致 (同一 [[BlackScholes.MillisPerYear]]
  * 年基准)，故指标 RV 与定价 IV 可直接比较。
  *
  * ## 「算不出」一律是 None，不是 0
  *
  * 预热不足 (收益数 < 对应窗口) 返回 `None`；[[RealizedVol.annualizedFromPrices]] 等静态入口同样
  * 返回 `Option`。曾经静态入口在样本不足时返回 `0.0`，而 0 是一个**合法的波动率取值**：调用方
  * 分不清「市场静止」与「没数据」，那个 0 会一路流进 Black-Scholes 当 IV，把期权按内在价值定价、
  * 希腊值全部归零，回测报告和实盘对冲都读不出任何异常。
  */
trait RealizedVol extends KlineSeries:
  /** 近端窗口 (根)：默认 24 (1h bar -> 1 天) */
  protected def rvShortBars: Int = 24
  /** 基线窗口 (根)：默认 168 (1h bar -> 1 周) */
  protected def rvLongBars: Int = 168

  // 一个收益算不出波动 (对零求和口径下 n=1 只是 |r| 的缩放, 不是离散度), 窗口至少要两根收益。
  require(rvShortBars >= 2, s"rvShortBars 至少为 2, 实际 $rvShortBars")
  require(rvLongBars >= 2, s"rvLongBars 至少为 2, 实际 $rvLongBars")

  private val returns = RingSeries(math.max(rvShortBars, rvLongBars))
  private var prevClose = Double.NaN
  private val barsPerYear = BlackScholes.MillisPerYear / periodMs

  abstract override protected def onBarClosed(bar: Candle): Unit =
    super.onBarClosed(bar)
    // 收盘价非正只能是上游数据损坏 (永续成交价恒为正)。跳过它会静默缩短年化窗口, 且下一根
    // 也因 prevClose 非正而跟着丢 —— 报出来才找得到坏在哪。
    require(bar.close > 0.0 && !bar.close.isNaN, s"K 线收盘价必须为正: close=${bar.close} openTime=${bar.openTime}")
    if !prevClose.isNaN then returns.push(math.log(bar.close / prevClose))
    prevClose = bar.close

  /** 近端年化实现波动 (收益数 < rvShortBars -> None) */
  def rvShort: Option[Double] =
    if returns.size < rvShortBars then None
    else Some(RealizedVol.annualized(returns.recent(rvShortBars), barsPerYear))

  /** 基线年化实现波动 (收益数 < rvLongBars -> None) */
  def rvLong: Option[Double] =
    if returns.size < rvLongBars then None
    else Some(RealizedVol.annualized(returns.recent(rvLongBars), barsPerYear))

  /** 波动放大比 = rvShort / rvLong。
    *
    * `rvLong == 0` (窗口内每根收益恰好为零) 时比值无定义，返回 None —— 与预热不足同一契约。 */
  def volRatio: Option[Double] =
    for s <- rvShort; l <- rvLong if l > 0.0 yield s / l

object RealizedVol:
  /** 年化实现波动 = sqrt( Σ rᵢ² / tYears )，r=对数收益，tYears = n / barsPerYear。
    * 采用"对零求和"(高频均值≈0) 口径。
    *
    * **前置条件**：至少两个收益、`barsPerYear > 0`。样本不足不是一个可以返回的数值，
    * 调用方应先判断窗口是否就绪 (见 [[RealizedVol.rvShort]]) 或改用返回 `Option` 的
    * [[annualizedFromPrices]]。 */
  def annualized(logReturns: collection.Seq[Double], barsPerYear: Double): Double =
    require(logReturns.sizeIs >= 2, s"年化实现波动至少需要 2 个对数收益, 实际 ${logReturns.size}")
    require(barsPerYear > 0.0, s"barsPerYear 必须为正, 实际 $barsPerYear")
    val sumSq = logReturns.iterator.map(r => r * r).sum
    val tYears = logReturns.size.toDouble / barsPerYear
    math.sqrt(sumSq / tYears)

  /** 由价格序列 (最旧->最新) 直接算年化实现波动: 取相邻对数收益后年化。
    * 框架级复用——各处预扫不再各写一遍 sliding(2)+log+annualized。
    *
    * 收益数不足 2 返回 `None`：调用方拿到的是"这段窗口估不出波动"，而不是一个假的 0。 */
  def annualizedFromPrices(prices: collection.Seq[Double], barsPerYear: Double): Option[Double] =
    val rets = logReturnsOf(prices)
    if rets.sizeIs < 2 then None else Some(annualized(rets, barsPerYear))

  /** 由价格序列算年化实现波动，采用"样本标准差去均值"(n-1) 口径:
    *   sqrt( Σ(rᵢ−r̄)² / (n−1) ) · sqrt(barsPerYear)
    * 与 [[annualizedFromPrices]] 的"对零求和"口径**并存且数值不同**——后者假设高频均值≈0,
    * 本变体显式去均值, 适用于较长周期窗 (逐小时/逐分钟) 的统计口径。
    *
    * 收益数不足 2 返回 `None` (n-1 分母为 0)。 */
  def annualizedSampleStdFromPrices(prices: collection.Seq[Double], barsPerYear: Double): Option[Double] =
    require(barsPerYear > 0.0, s"barsPerYear 必须为正, 实际 $barsPerYear")
    val rets = logReturnsOf(prices)
    if rets.sizeIs < 2 then None
    else
      val mean = rets.sum / rets.size
      val variance = rets.iterator.map { r => val d = r - mean; d * d }.sum / (rets.size - 1)
      Some(math.sqrt(variance) * math.sqrt(barsPerYear))

  /** 相邻对数收益 (最旧->最新)，两套年化口径共享的单一取数实现。
    *
    * 价格非正只能是上游数据损坏，跳过它会在不告知调用方的前提下缩短窗口 —— 直接抛。 */
  private def logReturnsOf(prices: collection.Seq[Double]): Vector[Double] =
    prices.iterator.zipWithIndex.foreach { (p, i) =>
      require(p > 0.0 && !p.isNaN, s"价格序列第 $i 个元素必须为正: $p")
    }
    prices.iterator
      .sliding(2)
      .withPartial(false)
      .map(pair => math.log(pair(1) / pair(0)))
      .toVector
