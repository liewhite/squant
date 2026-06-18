package hft.indicator

/** KDJ 指标 (随机指标) —— 混入 [[KlineSeries]] 的可叠加 trait，演示在同一 K 线上叠加多指标。
  *
  * RSV = (close − Ln) / (Hn − Ln) × 100，Hn/Ln 为最近 [[kdjPeriod]] 根的最高/最低；
  * K = (1−1/m1)·K_prev + (1/m1)·RSV，D = (1−1/m2)·D_prev + (1/m2)·K，J = 3K − 2D (K/D 播种 50)。
  * 收盘根冻结 K/D，盘中根用冻结 K/D + 当前 (含盘中根) 窗口的 RSV 动态推算。预热不足
  * (已收盘 bar < kdjPeriod) 时 [[kdjDirection]] 返回 0。
  */
trait Kdj extends KlineSeries:
  protected def kdjPeriod: Int = 9
  protected def kdjM1: Int = 3 // K 平滑
  protected def kdjM2: Int = 3 // D 平滑

  private var fK = 50.0
  private var fD = 50.0
  private var closedCount = 0
  private var dK = 50.0
  private var dD = 50.0
  private var dJ = 0.0

  // 逐根已收盘的 K/D/J 历史
  private val kHistory = RingSeries(maxBars)
  private val dHistory = RingSeries(maxBars)
  private val jHistory = RingSeries(maxBars)

  private def rsv(window: collection.Seq[Candle]): Double =
    val hi = window.map(_.high).max
    val lo = window.map(_.low).min
    if hi == lo then 50.0 else (window.last.close - lo) / (hi - lo) * 100.0

  abstract override protected def onBarClosed(bar: Candle): Unit =
    super.onBarClosed(bar)
    val r = rsv(bars.takeRight(kdjPeriod)) // bars 末尾即刚收盘的 bar
    fK = (1.0 - 1.0 / kdjM1) * fK + (1.0 / kdjM1) * r
    fD = (1.0 - 1.0 / kdjM2) * fD + (1.0 / kdjM2) * fK
    closedCount += 1
    kHistory.push(fK)
    dHistory.push(fD)
    jHistory.push(3 * fK - 2 * fD)

  abstract override protected def onBarUpdated(bar: Candle): Unit =
    super.onBarUpdated(bar)
    // 窗口 = 最近 (period-1) 根已收盘 + 当前盘中根
    val r = rsv(bars.takeRight(kdjPeriod - 1).toSeq :+ bar)
    dK = (1.0 - 1.0 / kdjM1) * fK + (1.0 / kdjM1) * r
    dD = (1.0 - 1.0 / kdjM2) * fD + (1.0 / kdjM2) * dK
    dJ = 3 * dK - 2 * dD

  /** (K, D, J) 当前 (含盘中根) 值 */
  def kdjValues: (Double, Double, Double) = (dK, dD, dJ)

  // 逐根已收盘历史 (最旧->最新)
  def kSeries: RingSeries = kHistory
  def dSeries: RingSeries = dHistory
  def jSeries: RingSeries = jHistory

  /** 方向：K 上穿 D 看多 (+1)、K 下穿 D 看空 (-1)、预热不足或持平 0 */
  def kdjDirection: Int =
    if closedCount < kdjPeriod then 0
    else if dK > dD then 1
    else if dK < dD then -1
    else 0
