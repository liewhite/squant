package hft.indicator

import scala.collection.mutable

/** Kaufman 自适应均线 (KAMA) —— 混入 [[KlineSeries]] 的可叠加 trait。
  *
  * 据"效率比 ER"在快/慢 EMA 平滑系数间自适应：趋势 (净位移≈路径长度, ER→1) 贴近快线、跟得紧；
  * 震荡 (路径来回长、净位移小, ER→0) 贴近慢线、几乎不动。故能滤掉高频抖动而保留趋势位移。
  *
  *   ER = |close_t − close_{t−n}| / Σ|close_i − close_{i−1}|   (n = [[kamaErPeriod]])
  *   SC = (ER·(fastSC − slowSC) + slowSC)²,  fastSC = 2/(fast+1), slowSC = 2/(slow+1)
  *   KAMA_t = KAMA_{t−1} + SC·(close_t − KAMA_{t−1})
  *
  * 仅在**已收盘** bar 上推进 ([[onBarClosed]])，即"一根周期一步"。预热不足 (已收盘 bar < erPeriod+1)
  * 时 [[kama]] 返回 None，调用方应回退到原始值。周期默认 ER=10 / fast=2 / slow=30，混入处可覆写。
  */
trait Kama extends KlineSeries:
  protected def kamaErPeriod: Int = 10
  protected def kamaFast: Int = 2
  protected def kamaSlow: Int = 30

  private val recentCloses = mutable.ArrayDeque.empty[Double] // 最近 erPeriod+1 根收盘 (算 ER)
  private var kamaValue = 0.0
  private var lastEr = 0.0 // 最近一次效率比 (趋势/震荡判据，∈[0,1])
  private var seeded = false

  abstract override protected def onBarClosed(bar: Candle): Unit =
    super.onBarClosed(bar)
    val c = bar.close
    recentCloses += c
    while recentCloses.size > kamaErPeriod + 1 do recentCloses.removeHead()
    if !seeded then
      kamaValue = c // 以首个收盘为基线 (该根不更新 lastEr；此时必预热不足, efficiencyRatio 仍返回 None, 陈旧值不外泄)
      seeded = true
    else if recentCloses.size >= kamaErPeriod + 1 then
      val change = math.abs(c - recentCloses.head)
      var volatility = 0.0
      var i = 1
      while i < recentCloses.size do
        volatility += math.abs(recentCloses(i) - recentCloses(i - 1))
        i += 1
      lastEr = if volatility == 0.0 then 0.0 else change / volatility
      val fastSC = 2.0 / (kamaFast + 1)
      val slowSC = 2.0 / (kamaSlow + 1)
      val sc = math.pow(lastEr * (fastSC - slowSC) + slowSC, 2)
      kamaValue = kamaValue + sc * (c - kamaValue)

  /** 当前 KAMA (已收盘根)。预热不足 (已收盘 bar < erPeriod+1) -> None。 */
  def kama: Option[Double] =
    if recentCloses.size >= kamaErPeriod + 1 then Some(kamaValue) else None

  /** 当前效率比 ER = |净位移|/|路径长度| ∈ [0,1]：→1 趋势 (走直线)、→0 震荡 (来回折返)。
    * 预热不足 -> None。衡量"有方向的波动" (大波动/趋势到来)，而非原始波动率。 */
  def efficiencyRatio: Option[Double] =
    if recentCloses.size >= kamaErPeriod + 1 then Some(lastEr) else None
