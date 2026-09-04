package hft.indicator

import scala.collection.mutable

/** **效率比 (Kaufman Efficiency Ratio) 的纯标量状态机**。
  *
  * {{{
  *   ER = |x_t − x_{t−n}| / Σ|x_i − x_{i−1}|     ∈ [0,1]
  * }}}
  *
  * 净位移 ÷ 路径长度：→1 说明这段路走得直 (趋势)、→0 说明来回折返 (震荡)。
  * 它衡量的是"有方向的波动"，而不是波动的大小 —— **同一个 ER 可以来自一条陡坡也可以来自
  * 一条缓坡**，这是它的性质，用它做判据时要知道。
  *
  * 抽成独立的核，是因为它当初有两个消费者：KAMA 拿它在快慢平滑系数之间插值，
  * [[EfficiencyRatio]] 直接把它当体制指标用。KAMA 那条均线后来没有消费者、已删除，
  * 只剩 ER 这个读数还在用 (`DeltaHedgeStrategy` 拿它选报价方式)。核留着：状态机本身
  * (收盘推进 / 盘中试算两条路径同一份算术) 与混入 [[KlineSeries]] 的那层是两件事。
  *
  * 预热不足 (推进次数 < period+1) 时返回 None：ER 的定义要跨 n 步的净位移，就得有 n+1 个点。
  */
final class EfficiencyRatioCore(val period: Int):
  require(period >= 1, s"ER 回看周期须 >= 1, 实为 $period")

  private val recent = mutable.ArrayDeque.empty[Double] // 最近 period+1 个样本
  private var lastEr = 0.0
  private var seeded = false

  /** 推进一步 (一根已收盘 bar / 一个已结束的周期)。返回是否算出了新的 ER。 */
  def step(x: Double): Boolean =
    recent += x
    while recent.size > period + 1 do recent.removeHead()
    if !seeded then
      seeded = true // 首个样本只做基线, 不算 ER (此时必预热不足, 陈旧值不外泄)
      false
    else if recent.size >= period + 1 then
      lastEr = ratio(recent.iterator.toIndexedSeq, x)
      true
    else false

  /** **不推进状态**地试算"如果下一步是 x"的 ER —— 供盘中动态值使用。
    *
    * 与 [[step]] 是同一套算术, 只是不落地。试算窗口 = 现有窗口 (超长则丢最旧) ++ [x]，
    * 所以它比已收盘的 ER **早一步就绪**。窗口仍不足 period+1 时 None。
    */
  def provisional(x: Double): Option[Double] =
    if !seeded then None
    else
      val n = recent.size
      val from = if n + 1 > period + 1 then 1 else 0
      if (n - from) + 1 < period + 1 then None
      else
        val window = recent.iterator.drop(from).toIndexedSeq :+ x
        Some(ratio(window, x))

  /** 已收盘的 ER；预热不足 -> None */
  def value: Option[Double] = if ready then Some(lastEr) else None

  /** 是否已预热 (推进次数 >= period+1) */
  def ready: Boolean = recent.size >= period + 1

  /** 窗口内的 ER。`window` 最旧在前、最新在后，`last` 是最新值。 */
  private def ratio(window: IndexedSeq[Double], last: Double): Double =
    val change = math.abs(last - window.head)
    var volatility = 0.0
    var i = 1
    while i < window.size do
      volatility += math.abs(window(i) - window(i - 1))
      i += 1
    if volatility == 0.0 then 0.0 else change / volatility
