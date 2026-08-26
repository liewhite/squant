package hft.indicator

import scala.collection.mutable

/** Kaufman 自适应均线的**纯标量状态机** —— 与"喂进来的是什么序列"无关。
  *
  * 公式与推导见 [[Kama]]。抽出来是因为它有两个消费者：
  *   - [[Kama]]：混入 [[KlineSeries]]，一根已收盘 bar 推进一步，喂的是收盘价；
  *   - [[BucketedKama]]：按固定时间桶推进，喂的可以是任何标量序列 (如账户净 delta)。
  *
  * 两处若各写一份公式，改了其中一处不会有任何编译错误 —— 它们只是从此对同一段数据
  * 给出不同的平滑结果，而"同一个指标只该有一个定义"正是这里要保证的。
  *
  * 预热不足 (推进次数 < erPeriod+1) 时 [[value]] 与 [[efficiencyRatio]] 均为 None，
  * 调用方应回退到原始值 —— 半成品的平滑值比没有值更危险。
  */
final class KamaCore(erPeriod: Int = 10, fast: Int = 2, slow: Int = 30):
  require(erPeriod >= 1, s"kama erPeriod 须 >= 1, 实为 $erPeriod")
  require(fast >= 1 && slow >= 1, s"kama fast/slow 须 >= 1, 实为 $fast/$slow")

  private val recent = mutable.ArrayDeque.empty[Double] // 最近 erPeriod+1 个样本 (算 ER)
  private var kamaValue = 0.0
  private var lastEr = 0.0
  private var seeded = false

  private val fastSc = 2.0 / (fast + 1)
  private val slowSc = 2.0 / (slow + 1)

  /** 推进一步 (一根已收盘 bar / 一个已结束的时间桶) */
  def step(x: Double): Unit =
    recent += x
    while recent.size > erPeriod + 1 do recent.removeHead()
    if !seeded then
      // 以首个样本为基线；该步不更新 lastEr (此时必预热不足，陈旧值不外泄)
      kamaValue = x
      seeded = true
    else if recent.size >= erPeriod + 1 then
      val change = math.abs(x - recent.head)
      var volatility = 0.0
      var i = 1
      while i < recent.size do
        volatility += math.abs(recent(i) - recent(i - 1))
        i += 1
      lastEr = if volatility == 0.0 then 0.0 else change / volatility
      val sc = math.pow(lastEr * (fastSc - slowSc) + slowSc, 2)
      kamaValue = kamaValue + sc * (x - kamaValue)

  /** **不推进状态**地试算"如果下一步是 x"的结果 —— 供盘中动态值使用。
    *
    * 与 [[step]] 是同一套算术（同一份公式，只是不落地），所以盘中值与该值真正收盘后的值
    * 在同一个输入上完全一致。分两份写就会出现"盘中显示的和收盘定下的不是一个数"，
    * 而这种偏差没有任何症状 —— 只是判据在收盘那一刻莫名跳一下。
    *
    * @return (KAMA, ER)；试算窗口不足 erPeriod+1 个点时 None
    */
  def provisional(x: Double): Option[(Double, Double)] =
    if !seeded then None
    else
      // 试算窗口 = 现有窗口 (超长则丢最旧) ++ [x]
      val n = recent.size
      val from = if n + 1 > erPeriod + 1 then 1 else 0
      if (n - from) + 1 < erPeriod + 1 then None
      else
        val change = math.abs(x - recent(from))
        var volatility = 0.0
        var i = from + 1
        while i < n do
          volatility += math.abs(recent(i) - recent(i - 1))
          i += 1
        volatility += math.abs(x - recent(n - 1))
        val er = if volatility == 0.0 then 0.0 else change / volatility
        val sc = math.pow(er * (fastSc - slowSc) + slowSc, 2)
        Some((kamaValue + sc * (x - kamaValue), er))

  /** 是否已预热 (推进次数 ≥ erPeriod+1) */
  def ready: Boolean = recent.size >= erPeriod + 1

  /** 当前 KAMA；预热不足 -> None */
  def value: Option[Double] = if ready then Some(kamaValue) else None

  /** 当前效率比 ER = |净位移|/|路径长度| ∈ [0,1]：→1 趋势 (走直线)、→0 震荡 (来回折返)。预热不足 -> None */
  def efficiencyRatio: Option[Double] = if ready then Some(lastEr) else None
