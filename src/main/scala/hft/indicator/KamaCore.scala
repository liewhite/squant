package hft.indicator

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
  require(fast >= 1 && slow >= 1, s"kama fast/slow 须 >= 1, 实为 $fast/$slow")

  /** ER 的公式不在这里 —— 它有第二个消费者 ([[EfficiencyRatio]]), 两处各写一份迟早错开 */
  private val er = EfficiencyRatioCore(erPeriod)
  private var kamaValue = 0.0
  private var seeded = false

  private val fastSc = 2.0 / (fast + 1)
  private val slowSc = 2.0 / (slow + 1)

  /** 推进一步 (一根已收盘 bar / 一个已结束的周期) */
  def step(x: Double): Unit =
    val hasEr = er.step(x)
    if !seeded then
      kamaValue = x // 以首个样本为基线
      seeded = true
    else if hasEr then kamaValue = kamaValue + smoothing(er.value.getOrElse(0.0)) * (x - kamaValue)

  /** **不推进状态**地试算"如果下一步是 x"的结果 —— 供盘中动态值使用。
    *
    * 与 [[step]] 是同一套算术，只是不落地，所以盘中值与该值真正收盘后的值在同一个输入上完全
    * 一致。分两份写就会出现"盘中显示的和收盘定下的不是一个数"，而这种偏差没有任何症状 ——
    * 只是判据在收盘那一刻莫名跳一下。
    *
    * @return (KAMA, ER)；试算窗口不足时 None
    */
  def provisional(x: Double): Option[(Double, Double)] =
    if !seeded then None
    else er.provisional(x).map(e => (kamaValue + smoothing(e) * (x - kamaValue), e))

  private def smoothing(erValue: Double): Double =
    math.pow(erValue * (fastSc - slowSc) + slowSc, 2)

  /** 是否已预热 */
  def ready: Boolean = er.ready

  /** 当前 KAMA；预热不足 -> None */
  def value: Option[Double] = if ready then Some(kamaValue) else None

  /** 当前效率比；预热不足 -> None */
  def efficiencyRatio: Option[Double] = er.value
