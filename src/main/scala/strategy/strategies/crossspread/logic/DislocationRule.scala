package strategy.strategies.crossspread.logic

/** 某个价差对在当下的读数 —— [[DislocationRule]] 的**全部**输入。
  *
  * 刻意只是一份快照，不是 [[SpreadWindow]] 本身：规则不该知道中枢是怎么算出来的
  * (等权均线？EWMA？中位数？)，窗口也不该知道谁在拿它做判定。两边各换各的，互不牵动。
  */
final case class SpreadObservation(pair: VenuePair, spreadBps: Double, baseline: SpreadBaseline)

/** 判定结论：这个价差对异动了，偏离量与强度如是。两者都**带符号** ——
  * 正 = [[VenuePair.a]] 相对变贵。方向的表达 (谁贵谁便宜) 是驱动层的事。 */
final case class SpreadVerdict(deviationBps: Double, z: Double)

/** 价差异动判定规则 —— 本模块唯一的**可替换件**。
  *
  * 纯函数：同一份读数必得同一个结论，不持有状态、不看时钟、不碰框架。
  * 换一种判法 = 新增一个实现，窗口层与驱动层一行不动。
  */
trait DislocationRule:
  def detect(observation: SpreadObservation): Option[SpreadVerdict]

/** 默认规则：**偏离均线多少个标准差**，且偏离量本身够大。
  *
  * 两个条件缺一不可，它们防的是两种不同的假信号：
  *   - 只看 z：清淡时段两所报价都钉在同一个 tick 上不动，σ 缩到接近 0，
  *     一次正常的换价就能打出几十个 σ —— 而那点偏离连手续费都不够。
  *   - 只看绝对值：结构性价差本来就大的标的 (流动性差、资金费率差得远) 天天在几十 bp 上
  *     晃，固定阈值会把它的常态报成异动。
  *
  * @param minZ            触发所需的 |偏离| / σ
  * @param minDeviationBps 触发所需的最小绝对偏离 (bp)
  * @param minSigmaBps     σ 的下限。它只保证 z 有限，不承担"够不够大"的判断 ——
  *                        那是 [[minDeviationBps]] 的事
  */
final case class ZScoreRule(
    minZ: Double = 5.0,
    minDeviationBps: Double = 15.0,
    minSigmaBps: Double = 1.0,
) extends DislocationRule:
  require(minZ > 0, s"minZ 须为正, 实为 $minZ")
  require(minDeviationBps > 0, s"minDeviationBps 须为正, 实为 $minDeviationBps")
  require(minSigmaBps > 0, s"minSigmaBps 须为正 (它是 z 的分母下限), 实为 $minSigmaBps")

  override def detect(o: SpreadObservation): Option[SpreadVerdict] =
    val deviation = o.spreadBps - o.baseline.meanBps
    val z = deviation / math.max(o.baseline.sigmaBps, minSigmaBps)
    Option.when(math.abs(z) >= minZ && math.abs(deviation) >= minDeviationBps)(SpreadVerdict(deviation, z))
