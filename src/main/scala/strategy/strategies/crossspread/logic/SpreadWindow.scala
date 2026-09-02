package strategy.strategies.crossspread.logic

import hft.indicator.RingSeries

/** 一个时间窗口内价差的均线与离散度。`samples` 是参与计算的样本数 (预热判据) */
final case class SpreadBaseline(meanBps: Double, sigmaBps: Double, samples: Int)

/** 单个价差对的滚动窗口 —— **纯可变结构**：不含时钟与 IO，采样由调用方按固定节拍推进。
  *
  * 窗口按**样本数**而不是时长表达，因为采样本身是等间隔的 (见 [[CrossSpreadDetector]])：
  * 窗口时长 = 采样间隔 × capacity。等间隔是均线有意义的前提 —— 若按行情到达逐条采样，
  * 活跃时段的一分钟能进上千个样本、清淡时段只有几十个，"最近 N 个样本"对两者根本不是
  * 同一段时间。
  *
  * 均值与方差每次重算而不增量维护：增量的平方和在长时间运行后会被浮点抵消污染
  * (而且没有任何症状，只是 σ 慢慢失真)，而这里一次重算不过几百个 double。
  */
final class SpreadWindow(val capacity: Int):
  require(capacity > 1, s"窗口容量须 > 1, 实为 $capacity")

  private val samples = RingSeries(capacity)

  def push(bps: Double): Unit = samples.push(bps)

  /** 丢弃全部历史 —— 数据断档之后，旧样本已不代表当下的价差中枢 */
  def reset(): Unit = samples.clear()

  def size: Int = samples.size

  /** 当前窗口的均线与标准差 (样本标准差，分母 n−1)。不足 2 个样本时无从谈离散度 */
  def baseline: Option[SpreadBaseline] =
    val values = samples.values
    if values.sizeIs < 2 then None
    else
      val n = values.size
      var sum = 0.0
      values.foreach(sum += _)
      val mean = sum / n
      var sq = 0.0
      values.foreach(v => sq += (v - mean) * (v - mean))
      Some(SpreadBaseline(meanBps = mean, sigmaBps = math.sqrt(sq / (n - 1)), samples = n))
