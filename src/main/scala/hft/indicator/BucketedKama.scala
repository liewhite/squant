package hft.indicator

import hft.domain.Timestamp

/** 对一条**任意标量序列**按固定时间桶做 KAMA —— 桶内取最后一个样本作为该桶的值，
  * 桶切换时推进 [[KamaCore]] 一步。
  *
  * 存在的理由：KAMA 的"一步"必须有明确的时间尺度。若来一个样本就推一步，`erPeriod` 窗口的
  * 实际长度就由**采样间隔**隐式决定 —— 上游把轮询从 1s 改成 3s，指标含义悄悄变成三倍尺度，
  * 没有任何一处会编译失败。桶宽显式给出，尺度就是配置里那个数。
  *
  * 与 [[Kama]] 的区别只是"一步是什么"：那边是一根已收盘 K 线，这边是一个已结束的时间桶。
  * 公式同源 ([[KamaCore]])。
  *
  * 预热不足 (已结束的桶 < erPeriod+1，默认即 11 个桶) 时 [[value]] 为 None ——
  * 调用方**必须回退到原始值**，而不是停止动作：这是冷启动，不是"没有信号"。
  *
  * @param periodMs 桶宽 (ms)。这就是"一步"的时间尺度
  */
final class BucketedKama(val periodMs: Long, erPeriod: Int = 10, fast: Int = 2, slow: Int = 30):
  require(periodMs > 0, s"桶宽须 > 0ms, 实为 $periodMs")

  private val core = KamaCore(erPeriod, fast, slow)
  private var curBucket: Long = Long.MinValue
  private var curLast: Double = 0.0

  /** 喂一个样本。跨桶时先用上一桶的最后一个样本推进一步 (成交/推送稀疏时不补空桶，
    * 与 [[KlineSeries]] "按有数据的 bar 推进"的口径一致)。 */
  def update(ts: Timestamp, x: Double): Unit =
    val bucket = ts / periodMs
    if curBucket == Long.MinValue then curBucket = bucket
    else if bucket > curBucket then
      core.step(curLast)
      curBucket = bucket
    curLast = x

  /** 当前 KAMA (已结束的桶)；预热不足 -> None，调用方回退到原始值 */
  def value: Option[Double] = core.value

  /** 当前效率比 ER ∈ [0,1]：→1 趋势、→0 震荡。预热不足 -> None */
  def efficiencyRatio: Option[Double] = core.efficiencyRatio
