package strategy.scan

import hft.domain.Timestamp
import hft.indicator.RingSeries

/** 单个标的的 taker 净流向：时间分桶滚动窗口 + 自身基线。
  *
  * 纯可变结构、单线程使用（归所属 actor 的线程独占），不含时钟与 IO —— 时间一律由调用方传入，
  * 因此可脱离引擎逐笔喂数据做同步单测。
  *
  * ## 为什么按桶而不是按笔
  *
  * 按笔存会让活跃标的（BTC 一分钟上千笔）与冷门标的（一分钟一笔）的窗口长度天差地别，
  * "最近 N 笔"对两者根本不是同一段时间。按固定时长分桶后，窗口对所有标的是同一段时间，
  * 横截面才可比。空桶要显式清零：没有成交就是 0，留着上一轮的值等于凭空造流量。
  *
  * ## 基线为什么取中位数 / MAD
  *
  * 均值与标准差会被我们要找的那种尖峰自己拉高 —— 异动一旦发生就抬高了自己的基线，
  * 于是越异常越检测不出。中位数与 MAD 对少数极端值不敏感，正好适配"找尖峰"。
  *
  * @param bucketMs        分桶时长
  * @param windowBuckets   滚动窗口含多少个桶（窗口时长 = bucketMs × windowBuckets）
  * @param baselineSamples 基线保留多少个历史窗口采样（每跨一个桶边界采一次）
  */
final class SymbolTakerFlow(bucketMs: Long, windowBuckets: Int, baselineSamples: Int):
  require(bucketMs > 0, s"bucketMs must be positive, got $bucketMs")
  require(windowBuckets > 0, s"windowBuckets must be positive, got $windowBuckets")
  require(baselineSamples > 1, s"baselineSamples must be > 1, got $baselineSamples")

  private val buyBuckets = Array.fill(windowBuckets)(0.0)
  private val sellBuckets = Array.fill(windowBuckets)(0.0)
  private var head = 0 // 当前桶在环里的下标
  private var currentBucket = Long.MinValue
  // 增量维护窗口和：每笔 O(1)，跨桶时减掉被挤出的那个桶。否则每笔都要扫一遍窗口。
  private var buySum = 0.0
  private var sellSum = 0.0
  private val baseline = RingSeries(baselineSamples)

  /** 窗口内 taker 净流向（买 − 卖，名义额）。正 = 主动买占优 */
  def windowFlow: Double = buySum - sellSum

  /** 窗口内 taker 双边名义额之和 —— 流动性下限的判据（冷门标的的 z 分不可信） */
  def windowNotional: Double = buySum + sellSum

  def baselineSize: Int = baseline.size

  /** 把时间推进到 `ts` 所属的桶：跨过的桶逐个采样进基线并清零。
    *
    * 每跨一个桶边界采样一次当时的窗口值，于是基线覆盖 `baselineSamples × bucketMs` 这段历史。
    * 长时间无成交的标的会被采进一串接近 0 的样本 —— 这是事实（它确实没有流量），
    * 由此得到的 MAD≈0 会让首笔成交的 z 爆表，故 [[zScore]] 有 MAD 下限、调用方还有名义额下限。
    */
  def advanceTo(ts: Timestamp): Unit =
    val bucket = ts / bucketMs
    if currentBucket == Long.MinValue then currentBucket = bucket
    else if bucket > currentBucket then
      // 跨越超过一个窗口时无需逐桶走：整窗都会被清空，采样同样是一串重复值
      val steps = math.min(bucket - currentBucket, windowBuckets.toLong).toInt
      var i = 0
      while i < steps do
        baseline.push(windowFlow) // 采"离开这个桶之前"的窗口值
        head = (head + 1) % windowBuckets
        buySum -= buyBuckets(head)
        sellSum -= sellBuckets(head)
        buyBuckets(head) = 0.0
        sellBuckets(head) = 0.0
        i += 1
      currentBucket = bucket

  /** 记一笔公共成交。`takerBuy` 为 true 表示主动买（aggTrade 的 isBuyerMaker=false）。 */
  def onTrade(ts: Timestamp, notional: Double, takerBuy: Boolean): Unit =
    advanceTo(ts)
    if takerBuy then
      buyBuckets(head) += notional
      buySum += notional
    else
      sellBuckets(head) += notional
      sellSum += notional

  /** 当前窗口净流向相对**本标的自身**历史的稳健 z 分。
    *
    * 基线样本不足时返回 None —— 冷启动阶段宁可不报，也不要拿三五个样本估出来的尺度去判异常。
    */
  def zScore: Option[Double] =
    if baseline.size < baselineSamples then None
    else
      val samples = baseline.values.toArray
      val med = TakerFlowStats.median(samples)
      val mad = TakerFlowStats.mad(samples, med)
      // MAD 下限：全零基线（长期无成交）会让分母为 0。用窗口名义额的一个极小比例兜底，
      // 使 z 有限；真正防止对枯水标的下结论的是调用方的名义额下限。
      val scale = math.max(TakerFlowStats.MadToSigma * mad, 1e-9)
      Some((windowFlow - med) / scale)

object TakerFlowStats:
  /** 正态分布下 MAD × 1.4826 ≈ 标准差，使 z 分与常见的 σ 口径可比 */
  val MadToSigma = 1.4826

  /** 中位数（会就地排序传入数组的副本语义由调用方保证；此处复制以免意外改动调用方数据） */
  def median(xs: Array[Double]): Double =
    if xs.isEmpty then 0.0
    else
      val a = xs.clone()
      java.util.Arrays.sort(a)
      val n = a.length
      if n % 2 == 1 then a(n / 2) else (a(n / 2 - 1) + a(n / 2)) / 2.0

  /** 绝对中位差 */
  def mad(xs: Array[Double], med: Double): Double =
    if xs.isEmpty then 0.0
    else median(xs.map(x => math.abs(x - med)))
