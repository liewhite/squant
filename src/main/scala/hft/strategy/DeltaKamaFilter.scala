package hft.strategy

import hft.indicator.{Kama, KlineSeries}

/** 净 delta 的 1min KAMA 滤波器 —— 把逐 tick 的净 delta 聚合成 1 分钟序列并用 KAMA 自适应平滑，
  * 输出"去高频抖动、保留趋势"的净 delta，供对冲触发判断使用：震荡里 delta 来回抖动被滤平 (少无效对冲)，
  * 趋势里净 delta 持续漂移则被如实跟随 (及时对冲)。
  *
  * 与 [[MacdBiasSignal]] 同构 (复用 [[KlineSeries]] bar 聚合 + 可叠加指标 trait)。预热不足时 [[value]]
  * 返回 None，调用方回退到原始 netDelta。
  */
final class DeltaKamaFilter(
    barIntervalMs: Long = 60_000,
    maxBars: Int = 200,
    erPeriod: Int = 10,
):
  private val series = new KlineSeries(barIntervalMs, maxBars) with Kama:
    override protected def kamaErPeriod: Int = erPeriod

  /** 喂入当前净 delta (以该笔虚拟时间聚合到 1min bar，分钟内取最后值为收盘) */
  def update(now: Long, netDelta: Double): Unit =
    series.update(now, netDelta)

  /** 当前 1min KAMA 平滑后的净 delta；预热不足 -> None */
  def value: Option[Double] = series.kama
