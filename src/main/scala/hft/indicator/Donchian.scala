package hft.indicator

/** Donchian 通道 —— 混入 [[KlineSeries]] 的可叠加 trait：维护最近 [[donchianBars]] 根**已收盘** bar 的
  * 滚动最高价 [[windowHigh]] / 最低价 [[windowLow]] (突破判据)。
  *
  * 只取**已收盘** bar (不含盘中根)，故"当前价 > windowHigh"即真向上突破近窗高点、"< windowLow"即向下突破——
  * 用当前价对照"过去 N 根"的极值，是标准 Donchian 突破。预热不足 (已收盘 bar < donchianBars) 时
  * 两者返回 None (调用方应视为"无突破信号")。
  *
  * 须 `maxBars >= donchianBars`，否则窗口被 [[KlineSeries]] 的环形缓存截断。
  */
trait Donchian extends KlineSeries:
  protected def donchianBars: Int = 300 // 默认 5h @ 1min bar

  private var hi: Option[Double] = None
  private var lo: Option[Double] = None

  abstract override protected def onBarClosed(bar: Candle): Unit =
    super.onBarClosed(bar)
    if bars.size >= donchianBars then
      val window = bars.takeRight(donchianBars)
      hi = Some(window.iterator.map(_.high).max)
      lo = Some(window.iterator.map(_.low).min)

  /** 近 [[donchianBars]] 根已收盘 bar 的最高价 (预热不足 -> None) */
  def windowHigh: Option[Double] = hi

  /** 近 [[donchianBars]] 根已收盘 bar 的最低价 (预热不足 -> None) */
  def windowLow: Option[Double] = lo
