package hft.indicator

import scala.collection.mutable

/** 简单移动均线 (SMA) —— 混入 [[KlineSeries]] 的可叠加 trait。
  *
  * 取最近 [[smaPeriod]] 根**已收盘** bar 的收盘价均值 (与 [[Macd]] 一致：只用确认值，不含盘中根)。
  * 预热不足 (已收盘 bar < smaPeriod) 时 [[sma]] 返回 None。
  *
  * 周期默认 60，需自定义时混入处覆写：`new KlineSeries(..) with Sma { override protected def smaPeriod = 30 }`
  */
trait Sma extends KlineSeries:
  protected def smaPeriod: Int = 60

  private val window = mutable.Queue.empty[Double]

  abstract override protected def onBarClosed(bar: Candle): Unit =
    super.onBarClosed(bar)
    window.enqueue(bar.close)
    while window.size > smaPeriod do window.dequeue()

  /** SMA 值；已收盘 bar 不足 smaPeriod 根返回 None */
  def sma: Option[Double] =
    if window.size < smaPeriod then None else Some(window.iterator.sum / smaPeriod)
