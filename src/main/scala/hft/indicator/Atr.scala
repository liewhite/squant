package hft.indicator

/** ATR (Average True Range) —— 混入 [[KlineSeries]] 的可叠加 trait，衡量波动幅度。
  *
  * True Range (真实波幅) = max(high−low, |high−prevClose|, |low−prevClose|)，
  * ATR = TR 的 Wilder 平滑 (RMA)：前 [[atrPeriod]] 根用 TR 均值播种，其后
  * `ATR_t = (ATR_{t-1}·(period−1) + TR_t) / period`。
  *
  * 只用**已收盘** bar (不含盘中根)，预热不足 (已收盘 bar < atrPeriod+1) 时 [[atr]] 返回 None。
  * 用途：以"价偏离均线 / ATR"度量趋势的拉伸程度 (越大越超买/超卖，均值回归风险越高)。
  */
trait Atr extends KlineSeries:
  protected def atrPeriod: Int = 14

  private var prevClose: Option[Double] = None
  private var trSum = 0.0     // 播种期 TR 累加
  private var trCount = 0     // 已累计 TR 根数
  private var atrValue: Option[Double] = None

  abstract override protected def onBarClosed(bar: Candle): Unit =
    super.onBarClosed(bar)
    prevClose match
      case None => prevClose = Some(bar.close) // 首根无前收, 无法算 TR
      case Some(pc) =>
        val tr = math.max(bar.high - bar.low, math.max(math.abs(bar.high - pc), math.abs(bar.low - pc)))
        prevClose = Some(bar.close)
        trCount += 1
        atrValue match
          case None =>
            trSum += tr
            if trCount >= atrPeriod then atrValue = Some(trSum / atrPeriod) // 播种: 前 period 根 TR 均值
          case Some(prev) =>
            atrValue = Some((prev * (atrPeriod - 1) + tr) / atrPeriod) // Wilder 平滑

  /** 当前 ATR (预热不足 -> None) */
  def atr: Option[Double] = atrValue
