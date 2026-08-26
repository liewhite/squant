package hft.indicator

/** **效率比 ER** —— 混入 [[KlineSeries]] 的可叠加 trait，把它当体制指标直接用
  * （不需要 [[Kama]] 那条均线本身）。
  *
  * 公式与性质见 [[EfficiencyRatioCore]]。**收盘固定 + 盘中动态**（与 [[Macd]] / [[Kama]]
  * 同一形态）：已收盘 bar 推进真正的一步，盘中根用已收盘窗口加当前价试算。
  *
  * 盘中形态不是可有可无的：只在收盘推进的话，指标在一根 bar 内是**冻结**的 ——
  * 拿它调对冲阈值时，一次跳空最多要等一整根 bar 才反映到判据上。有了它，一次跳空会**立刻**
  * 抬高净位移、从而抬高 ER。
  *
  * 周期默认 10 根，混入处可覆写 [[erPeriod]]。
  */
trait EfficiencyRatio extends KlineSeries:
  protected def erPeriod: Int = 10

  private lazy val erCore = EfficiencyRatioCore(erPeriod)
  private var erLive: Option[Double] = None

  abstract override protected def onBarClosed(bar: Candle): Unit =
    super.onBarClosed(bar)
    erCore.step(bar.close): Unit

  abstract override protected def onBarUpdated(bar: Candle): Unit =
    super.onBarUpdated(bar)
    erLive = erCore.provisional(bar.close)

  /** 当前 ER，**含盘中根**（逐笔更新）。预热不足 -> None，调用方应回退到中性行为。 */
  def efficiencyRatio: Option[Double] = erLive

  /** 已收盘的 ER（确认值，不含盘中根） */
  def efficiencyRatioAtClose: Option[Double] = erCore.value
