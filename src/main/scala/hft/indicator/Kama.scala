package hft.indicator

/** Kaufman 自适应均线 (KAMA) —— 混入 [[KlineSeries]] 的可叠加 trait。
  *
  * 据"效率比 ER"在快/慢 EMA 平滑系数间自适应：趋势 (净位移≈路径长度, ER→1) 贴近快线、跟得紧；
  * 震荡 (路径来回长、净位移小, ER→0) 贴近慢线、几乎不动。故能滤掉高频抖动而保留趋势位移。
  *
  *   ER = |close_t − close_{t−n}| / Σ|close_i − close_{i−1}|   (n = [[kamaErPeriod]])
  *   SC = (ER·(fastSC − slowSC) + slowSC)²,  fastSC = 2/(fast+1), slowSC = 2/(slow+1)
  *   KAMA_t = KAMA_{t−1} + SC·(close_t − KAMA_{t−1})
  *
  * **收盘固定 + 盘中动态**（与 [[Macd]] 同一形态）：已收盘 bar 推进真正的一步 ([[onBarClosed]])；
  * 盘中根用已收盘的基线加当前价**试算**一个值 ([[onBarUpdated]])。所以 [[kama]] / [[efficiencyRatio]]
  * 是**含盘中**的动态值，逐笔都在变；[[kamaAtClose]] / [[efficiencyRatioAtClose]] 是已收盘的确认值。
  *
  * 盘中形态不是可有可无的。只在收盘推进的话，指标在一根 bar 内是**冻结**的 —— 1 分钟粒度下，
  * 一次跳空最多要等 60 秒才能反映到判据上，而拿它做对冲判据时那 60 秒就是裸敞口。
  * 试算走的是 [[KamaCore.provisional]]，与 [[KamaCore.step]] 同一份公式，所以盘中值与它收盘后
  * 定下的值在同一个输入上完全一致。
  *
  * 预热不足时返回 None，调用方应回退到原始值。周期默认 ER=10 / fast=2 / slow=30，混入处可覆写。
  * 公式本身在 [[KamaCore]]。
  */
trait Kama extends KlineSeries:
  protected def kamaErPeriod: Int = 10
  protected def kamaFast: Int = 2
  protected def kamaSlow: Int = 30

  private lazy val core = KamaCore(kamaErPeriod, kamaFast, kamaSlow)
  private var live: Option[(Double, Double)] = None

  abstract override protected def onBarClosed(bar: Candle): Unit =
    super.onBarClosed(bar)
    core.step(bar.close)

  abstract override protected def onBarUpdated(bar: Candle): Unit =
    super.onBarUpdated(bar)
    live = core.provisional(bar.close)

  /** 当前 KAMA，**含盘中根**（逐笔更新）。预热不足 -> None，调用方回退到原始值。 */
  def kama: Option[Double] = live.map(_._1)

  /** 当前效率比 ER = |净位移|/|路径长度| ∈ [0,1]，**含盘中根**：→1 趋势 (走直线)、→0 震荡
    * (来回折返)。预热不足 -> None。衡量"有方向的波动" (大波动/趋势到来)，而非原始波动率。 */
  def efficiencyRatio: Option[Double] = live.map(_._2)

  /** 已收盘的 KAMA（确认值，不含盘中根） */
  def kamaAtClose: Option[Double] = core.value

  /** 已收盘的效率比（确认值，不含盘中根） */
  def efficiencyRatioAtClose: Option[Double] = core.efficiencyRatio
