package strategy.utils.hedge

import hft.domain.Coin

/** 一次**敞口轴**对冲判定的上下文 (纯数据)。
  *
  * @param exposure        当前**真实**净敞口 (币本位, 带符号：正=净多、负=净空)。
  *                        判据就是它 —— 各种信号只调整阈值, 不替换被测量的量
  * @param macdDir         MACD 柱方向 ∈ {-1,0,1}：+1 看多、-1 看空、0 预热不足或持平
  * @param efficiencyRatio 效率比 ER = |净位移|/|路径长度| ∈ [0,1]：→1 趋势、→0 震荡；None = 预热不足
  */
final case class DeltaCtx(exposure: Coin, macdDir: Int, efficiencyRatio: Option[Double])

/** **敞口轴的对冲死区** —— 把"何时对冲"表达为真实净敞口的双向阈值 (均 > 0)：
  *   - `exposure >  多头敞口侧阈值` -> 净多超限, 需卖出
  *   - `exposure < −空头敞口侧阈值` -> 净空超限, 需买入
  *   - 之间是死区, 不动 —— 避免 delta 随价格漂移引起的持续抹布式调仓
  *
  * ## 判据是真实敞口，信号只调阈值
  *
  * 这条分工是有原因的。曾经的实现让平滑后的敞口**替换**真实敞口去过阈值，结果是"超过阈值
  * 就对冲"这条根本规则被架空：在边抖边涨这类行情里，平滑值会把真实敞口的累积压住一两个
  * 小时，敞口没有上界。要把上界找回来就得再加一道"绝对值硬闸门"——那是给自己制造的问题
  * 打补丁，而且从此有两个判据。
  *
  * 现在所有信号都只是阈值的乘数，于是**最宽的阈值就是敞口的上界**，无需第二道闸门。
  *
  * ## 为什么不复用 [[HedgeBand]]
  *
  * [[HedgeBand]] 的契约是"离对冲中心多远的**价距**" (`HedgeCtx` 给的是 px/center/atr)，
  * 那是 gamma scalping 的节奏：以价格位移为触发量。这里的触发量是**敞口本身**。
  * 把 delta 阈值塞进 [[HedgeBand]] 会让 `HedgeCtx` 一半字段对一半实现永远没有意义，
  * 而调用方从接口上读不出"这个实现只看哪几个字段" —— 那是虚假抽象。
  *
  * 新的阈值思路 = 新增实现，不改既有 (开放封闭)。
  */
trait DeltaBand:
  /** 返回 (多头敞口侧阈值, 空头敞口侧阈值)，均 > 0 */
  def bands(ctx: DeltaCtx): (Coin, Coin)

object DeltaBand:
  /** **固定死区** (上下同宽, 不受任何信号影响) —— 纯 delta 阈值对冲基准, 用于校验口径 */
  def fixed(base: Coin): DeltaBand = Adaptive(base, 1.0, 1.0, 1.0)

  /** **自适应死区**：两个信号各自缩放阈值，判据始终是真实敞口。
    *
    * **体制缩放 (ER)** —— 在 [[chopWiden]] 与 [[trendTighten]] 之间按 ER 线性插值：
    *   - ER→0 (来回折返)：阈值 ×`chopWiden` (>1) -> **放宽** -> 少对冲。每次对冲都是成本
    *     (空头 gamma 组合的离散对冲必然"涨了买、跌了卖")，震荡里的穿越不值得追。
    *   - ER→1 (走出方向)：阈值 ×`trendTighten` (<1) -> **收紧** -> 尽快跟上。
    *   - ER 预热不足：系数 1.0 (用基准阈值, 不猜体制)。这里可以安全地"不猜"，因为判据是
    *     真实敞口 —— 猜错只影响对冲的疏密，不会让敞口失去上界。
    *
    * **MACD 侧向收紧** —— 看多时**空头敞口**侧阈值 ×[[macdTighten]]；看空时多头敞口侧同理。
    * 语义：看多时账户留着空头敞口是逆势的，那一侧收紧 = 更快把它对冲掉；顺势那一侧不动 =
    * 小幅顺势敞口不值得对冲。
    *
    * 两个信号**相乘**叠加。副作用是有意的：两侧阈值不等，无动作带不再以 0 为中心，稳态净敞口
    * 偏向顺势方向。这是刻意的趋势跟随，代价是震荡市里会朝顺势方向棘轮式累积、最后由一次反向
    * 对冲一口气平掉。
    *
    * **敞口上界 = `base × chopWiden`** —— 任何信号组合都不会让阈值超过它。
    *
    * @param base         基准阈值 (币本位, > 0)
    * @param macdTighten  MACD 逆势侧的收紧系数 ∈ (0,1]，默认 0.5 (减半)；1.0 = 不收紧
    * @param chopWiden    ER→0 时的放宽系数 (>= 1)，默认 2.0
    * @param trendTighten ER→1 时的收紧系数 ∈ (0, 1]，默认 0.5
    */
  def adaptive(
      base: Coin,
      macdTighten: Double = 0.5,
      chopWiden: Double = 2.0,
      trendTighten: Double = 0.5,
  ): DeltaBand = Adaptive(base, macdTighten, chopWiden, trendTighten)

  private final case class Adaptive(base: Coin, macdTighten: Double, chopWiden: Double, trendTighten: Double)
      extends DeltaBand:
    require(base > Coin.Zero, s"死区基准阈值须 > 0, 实为 ${base.value}")
    require(macdTighten > 0.0 && macdTighten <= 1.0, s"MACD 收紧系数须 ∈ (0,1], 实为 $macdTighten")
    require(chopWiden >= 1.0, s"震荡放宽系数须 >= 1, 实为 $chopWiden")
    require(trendTighten > 0.0 && trendTighten <= 1.0, s"趋势收紧系数须 ∈ (0,1], 实为 $trendTighten")

    def bands(ctx: DeltaCtx): (Coin, Coin) =
      // ER 缺失时取 1.0 而不是 chopWiden/trendTighten 中的任一个: 不猜体制, 用基准阈值
      val regime = ctx.efficiencyRatio.fold(1.0)(er => chopWiden + (trendTighten - chopWiden) * er)
      val b = base.scaled(regime)
      val tight = b.scaled(macdTighten)
      ctx.macdDir match
        case d if d > 0 => (b, tight)  // 看多: 空头敞口 (逆势) 侧收紧
        case d if d < 0 => (tight, b)  // 看空: 多头敞口 (逆势) 侧收紧
        case _          => (b, b)      // 预热不足/持平: 对称, 不猜方向

    /** 阈值的上界 —— 也就是**真实敞口的上界** (超过它必然对冲)。诊断与装配期日志用。 */
    def maxBand: Coin = base.scaled(chopWiden)
