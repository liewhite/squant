package strategy.utils.hedge

import hft.domain.Coin

/** 一次**敞口轴**对冲判定的上下文 (纯数据)。
  *
  * @param signal  用来判越界的敞口读数 (币本位, 带符号：正=净多、负=净空)。
  *                注意它可能是**平滑后**的值，而对冲下单的数量另取真实敞口 —— 判据与数量
  *                本就是两件事 (见 [[strategy.strategies.ivsellhedge.logic.DeltaKamaHedgeStrategy]])
  * @param macdDir MACD 柱方向 ∈ {-1,0,1}：+1 看多、-1 看空、0 预热不足或持平
  */
final case class DeltaCtx(signal: Coin, macdDir: Int)

/** **敞口轴的对冲死区** —— 把"何时对冲"表达为净敞口的双向阈值 (均 > 0)：
  *   - `signal >  多头敞口侧阈值` -> 净多超限, 需卖出
  *   - `signal < −空头敞口侧阈值` -> 净空超限, 需买入
  *   - 之间是死区, 不动 —— 避免 delta 随价格漂移引起的持续抹布式调仓
  *
  * ## 为什么不复用 [[HedgeBand]]
  *
  * [[HedgeBand]] 的契约是"离对冲中心多远的**价距**" (`HedgeCtx` 给的是 px/center/atr)，
  * 那是 gamma scalping 的节奏：以价格位移为触发量。这里的触发量是**敞口本身**。
  * 把 delta 阈值塞进 [[HedgeBand]] 会让 `HedgeCtx` 一半字段对一半实现永远没有意义，
  * 而调用方从接口上读不出"这个实现只看哪几个字段" —— 那是虚假抽象。两个触发族并列，
  * 各自的上下文只装自己需要的事实。
  *
  * 新的阈值思路 = 新增实现，不改既有 (开放封闭)。
  */
trait DeltaBand:
  /** 返回 (多头敞口侧阈值, 空头敞口侧阈值)，均 > 0 */
  def bands(ctx: DeltaCtx): (Coin, Coin)

object DeltaBand:
  /** **对称死区** (上下同宽, 无方向性) —— 纯 delta 阈值对冲基准, 用于校验口径 */
  def symmetric(base: Coin): DeltaBand = MacdTightened(base, tightenRatio = 1.0)

  /** **MACD 顺势侧收紧**：MACD 看多 -> **空头敞口**侧阈值 ×[[tightenRatio]]；看空 -> 多头敞口侧同理。
    *
    * 语义：看多时账户留着空头敞口是逆势的，那一侧阈值收紧 = 更快把它对冲掉、跟上趋势；
    * 顺势的那一侧 (看多时的多头敞口) 阈值不动 = 小幅顺势敞口不值得对冲。
    *
    * 副作用是**有意的**：两侧阈值不等，无动作带 `signal ∈ [−空头侧, +多头侧]` 不再以 0 为中心，
    * 稳态净敞口偏移 = (多头侧 − 空头侧)/2，**偏向顺势方向**。这不是对冲误差 —— 每次对冲都是
    * 成本 (空头 gamma 组合的离散对冲必然"涨了买、跌了卖")，逆势侧收紧、顺势侧放松就是
    * 「小回调不值得对冲、真反转才动」。代价是震荡市里会朝顺势方向棘轮式累积，最后由一次反向
    * 对冲一口气平掉。
    *
    * @param base         基准阈值 (币本位, > 0)
    * @param tightenRatio 逆势侧的收紧系数 ∈ (0,1]，默认 0.5 (减半)。1.0 = 不收紧, 退化为对称死区
    */
  def macdTightened(base: Coin, tightenRatio: Double = 0.5): DeltaBand = MacdTightened(base, tightenRatio)

  private final case class MacdTightened(base: Coin, tightenRatio: Double) extends DeltaBand:
    require(base > Coin.Zero, s"死区基准阈值须 > 0, 实为 ${base.value}")
    require(tightenRatio > 0.0 && tightenRatio <= 1.0, s"收紧系数须 ∈ (0,1], 实为 $tightenRatio")

    def bands(ctx: DeltaCtx): (Coin, Coin) =
      val tight = base.scaled(tightenRatio)
      ctx.macdDir match
        case d if d > 0 => (base, tight)  // 看多: 空头敞口 (逆势) 侧收紧
        case d if d < 0 => (tight, base)  // 看空: 多头敞口 (逆势) 侧收紧
        case _          => (base, base)   // 预热不足/持平: 对称, 不猜方向
