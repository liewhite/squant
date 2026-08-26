package strategy.utils.hedge

import hft.domain.{Coin, Price}
import hft.option.BlackScholes

/** 一次**敞口轴**对冲判定的上下文 (纯数据) —— 只装"观测到的事实"，怎么定阈值归 [[DeltaBand]]。
  *
  * @param exposure    当前**真实**净敞口 (币本位, 带符号：正=净多、负=净空)。判据就是它
  * @param driftDir    方向预测 ∈ {-1,0,1}：+1 预测标的上涨、-1 下跌、0 不知道 (来自 MACD 柱符号)。
  *                    **只出符号不出强度** —— 方向判断本来就比强度判断可靠, 把强度也用上还要再拍
  *                    一组映射, 没有依据
  * @param gamma       期权组合 gamma (Coin per 价格单位)：敞口对价格的二阶敏感度
  * @param spot        标的价
  * @param sigmaAnnual 年化波动率 (实现波动或期权 IV)；None = 尚未就绪
  */
final case class DeltaCtx(
    exposure: Coin,
    driftDir: Int,
    gamma: Coin,
    spot: Price,
    sigmaAnnual: Option[Double],
)

/** **敞口轴的对冲死区** —— 把"何时对冲"表达为真实净敞口的双向阈值 (均 > 0)：
  *   - `exposure >  多头敞口侧阈值` -> 净多超限, 需卖出
  *   - `exposure < −空头敞口侧阈值` -> 净空超限, 需买入
  *   - 之间是死区, 不动
  *
  * ## 判据是真实敞口，信号只决定阈值
  *
  * 曾经的实现让平滑后的敞口**替换**真实敞口去过阈值，结果 "超过阈值就对冲" 这条根本规则被架空：
  * 边抖边涨的行情里，平滑值会把真实敞口的累积压住一两个小时，敞口没有上界。要把上界找回来就得
  * 再加一道绝对值硬闸门 —— 那是给自己制造的问题打补丁。现在所有信号都只决定阈值，
  * 而阈值有硬上限，所以**真实敞口有确定上界**，只有一个判据。
  *
  * ## 为什么不复用 [[HedgeBand]]
  *
  * [[HedgeBand]] 的契约是"离对冲中心多远的**价距**"，那是 gamma scalping 的节奏：以价格位移为
  * 触发量。这里的触发量是**敞口本身**。把 delta 阈值塞进去会让 `HedgeCtx` 一半字段对一半实现
  * 永远没有意义，而调用方从接口上读不出这件事 —— 那是虚假抽象。
  */
trait DeltaBand:
  /** 返回 (多头敞口侧阈值, 空头敞口侧阈值)，均 > 0 */
  def bands(ctx: DeltaCtx): (Coin, Coin)

object DeltaBand:
  /** **固定死区** (上下同宽, 不受任何观测影响) —— 纯 delta 阈值对冲基准, 用于校验口径 */
  def fixed(theta: Coin): DeltaBand = Fixed(theta)

  /** **按预测波动范围定阈值**，方向预测决定两侧的不对称。
    *
    * ## 尺度：一次对冲要能"撑住"多久
    *
    * 敞口的扩散率 = `|gamma| × 标的价 × σ`，即敞口每单位时间漂移的标准差。于是
    *
    * {{{
    *   预测波动范围 range = |gamma| × spot × σ_annual × √(horizon / 一年)
    * }}}
    *
    * 就是"敞口在 [[horizonMs]] 这段时间里的典型漂移幅度"。两侧阈值都以它为单位。
    *
    * 这样做的收益是**对冲频率可控**：首次碰壁时间 ∝ 阈值² / 扩散率²，阈值不随波动走的话，
    * 波动翻倍会让对冲频率变四倍。以 range 为单位后，波动翻倍阈值也翻倍，频率基本不变。
    *
    * ## 不对称：两侧的意义完全不同
    *
    * 对冲后敞口从 0 出发：
    *
    *   - **漂移推向的那一侧**：碰到它意味着敞口在真实地累积 —— 对冲它是对的，而且会**撑住**
    *     (漂移继续同向)。所以用 [[tightMult]] (< 1)，早点动、尽快跟上。
    *   - **反侧**：碰到它只可能是噪声把敞口拉了回来 —— 在那里对冲，下一步很可能被反向平掉，
    *     也就是"被正常波动来回止损"。所以用 [[looseMult]] (> 1)，**放到噪声之外**。
    *     `looseMult≈2` 时路径触及它的概率大约 5%，所以这个参数的含义是"愿意接受多少比例的假对冲"。
    *
    * 空头跨式的方向对应关系：价格涨 -> 期权 delta 更负 -> 账户偏空 -> 需要**买入**对冲。
    * 所以 `driftDir=+1` (预测上涨) 时，漂移推向的是**空头敞口侧**，那一侧收紧。
    *
    * 方向未知 (`driftDir=0`, MACD 预热不足或柱持平) 时两侧都取 1.0 倍 range，退化为对称带。
    *
    * ## 两个 clamp 各管一头
    *
    *   - [[maxTheta]]：**纯风险上限** (delta 单位、绝对的)。它只在高波动时咬住 —— 那时
    *     "别被噪声碰到"要求很宽的带，而"敞口不能太大"不允许，正是该咬的时候。
    *   - [[minTheta]]：防波动塌缩时阈值趋零、退化成逐笔抖动。σ 尚未就绪时也取它
    *     (宁可对冲得频繁些，也不要按一个不存在的波动率放宽敞口)。
    *
    * ## 一个前提
    *
    * 不对称**只有在方向预测有 edge 时才赚钱**。若方向判断接近 50% 命中，`tight`/`loose` 的
    * 不对称就是在随机地收紧一侧，净效果退回对称带，还多背一个隐式趋势偏移
    * (稳态敞口偏向被放宽的那侧，也就是预测的顺势方向；预测错了这个偏移就是纯亏)。
    * 所以调这两个参数之前，应先回测方向预测在"敞口即将越界"这些时点上的条件命中率。
    *
    * @param tightMult  漂移推向那一侧的倍数 ∈ (0, 1]，默认 0.5
    * @param looseMult  反侧的倍数 >= 1，默认 2.0
    * @param horizonMs  期望一次对冲能撑多久 (ms)，默认 30 分钟
    * @param minTheta   阈值下限
    * @param maxTheta   阈值上限 = **敞口的硬上界**
    */
  def volScaled(
      tightMult: Double = 0.5,
      looseMult: Double = 2.0,
      horizonMs: Long = 30 * 60_000L,
      minTheta: Coin = Coin(0.02),
      maxTheta: Coin = Coin(1.0),
  ): DeltaBand = VolScaled(tightMult, looseMult, horizonMs, minTheta, maxTheta)

  private final case class Fixed(theta: Coin) extends DeltaBand:
    require(theta > Coin.Zero, s"死区阈值须 > 0, 实为 ${theta.value}")
    def bands(ctx: DeltaCtx): (Coin, Coin) = (theta, theta)

  private final case class VolScaled(
      tightMult: Double,
      looseMult: Double,
      horizonMs: Long,
      minTheta: Coin,
      maxTheta: Coin,
  ) extends DeltaBand:
    require(tightMult > 0.0 && tightMult <= 1.0, s"收紧倍数须 ∈ (0,1], 实为 $tightMult")
    require(looseMult >= 1.0, s"放宽倍数须 >= 1, 实为 $looseMult")
    require(horizonMs > 0, s"对冲时间尺度须 > 0ms, 实为 $horizonMs")
    require(minTheta > Coin.Zero, s"阈值下限须 > 0, 实为 ${minTheta.value}")
    require(maxTheta >= minTheta, s"阈值上限 ${maxTheta.value} 须 >= 下限 ${minTheta.value}")

    /** √(时间尺度 / 一年) —— 年化波动率换算到本时间尺度 */
    private val horizonYears = math.sqrt(horizonMs.toDouble / BlackScholes.MillisPerYear)

    def bands(ctx: DeltaCtx): (Coin, Coin) =
      val range = ctx.sigmaAnnual.fold(0.0) { sigma =>
        math.abs(ctx.gamma.value) * ctx.spot.value * sigma * horizonYears
      }
      val tight = clamp(range * tightMult)
      val loose = clamp(range * looseMult)
      ctx.driftDir match
        // 预测上涨 -> 敞口被推向空头侧 -> 空头侧收紧
        case d if d > 0 => (loose, tight)
        case d if d < 0 => (tight, loose)
        case _          => (clamp(range), clamp(range)) // 方向未知: 对称, 不猜
    private def clamp(theta: Double): Coin = Coin(theta).max(minTheta).min(maxTheta)
