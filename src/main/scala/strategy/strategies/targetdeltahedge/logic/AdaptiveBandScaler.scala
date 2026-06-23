package strategy.strategies.targetdeltahedge.logic

import hft.strategy.{OutcomeEvent, Strategy}

import hft.domain.{Side, Timestamp}

/** 方向自适应对冲带乘子 ∈ [[[floor]], 1]，买/卖各自独立。
  *
  * 目标：**行情中及时跟上，静默时少来回**——
  *   - 每次对冲成交 -> 收窄该方向乘子 (×(1−[[shrinkPerHedge]]))，下限 [[floor]] (默认 0.5=初始带一半)：
  *     频繁对冲 (有行情) 时带越收越紧，delta 漂移更早触发对冲，跟得上趋势；
  *   - 距上次对冲每过一分钟 -> 该方向乘子 ×(1+[[recoverPerMin]])，上限 1 (回到初始带)：
  *     静默时带逐步放宽，减少无效来回对冲。
  *
  * 实际对冲带 = 基础带 × [[mult]]。乘子按"距上次更新的虚拟分钟数"惰性回升 (纯函数, 无需逐 tick 推进)。
  *
  * @param floor          乘子下限 (0.5 = 最紧收到初始带的一半)
  * @param shrinkPerHedge 每次对冲的收窄比例 (0.10 = 每次 ×0.9)
  * @param recoverPerMin  每分钟的回升比例 (0.10 = 每分钟 ×1.1)
  */
final class AdaptiveBandScaler(
    floor: Double = 0.5,
    shrinkPerHedge: Double = 0.10,
    recoverPerMin: Double = 0.10,
):
  private var mLong: Double = 1.0
  private var mShort: Double = 1.0
  private var tsLong: Timestamp = Long.MinValue
  private var tsShort: Timestamp = Long.MinValue

  /** 当前乘子：以上次更新值为基，按经过的虚拟分钟数回升 (上限 1)。首次 (无对冲史) 返回 1。 */
  def mult(side: Side, now: Timestamp): Double =
    val (m0, ts0) = anchor(side)
    if ts0 == Long.MinValue then 1.0
    else
      val minutes = math.max(0.0, (now - ts0).toDouble / 60000.0)
      math.min(1.0, m0 * math.pow(1.0 + recoverPerMin, minutes))

  /** 对冲成交后收窄该方向乘子 (基于当前已回升值再 ×(1−shrink)，下限 floor)。 */
  def onHedge(side: Side, now: Timestamp): Unit =
    val m = math.max(floor, mult(side, now) * (1.0 - shrinkPerHedge))
    side match
      case Side.Long  => mLong = m; tsLong = now
      case Side.Short => mShort = m; tsShort = now

  private def anchor(side: Side): (Double, Timestamp) = side match
    case Side.Long  => (mLong, tsLong)
    case Side.Short => (mShort, tsShort)
