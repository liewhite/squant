package voltrade

import hft.domain.Side
import hft.strategy.edge.{HedgeCtx, MaAsymHedgeBand}

/** 永续 delta 对冲的**纯决策** (与回测 MakerHedgeStrategy 同一套带逻辑, 抽成无 IO 纯函数便于单测)。
  * 账户净 delta = 期权账户 delta + 永续持仓; 越 MaAsym 带则在永续上挂被动单对冲到中性, 5s 未成交撤单重挂。 */
object PerpHedger:
  final case class Params(
      offsetPct: Double = 0.0002, // 被动挂单改善 (现价外 0.02%)
      requoteMs: Long = 5000,
      minHedge: Double = 0.01,    // 净 delta 小于此不动作 (避免碎单)
      tightAtr: Double = 1.0,
      looseAtr: Double = 2.0,
  )

  /** 对冲状态 (跨轮询周期保持)。restingId=当前永续挂单的交易所 id; center=对冲中心。 */
  final case class State(center: Double = Double.NaN, restingId: Option[String] = None, restingAt: Long = 0L)

  enum Action:
    case Hold
    case Place(side: Side, price: Double, qty: Double)
    case Cancel(id: String)

  /** 决策。netDelta=期权delta+永续持仓; maBias=sign(mid−MA20)。返回动作 + 新状态 (中心初始化等)。
    * 注: 成交后的"中心重置/清挂单"由调用方 (轮询层) 在侦测到挂单消失/持仓变化时做。 */
  def decide(mid: Double, atr: Double, maBias: Int, netDelta: Double, nowMs: Long, st: State, p: Params): (Action, State) =
    val st1 = if st.center.isNaN then st.copy(center = mid) else st
    st1.restingId match
      case Some(id) =>
        if nowMs - st1.restingAt > p.requoteMs then (Action.Cancel(id), st1.copy(restingId = None))
        else (Action.Hold, st1)
      case None =>
        if atr <= 0.0 || st1.center.isNaN then (Action.Hold, st1)
        else
          val (up, down) = MaAsymHedgeBand(p.tightAtr, p.looseAtr).bands(HedgeCtx(mid, st1.center, atr, 1.0, 0, maBias))
          val crossed = (mid - st1.center > up) || (st1.center - mid > down)
          if crossed && math.abs(netDelta) >= p.minHedge then
            val side = if netDelta > 0 then Side.Short else Side.Long // 净多→卖, 净空→买
            val price = if side == Side.Short then mid * (1.0 + p.offsetPct) else mid * (1.0 - p.offsetPct)
            (Action.Place(side, price, math.abs(netDelta)), st1)
          else (Action.Hold, st1)
