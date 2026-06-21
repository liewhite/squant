package voltrade

import hft.domain.Side
import hft.strategy.edge.{HedgeCtx, MaAsymHedgeBand}

/** 永续 delta 对冲的**纯决策** (与回测 MakerHedgeStrategy 同一套带逻辑, 抽成无 IO 纯函数便于单测)。
  * 账户净 delta = 期权账户 delta + 永续持仓; 越 MaAsym 带则在永续上挂被动单对冲到中性, 5s 未成交撤单重挂。
  *
  * 关键不变量 (资金安全):
  *   - **中心重置由 delta 中性驱动**: |净Δ|<minHedge 即视为已对冲, center 移到现价 (与是否侦测到成交解耦,
  *     避免把"被拒/被撤"误判成交)。
  *   - **qty 按 qtyStep 向下取整且 ≥minQty, price 按 tickSize 定向取整保被动** (卖向上、买向下), 否则不下单。
  *   - **Cancel 不在决策里清 restingId**: 由调用方在撤单**成功后**清, 撤单失败保留 -> 防双倍敞口。
  */
object PerpHedger:
  final case class Params(
      offsetPct: Double = 0.0002, // 被动挂单改善 (现价外 0.02%)
      requoteMs: Long = 5000,
      minHedge: Double = 0.01,    // 净 delta 小于此视为已中性 (不动作 + 重置中心)
      tightAtr: Double = 1.0,
      looseAtr: Double = 2.0,
      qtyStep: Double = 0.0,      // 永续下单量步长 (来自交易所 instrument)
      minQty: Double = 0.0,       // 永续最小下单量
      tickSize: Double = 0.0,     // 永续价格最小变动
  )

  /** 对冲状态 (跨轮询周期保持)。restingId=当前永续挂单 id; center=对冲中心。 */
  final case class State(center: Double = Double.NaN, restingId: Option[String] = None, restingAt: Long = 0L)

  enum Action:
    case Hold
    case Place(side: Side, price: Double, qty: Double)
    case Cancel(id: String)

  /** 决策。netDelta=期权delta+永续持仓; maBias=sign(mid−MA20)。返回动作 + 新状态。
    * Cancel 返回时**不清 restingId** (由调用方撤单成功后清)。 */
  def decide(mid: Double, atr: Double, maBias: Int, netDelta: Double, nowMs: Long, st: State, p: Params): (Action, State) =
    if st.center.isNaN then (Action.Hold, st.copy(center = mid))
    else if math.abs(netDelta) < p.minHedge then (Action.Hold, st.copy(center = mid)) // 已中性 -> 重置中心
    else
      st.restingId match
        case Some(id) =>
          if nowMs - st.restingAt > p.requoteMs then (Action.Cancel(id), st) // 保留 restingId, 撤成功后由调用方清
          else (Action.Hold, st)
        case None =>
          if atr <= 0.0 then (Action.Hold, st)
          else
            val (up, down) = MaAsymHedgeBand(p.tightAtr, p.looseAtr).bands(HedgeCtx(mid, st.center, atr, 1.0, 0, maBias))
            val crossed = (mid - st.center > up) || (st.center - mid > down)
            if !crossed then (Action.Hold, st)
            else
              val side = if netDelta > 0 then Side.Short else Side.Long // 净多→卖, 净空→买
              val rawPx = if side == Side.Short then mid * (1.0 + p.offsetPct) else mid * (1.0 - p.offsetPct)
              SellVolPlan.quantizeQty(math.abs(netDelta), p.qtyStep, p.minQty) match
                case Some(q) => (Action.Place(side, alignPrice(rawPx, p.tickSize, side), q), st)
                case None    => (Action.Hold, st) // 净Δ 量化后低于最小下单量, 无法对冲该残量
  end decide

  /** 价格按 tickSize 定向取整以保被动: 卖单向上、买单向下 (远离对手价, 确保 PostOnly 不越价)。 */
  def alignPrice(price: Double, tick: Double, side: Side): Double =
    if tick <= 0 then price
    else if side == Side.Short then math.ceil(price / tick) * tick
    else math.floor(price / tick) * tick
