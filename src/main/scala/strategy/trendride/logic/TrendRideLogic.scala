package strategy.trendride.logic

import hft.domain.*

/** TrendRide 纯决策逻辑 —— 把"测量量"翻译成"目标仓位"再翻译成"该挂的单"。全是纯函数, 无副作用、可直接断言。
  *
  * 设计 (第一性原理)：
  *   - 两个自归一化测量原语：多周期 drift 带噪比 (趋势, 见 [[hft.indicator.TrendConviction]])、
  *     短周期 stretch z-score (超买超卖/波动)。
  *   - 两本账叠加成**目标仓位**：动量账 core = C·mMax (顺势)、均值回归账 −z·rMax (高抛低吸)。
  *   - **信念安全带**钳住目标符号：C>死区 只允许净多、C<−死区 只允许净空、|C|≤死区 对称轻仓 ——
  *     "绝不持大规模逆势仓"是**构造出来**的, 目标函数永不要求逆势仓。
  *   - 执行激进度由**成因**决定：顺势加仓 / 超买止盈走 maker (耐心吃价差)；逆势 / 翻向走 taker (立即拍平反手)。
  */
object TrendRideLogic:

  /** 短周期拉伸 z-score：价相对短锚偏离几个 (σ·√anchorBars)。正 = 超买、负 = 超卖。 */
  def stretchZ(price: Price, anchor: Price, sigmaPerBar: Double, anchorBars: Int): Double =
    if anchor <= 0.0 || sigmaPerBar <= 0.0 || anchorBars <= 0 then 0.0
    else math.log(price / anchor) / (sigmaPerBar * math.sqrt(anchorBars.toDouble))

  /** 策略系数 (币本位/比例)。 */
  final case class Params(
      mMax: Double,          // 满信念趋势仓上限
      rMax: Double,          // 均值回归账规模 (|C|≈0 时的满额)
      stepQty: Quantity,     // 单笔 maker 分批量上限
      band: Double,          // 不交易带：|pos−target| 不超过它则不动 (抗手续费磨损)
      adverseCap: Double,    // 逆势仓硬上限：|逆势 pos| 超过即 taker 强平
      convDead: Double,      // 信念死区：|C|≤convDead 视为混乱 (对称轻仓)
      passiveOffset: Double, // maker 相对现价的被动偏移 (比例)
      priceTol: Double,      // resting 单价容差 (比例)：偏离期望价超过则撤换
      mrTrendDecay: Double = 0.0, // 均值回归随信念衰减系数 ∈[0,1]：effRMax=rMax·(1−decay·|C|)。
                                  // 0=不衰减 (旧行为，强趋势里 MR 反噬)；1=满信念时 MR 全关 (纯顺势)。
      trendEntryTaker: Boolean = false, // 顺势动量加仓是否主动吃单 (taker)。被动 maker 在趋势里挂在错边、
                                        // 价格越跑越远接不到 → 趋势仓建不起来。taker 主动参与才能骑上趋势 (代价: taker 费)。
  )

  /** 信念 + 拉伸 → 目标净仓 (带符号)。安全带保证目标永不在逆势侧 (硬约束 #7 构造性满足)。
    *
    * 均值回归账规模随信念衰减 (`mrTrendDecay`)：强趋势 (|C|→1) 时压缩甚至关闭抄底/逃顶, 让趋势仓跑满；
    * 混乱 (|C|→0) 时恢复满额, 轻仓高抛低吸。修复"短锚在单边趋势里恒判超买/超卖、持续反噬趋势"的病灶。 */
  def target(c: Double, z: Double, p: Params): Double =
    val effRMax = p.rMax * (1.0 - p.mrTrendDecay * math.min(1.0, math.abs(c)))
    val core = c * p.mMax
    val raw = core - z * effRMax // 均值回归：超买(z>0)降目标、超卖(z<0)升目标
    val (lo, hi) =
      if c > p.convDead then (0.0, p.mMax)         // 看多：只允许净多
      else if c < -p.convDead then (-p.mMax, 0.0)  // 看空：只允许净空
      else (-p.rMax, p.rMax)                       // 混乱：对称轻仓高抛低吸
    math.max(lo, math.min(hi, raw))

  enum Exec:
    case Maker, Taker

  /** 期望挂单 (单笔)。None = 在不交易带内, 不动。 */
  final case class Desired(side: Side, price: Price, qty: Quantity, reduceOnly: Boolean, exec: Exec)

  /** 由 (当前仓, 目标仓, 信念, 现价) 推出本拍该挂的单：
    *   - |gap| ≤ band → None (不动)。
    *   - 逆势 (pos 与 C 反号 且 |pos| > adverseCap) 或 需翻向 (target 与 pos 反号) → **taker** 一次吃掉 gap (可穿越反手)。
    *   - 否则顺势 **maker 分批**：朝目标同向 = 加仓 (reduceOnly=false)；朝零方向 = 减仓/超买止盈 (reduceOnly=true)；
    *     单笔量截到 stepQty，挂被动价 (买在现价下方、卖在现价上方) 等回调/反弹成交。
    */
  def desired(pos: Double, target: Double, c: Double, price: Price, p: Params): Option[Desired] =
    val gap = target - pos
    if math.abs(gap) <= p.band then None
    else
      val side = if gap > 0 then Side.Long else Side.Short
      val adverse = pos * c < 0.0 && math.abs(pos) > p.adverseCap
      val flipping = target * pos < 0.0
      if adverse || flipping then
        // 反手 (target 与 pos 反号) 需穿越零 → reduceOnly=false；纯强平 (仅超 cap) 只减不增 → reduceOnly=true,
        // 由撮合层按实时持仓截断, 杜绝成交竞态下反向开仓。
        Some(Desired(side, price, math.abs(gap), reduceOnly = !flipping, Exec.Taker))
      else
        val reducing = pos != 0.0 && math.signum(gap) != math.signum(pos) // 朝零 = 减仓/止盈
        val qty = math.min(p.stepQty, math.abs(gap))
        // 顺势动量加仓 = 非减仓 且 信念明确 (|C|>死区) 且 加仓方向与信念同向。混乱区 (|C|≤死区) 的开仓属均值回归, 仍走 maker。
        val momentumAdd = !reducing && math.abs(c) > p.convDead && math.signum(gap) == math.signum(c)
        if p.trendEntryTaker && momentumAdd then
          Some(Desired(side, price, qty, reduceOnly = false, Exec.Taker)) // 主动吃单骑趋势
        else
          val px = side match
            case Side.Long  => price * (1 - p.passiveOffset) // 减仓/混乱回归: 被动挂单等回调/反弹
            case Side.Short => price * (1 + p.passiveOffset)
          Some(Desired(side, px, qty, reducing, Exec.Maker))

  /** 现有挂单的精简视图 (对账用纯数据)。`isLimit=false` 即在飞的市价 (taker) 单。 */
  final case class Resting(id: OrderId, side: Side, reduceOnly: Boolean, isLimit: Boolean, price: Price, confirmed: Boolean)

  /** 对账计划：本拍要撤的挂单 id + 至多一张要下的单。 */
  final case class Plan(cancelIds: Vector[OrderId], place: Option[Desired])

  private def matches(r: Resting, d: Desired, priceTol: Double): Boolean =
    r.isLimit && r.side == d.side && r.reduceOnly == d.reduceOnly && math.abs(r.price - d.price) / d.price <= priceTol

  /** 由 (期望单, 现有挂单) 推出本拍对账计划。**纯函数, 与 StateManager 解耦, 可直接断言。**
    *
    *   - want=None (进不交易带)：撤掉所有已确认 limit 挂单。
    *   - Taker (逆势强平/反手)：**同拍**撤掉所有已确认 limit 挂单 **并发**下市价单 (不等撤单确认, 避免强平哑火窗口)；
    *     仅当已有市价单在飞 (isLimit=false) 时抑制重复下单。
    *   - Maker (顺势加仓/止盈)：保留价/向匹配的挂单, 撤掉不匹配的已确认 limit; 无匹配且无 taker 在飞时补挂一张。
    *
    * `alreadyCancelling` 标记已发出撤单待确认的 id (避免重复撤)。 */
  def plan(want: Option[Desired], resting: Vector[Resting], alreadyCancelling: OrderId => Boolean, priceTol: Double): Plan =
    val takerInFlight = resting.exists(r => !r.isLimit)
    def cancelable(keep: Resting => Boolean): Vector[OrderId] =
      resting.filter(r => r.isLimit && r.confirmed && !alreadyCancelling(r.id) && !keep(r)).map(_.id)
    want match
      case None =>
        Plan(cancelable(_ => false), None)
      case Some(d) if d.exec == Exec.Taker =>
        Plan(cancelable(_ => false), if takerInFlight then None else Some(d))
      case Some(d) =>
        val matching = resting.exists(r => matches(r, d, priceTol))
        Plan(cancelable(r => matches(r, d, priceTol)), if matching || takerInFlight then None else Some(d))
