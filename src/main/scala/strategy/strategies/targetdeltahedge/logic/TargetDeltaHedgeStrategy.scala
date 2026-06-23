package strategy.strategies.targetdeltahedge.logic
import strategy.utils.hedge.{HedgeExecution, HedgeOverlay, HedgeRequest, MacdBiasOverlay}

import hft.strategy.{OutcomeEvent, Strategy}

import hft.domain.*
import hft.exchange.SubscriptionKind
import hft.messaging.{EventData, IncomeEvent, StateManager, SymbolState}

/** 方向性 **目标 delta** 对冲策略 (卖方 short-gamma 友好)：把账户净 delta 维持到一个由注入的
  * [[HedgeOverlay]] 决定的**目标 delta**（而非中性 0），偏离超 gamma 动态带即把净 delta 拉回目标。
  * **如何下单**由注入的 [[HedgeExecution]] 决定 (市价 / 限价追价)，本类只负责"何时对冲、拉到多少"。
  *
  * 与 [[GammaScalpStrategy]] 互补、各自独立实现 (开放封闭，不在一个类里用 flag 分发对冲)。
  *
  * **方向 overlay**：目标 delta 与带宽乘子均来自 [[HedgeOverlay]] (MACD 方向 / KAMA-趋势 等可插拔实现，
  * 策略不关心来源)——target=0 即纯中性对冲。
  *
  * **gamma 动态阈值**：对冲容忍带随 gamma (∝ 头寸规模与临期程度) 自适应，无魔法绝对值——
  *   band = max([[minBand]], [[bandMoveRatio]] · |gamma| · S · overlay.bandMult)
  *
  * 净 delta = 期权 delta (greeks，含现货 cashBal 修正) + 永续持仓。onEvent 由框架单线程串行调用。
  */
final class TargetDeltaHedgeStrategy(
    /** 永续对冲场所 + greeks 标记的交易所 (回测中二者一致) */
    exchange: Exchange,
    /** 永续 symbol, e.g. "ETHUSDT" */
    symbol: Symbol,
    /** greeks 币种, e.g. "ETH" */
    ccy: String,
    /** 对冲执行器：决定如何把净 delta 拉到目标 (市价 / 限价追价) */
    execution: HedgeExecution,
    /** 方向 overlay：决定目标净 delta 与带宽乘子 (MACD / KAMA-趋势 等)。默认 MACD 方向偏移。 */
    overlay: HedgeOverlay = MacdBiasOverlay(),
    /** 对冲容忍带的参考行情比例: band = |gamma|·S·bandMoveRatio·overlay.bandMult。0.004 = 价格约走 0.4% 才对冲 */
    bandMoveRatio: Double = 0.004,
    /** 对冲带下限 (币本位): gamma 极小时避免零带导致频繁碎单对冲 */
    minBand: Double = 0.01,
    /** 价位棘轮：开启后买入须**高于**上次买入价、卖出须**低于**上次卖出价才执行，否则跳过。
      * 买/卖各自独立锚、只外扩不重置 (同向对冲阈值逐渐加大)——区间内买过顶/卖过底后同向加仓被挡，
      * 仅价格创新高/新低 (区间扩张) 才再对冲，抑制震荡中的反复对冲。
      * 代价：反转初期对冲滞后 (须先突破上次反向极值)，裸敞口风险换更低 whipsaw。 */
    ratchet: Boolean = false,
    /** 方向自适应对冲带：Some 时对冲带 = 基础带 × 该方向乘子 (对冲收紧、静默回升)，见 [[AdaptiveBandScaler]]。
      * None = 固定带。 */
    bandScaler: Option[AdaptiveBandScaler] = None,
    /** 棘轮滞回开关 ([[RatchetSwitch]])：Some 且仅当 [[ratchet]]=true 时生效——大反弹临时关棘轮、再下杀
      * 重启并重置锚 (防高空坠落)。None = 棘轮常开。 */
    ratchetSwitch: Option[RatchetSwitch] = None,
    /** 净 delta 的 1min KAMA 滤波器 ([[DeltaKamaFilter]])：Some 时**仅用平滑后的净 delta 作触发门**
      * (决定是否对冲——震荡少来回、趋势照常跟随)，而对冲**方向与数量仍按真实净 delta**算 (真正中和到目标，
      * 否则平滑值滞后会令同向单反复触发、头寸发散)。预热不足回退原始值。None = 直接用瞬时净 delta。 */
    deltaFilter: Option[DeltaKamaFilter] = None,
) extends Strategy:

  /** 价位棘轮锚 (按真实成交 [[EventData.FillUpdate]] 记录)：下次买入须 > buyAnchor、卖出须 < sellAnchor。
    * 初值 ∓∞ -> 首次买/卖恒放行；之后各自单调外扩 (买上行、卖下行)。 */
  private var buyAnchor: Price = Double.NegativeInfinity
  private var sellAnchor: Price = Double.PositiveInfinity

  override def publicStreams: Map[Exchange, Set[SubscriptionKind]] =
    Map(exchange -> Set(SubscriptionKind.Trade(symbol)))

  override def orderTimeoutMs: Long = 5000

  override def onEvent(event: IncomeEvent, state: StateManager): Vector[OutcomeEvent] =
    event.data match
      // 逐笔成交：喂 overlay (方向信号) + 以最新成交价 + 该笔虚拟时间评估对冲
      case EventData.MarketTradeUpdate(t) if t.exchange == exchange && t.symbol == symbol =>
        overlay.update(t.timestamp, t.price, t.qty)
        // 棘轮开关推进：重新启用时重置锚 (清除 stale 锚, 防高空坠落后的反向锁死)
        ratchetSwitch.foreach(sw => if sw.update(t.price) then resetAnchors())
        hedge(t.price, t.timestamp, state)
      // greeks 变化 (delta 漂移) 也即时触发，用 state 中最新成交价为基准 + greeks 自带虚拟时间
      case EventData.GreeksUpdate(g) if g.exchange == exchange && g.ccy == ccy =>
        state
          .symbolState(symbol)
          .flatMap(_.lastTradePrice(exchange))
          .map(hedge(_, g.timestamp, state))
          .getOrElse(Vector.empty)
      // 对冲成交：外扩棘轮锚 (买上行 / 卖下行) + 收窄自适应带乘子
      case EventData.FillUpdate(f) if f.exchange == exchange && f.symbol == symbol =>
        f.side match
          case Side.Long  => buyAnchor = f.price
          case Side.Short => sellAnchor = f.price
        bandScaler.foreach(_.onHedge(f.side, f.timestamp))
        Vector.empty
      case _ => Vector.empty

  private def hedge(refPrice: Price, now: Timestamp, state: StateManager): Vector[OutcomeEvent] =
    (for
      symbolState <- state.symbolState(symbol)
      greeks <- state.greeks(exchange, ccy) // greeks 与 cashBal 均到达才动作 (delta 修正就绪)
    yield
      val netDelta = greeks.delta + symbolState.positionSize(exchange)
      // 1min KAMA 滤波 (若启用)：喂入瞬时净 delta，**仅用平滑值做触发门** (是否对冲)；预热不足回退原始。
      deltaFilter.foreach(_.update(now, netDelta))
      val triggerDelta = deltaFilter.flatMap(_.value).getOrElse(netDelta)
      val gammaScale = math.abs(greeks.gamma) * refPrice // |gamma|·S: 单位价幅对应的 delta 漂移 (币本位)
      val target = overlay.targetDelta(gammaScale) // 目标净 delta (MACD / KAMA-趋势 等)，0=中性
      val triggerDev = triggerDelta - target // 平滑残差：决定"是否"对冲 (噪声门，过滤震荡里的反复越带)
      val realDev = netDelta - target        // 真实残差：决定对冲"方向与数量" (真正中和净 delta, 防再触发发散)
      val side = if realDev > 0 then Side.Short else Side.Long // 偏多->卖, 偏空->买
      val mult = bandScaler.map(_.mult(side, now)).getOrElse(1.0) // 方向自适应带乘子
      val band = math.max(minBand, bandMoveRatio * gammaScale * mult * overlay.bandMult)
      // 棘轮仅在开关启用时生效 (无开关=常开)；关闭期间退化为常规逐带对冲, 反转可及时止损
      val gateOn = ratchet && ratchetSwitch.forall(_.enabled)
      // 触发需**真实残差与平滑残差同时越带**：平滑值是确认门 (震荡里 KAMA 留在带内 -> 抑制 whipsaw)，
      // 真实残差则保证只在确有敞口时动手、且中和后即回带内不micro-churn (无 filter 时 trigger==real -> 同 baseline)
      val req =
        if math.abs(realDev) <= band || math.abs(triggerDev) <= band then None
        else if gateOn && !ratchetAllows(side, refPrice) then None // 同向未越过上次对冲价 -> 跳过
        else Some(HedgeRequest(side, math.abs(realDev), refPrice)) // 触发后按真实残差中和到目标
      execution(req, symbolState.pendingOrders.headOption, now)
    ).getOrElse(Vector.empty)

  /** 价位棘轮门：买入须高于上次买入价 (buyAnchor)、卖出须低于上次卖出价 (sellAnchor)。
    * 买/卖独立判定，首次 (∓∞) 恒放行。 */
  private def ratchetAllows(side: Side, refPrice: Price): Boolean = side match
    case Side.Long  => refPrice > buyAnchor
    case Side.Short => refPrice < sellAnchor

  /** 重置棘轮锚 (开关重新启用时调用)：清除 stale 锚, 下一买/卖恒放行。 */
  private def resetAnchors(): Unit =
    buyAnchor = Double.NegativeInfinity
    sellAnchor = Double.PositiveInfinity
