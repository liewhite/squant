package strategy.research

import hft.strategy.{OutcomeEvent, Strategy}

import hft.domain.*
import hft.messaging.{EventData, IncomeEvent, StateManager}

/** 方向性目标 delta 对冲策略单测：gamma 动态带 / minBand 下限 / MACD 方向目标符号 /
  * 限价挂价与 PostOnly / 3 秒 repeg 撤单 / greeks 未就绪不动作。
  */
class TargetDeltaHedgeStrategySpec extends munit.FunSuite:
  private val ex = Exchange.Binance
  private val sym = "ETHUSDT"
  private val ccy = "ETH"
  private val hourMs = 3_600_000L

  private def strat(
      bandMoveRatio: Double = 0.004,
      tiltMoveRatio: Double = 0.005,
      minBand: Double = 0.01,
      repegMs: Long = 3000,
  ) = TargetDeltaHedgeStrategy(
    ex, sym, ccy,
    execution = LimitRepegHedgeExecution(ex, sym, repegMs = repegMs),
    overlay = MacdBiasOverlay(tiltMoveRatio = tiltMoveRatio),
    bandMoveRatio = bandMoveRatio,
    minBand = minBand,
  )

  /** 市价执行版策略 (其余同 strat) */
  private def stratMarket(
      bandMoveRatio: Double = 0.004,
      tiltMoveRatio: Double = 0.005,
      minBand: Double = 0.01,
      ratchet: Boolean = false,
  ) =
    TargetDeltaHedgeStrategy(
      ex, sym, ccy,
      execution = MarketHedgeExecution(ex, sym),
      overlay = MacdBiasOverlay(tiltMoveRatio = tiltMoveRatio),
      bandMoveRatio = bandMoveRatio,
      minBand = minBand,
      ratchet = ratchet,
    )

  /** 构造已就绪状态：cashBal + greeks(含 gamma) + 一笔触发用 trade (设最新成交价)。返回 (state, 触发事件)。 */
  private def setup(
      rawDelta: Double,
      gamma: Double,
      tradePrice: Price,
      ts: Timestamp = 1,
      perpPos: Double = 0.0,
  ): (StateManager, IncomeEvent) =
    val sm = StateManager(Iterable(sym), orderTimeoutMs = 5000)
    sm.apply(IncomeEvent(0, 0, EventData.BalanceUpdate(Balance(ex, ccy, 0.0, 0))))
    sm.apply(IncomeEvent(0, 0, EventData.GreeksUpdate(Greeks(ex, ccy, rawDelta, gamma, -0.5, 1.0, 0))))
    if perpPos != 0.0 then sm.apply(IncomeEvent(0, 0, EventData.PositionUpdate(Position(ex, sym, perpPos, 100.0, 0.0))))
    val ev = tradeEv(tradePrice, ts)
    sm.apply(ev)
    (sm, ev)

  private def tradeEv(price: Price, ts: Timestamp): IncomeEvent =
    IncomeEvent(ts, ts, EventData.MarketTradeUpdate(MarketTrade(ex, sym, price, 1.0, isBuyerMaker = false, ts)))

  private def placedOrder(out: Vector[OutcomeEvent]): Order = out match
    case Vector(OutcomeEvent.PlaceOrders(orders, _)) => orders.head
    case other                                       => fail(s"expected single PlaceOrders, got $other")

  private def near(a: Double, b: Double): Unit = assert(math.abs(a - b) < 1e-9, s"expected $b got $a")

  // ---- 暖机不足 (bias=0) -> 目标 delta = 0，退化为纯 gamma 动态带中性对冲 ----

  test("bias=0 偏多越带 -> 最新价挂限价卖单 (PostOnly), 数量=净delta, 目标=0"):
    // gammaScale = 0.05*100 = 5; band = max(0.01, 0.004*5)=0.02; netDelta=0.5 > 0.02
    val (sm, ev) = setup(rawDelta = 0.5, gamma = 0.05, tradePrice = 100.0)
    val o = placedOrder(strat().onEvent(ev, sm))
    assertEquals(o.side, Side.Short)
    near(o.quantity, 0.5)
    o.orderType match
      case OrderType.Limit(px, tif) =>
        assertEquals(tif, TimeInForce.PostOnly)
        near(px, 100.0) // 挂最新成交价
      case x => fail(s"expected Limit, got $x")

  test("bias=0 偏空越带 -> 最新价挂限价买单"):
    val (sm, ev) = setup(rawDelta = -0.5, gamma = 0.05, tradePrice = 100.0)
    val o = placedOrder(strat().onEvent(ev, sm))
    assertEquals(o.side, Side.Long)
    near(o.quantity, 0.5)

  test("gamma 动态带: 净delta 在 |gamma|·S·bandMoveRatio 带内 -> 不挂单"):
    // gammaScale=0.05*100=5; band=0.004*5=0.02; netDelta=0.015 < band
    val (sm, ev) = setup(rawDelta = 0.015, gamma = 0.05, tradePrice = 100.0)
    assertEquals(strat().onEvent(ev, sm), Vector.empty)

  test("gamma 动态带: 同样净delta 在更大 gamma 下越带 -> 挂单"):
    // gammaScale=0.2*100=20; band=0.004*20=0.08 -> 0.015 仍在带内? 不, 取更小净delta对照
    // 用 netDelta=0.1: 小 gamma band=0.02 -> 越带; 这里验证大 gamma 把带撑大到 0.08 -> 0.05 入带
    val (smIn, evIn) = setup(rawDelta = 0.05, gamma = 0.2, tradePrice = 100.0) // band=0.08, 0.05<0.08 入带
    assertEquals(strat().onEvent(evIn, smIn), Vector.empty)
    val (smOut, evOut) = setup(rawDelta = 0.05, gamma = 0.05, tradePrice = 100.0) // band=0.02, 0.05>0.02 越带
    assertEquals(placedOrder(strat().onEvent(evOut, smOut)).side, Side.Short)

  test("minBand 下限: gamma 极小 -> 带取 minBand, 微小净delta 不触发"):
    // gammaScale=0.0001*100=0.01; bandMoveRatio*scale=0.00004 -> 取 minBand=0.01; netDelta=0.005<0.01
    val (sm, ev) = setup(rawDelta = 0.005, gamma = 0.0001, tradePrice = 100.0)
    assertEquals(strat(minBand = 0.01).onEvent(ev, sm), Vector.empty)

  // ---- 方向性目标 (MACD 暖机后) ----

  /** 喂 n 根递增/递减 K 线把 MACD 暖机出方向 (greeks 未就绪期间 hedge 自动 no-op, 仅 signal 暖机) */
  private def warmup(s: TargetDeltaHedgeStrategy, sm: StateManager, rising: Boolean, bars: Int = 60): Unit =
    for i <- 1 to bars do
      val price = if rising then 100.0 + i else 200.0 - i
      s.onEvent(tradeEv(price, i * hourMs), sm)

  test("MACD 水上 (上升趋势) + 净delta=0 -> 目标正 delta -> 挂买单 (做多敞口)"):
    val s = strat()
    val sm = StateManager(Iterable(sym), orderTimeoutMs = 5000)
    warmup(s, sm, rising = true) // greeks 未就绪, 仅暖机 signal
    // 暖机后置入 greeks: netDelta=0, gamma 足够大使 target>band
    val finalTs = 61 * hourMs
    val px = 161.0
    sm.apply(IncomeEvent(0, 0, EventData.BalanceUpdate(Balance(ex, ccy, 0.0, 0))))
    sm.apply(IncomeEvent(0, 0, EventData.GreeksUpdate(Greeks(ex, ccy, 0.0, 0.05, -0.5, 1.0, 0))))
    val ev = tradeEv(px, finalTs)
    sm.apply(ev)
    val o = placedOrder(s.onEvent(ev, sm))
    assertEquals(o.side, Side.Long, "水上目标正 delta, netDelta=0 应买入拉高至目标")

  test("MACD 水下 (下降趋势) + 净delta=0 -> 目标负 delta -> 挂卖单 (做空敞口)"):
    val s = strat()
    val sm = StateManager(Iterable(sym), orderTimeoutMs = 5000)
    warmup(s, sm, rising = false)
    val finalTs = 61 * hourMs
    val px = 139.0
    sm.apply(IncomeEvent(0, 0, EventData.BalanceUpdate(Balance(ex, ccy, 0.0, 0))))
    sm.apply(IncomeEvent(0, 0, EventData.GreeksUpdate(Greeks(ex, ccy, 0.0, 0.05, -0.5, 1.0, 0))))
    val ev = tradeEv(px, finalTs)
    sm.apply(ev)
    val o = placedOrder(s.onEvent(ev, sm))
    assertEquals(o.side, Side.Short, "水下目标负 delta, netDelta=0 应卖出压低至目标")

  // ---- repeg: 限价单超时撤单 ----

  test("限价单 repegMs 内未成交 -> 超时后撤单 (追价)"):
    val s = strat(repegMs = 3000)
    val sm = StateManager(Iterable(sym), orderTimeoutMs = 5000)
    sm.apply(IncomeEvent(0, 0, EventData.BalanceUpdate(Balance(ex, ccy, 0.0, 0))))
    sm.apply(IncomeEvent(0, 0, EventData.GreeksUpdate(Greeks(ex, ccy, 0.5, 0.05, -0.5, 1.0, 0))))
    // 注入一张已确认的 Short 在途单 (模拟下单已被交易所确认, 与偏多->卖方向一致)
    sm.apply(
      IncomeEvent(
        0,
        0,
        EventData.OrderUpdated(
          OrderUpdate("1", Some("c1"), ex, sym, Side.Short, OrderStatus.Pending, 100.0, 0.5, 0.0, 0.0, 0)
        ),
      )
    )
    val t0 = 1_000L
    // 首次评估: 采用当前虚拟时间作为挂出时间, 未到 repeg -> 不撤
    assertEquals(s.onEvent(tradeEv(100.0, t0), sm), Vector.empty)
    // 超过 repegMs 后: 撤单
    val out = s.onEvent(tradeEv(100.0, t0 + 3000), sm)
    out match
      case Vector(OutcomeEvent.CancelOrder(e, sy, id)) =>
        assertEquals(e, ex); assertEquals(sy, sym); assertEquals(id, "1")
      case other => fail(s"expected CancelOrder, got $other")

  /** 注入一张已确认 (Pending) 的在途单到 state (模拟交易所确认回报) */
  private def injectPending(sm: StateManager, side: Side, orderId: OrderId, price: Price): Unit =
    sm.apply(
      IncomeEvent(
        0,
        0,
        EventData.OrderUpdated(OrderUpdate(orderId, Some("c" + orderId), ex, sym, side, OrderStatus.Pending, price, 0.5, 0.0, 0.0, 0)),
      )
    )

  test("在途单未到期且方向一致 -> 不重复下单 (in-flight 守卫)"):
    val s = strat(repegMs = 3000)
    // net delta=0.5 > band -> 需卖; 在途已有同向 Short 单
    val (sm, _) = setup(rawDelta = 0.5, gamma = 0.05, tradePrice = 100.0)
    injectPending(sm, Side.Short, "1", 100.0)
    assertEquals(s.onEvent(tradeEv(100.0, 1_000), sm), Vector.empty) // 未到期+同向 -> 不动作

  test("目标方向翻转 -> 立即撤单 (sideChanged, 不等 repeg)"):
    val s = strat(repegMs = 10_000) // repeg 设很长, 证明撤单来自方向翻转而非超时
    // net delta=-0.5 < -band -> 需买(Long); 但在途是反向的 Short -> 应立即撤
    val (sm, _) = setup(rawDelta = -0.5, gamma = 0.05, tradePrice = 100.0)
    injectPending(sm, Side.Short, "1", 100.0)
    s.onEvent(tradeEv(100.0, 1_000), sm) match
      case Vector(OutcomeEvent.CancelOrder(_, _, id)) => assertEquals(id, "1") // 方向不符即撤, 不等 repeg
      case other                                      => fail(s"expected CancelOrder, got $other")

  test("对冲成交后净 delta 拉回带内 -> 不再动作"):
    // 期权 delta=0.5, 模拟 Short 0.5 成交 -> 永续 -0.5 -> 净 delta=0 入带
    val sm = StateManager(Iterable(sym), orderTimeoutMs = 5000)
    sm.apply(IncomeEvent(0, 0, EventData.BalanceUpdate(Balance(ex, ccy, 0.0, 0))))
    sm.apply(IncomeEvent(0, 0, EventData.GreeksUpdate(Greeks(ex, ccy, 0.5, 0.05, -0.5, 1.0, 0))))
    sm.apply(IncomeEvent(2, 2, EventData.FillUpdate(Fill(ex, sym, Side.Short, 100.0, 0.5, 2))))
    val ev = tradeEv(100.0, 3)
    sm.apply(ev)
    assertEquals(strat().onEvent(ev, sm), Vector.empty) // 净 delta 已回 0, 带内不动作

  // ---- 市价执行器 (MarketHedgeExecution) ----

  test("市价执行: 越带 -> 下市价单 (Market), 数量=净delta, 无挂价"):
    val (sm, ev) = setup(rawDelta = 0.5, gamma = 0.05, tradePrice = 100.0) // net 0.5 > band 0.02
    val o = placedOrder(stratMarket().onEvent(ev, sm))
    assertEquals(o.side, Side.Short)
    near(o.quantity, 0.5)
    assertEquals(o.orderType, OrderType.Market)

  test("市价执行: 已有在途单 -> 不重复下 (in-flight 守卫)"):
    val s = stratMarket()
    val (sm, _) = setup(rawDelta = 0.5, gamma = 0.05, tradePrice = 100.0)
    injectPending(sm, Side.Short, "1", 100.0)
    assertEquals(s.onEvent(tradeEv(100.0, 1_000), sm), Vector.empty)

  test("市价执行: 带内 -> 不动作"):
    val (sm, ev) = setup(rawDelta = 0.015, gamma = 0.05, tradePrice = 100.0) // band 0.02 > 0.015
    assertEquals(stratMarket().onEvent(ev, sm), Vector.empty)

  // ---- 价位棘轮 (ratchet) ----

  /** 模拟一次对冲成交：更新 state 仓位 + 喂给策略记录棘轮锚 */
  private def feedFill(s: TargetDeltaHedgeStrategy, sm: StateManager, side: Side, price: Price, size: Double, ts: Timestamp): Unit =
    val ev = IncomeEvent(ts, ts, EventData.FillUpdate(Fill(ex, sym, side, price, size, ts)))
    sm.apply(ev)
    s.onEvent(ev, sm)

  test("棘轮: 同向对冲须越过上次对冲价, 未越过 -> 不对冲"):
    val s = stratMarket(ratchet = true)
    val sm = StateManager(Iterable(sym), orderTimeoutMs = 5000)
    sm.apply(IncomeEvent(0, 0, EventData.BalanceUpdate(Balance(ex, ccy, 0.0, 0))))
    sm.apply(IncomeEvent(0, 0, EventData.GreeksUpdate(Greeks(ex, ccy, 1.5, 0.05, -0.5, 1.0, 0)))) // net 1.5 需 Short
    feedFill(s, sm, Side.Short, 100.0, 0.5, 1) // 锚: Short@100; 仓位 -0.5 -> net 仍 1.0 > band
    // refPrice=101: 同向 Short 需 < 100, 101 不满足 -> 不对冲
    val up = tradeEv(101.0, 10); sm.apply(up)
    assertEquals(s.onEvent(up, sm), Vector.empty)
    // refPrice=99: 99 < 100 -> 越过 -> 下 Short 市价
    val dn = tradeEv(99.0, 11); sm.apply(dn)
    assertEquals(placedOrder(s.onEvent(dn, sm)).side, Side.Short)

  test("棘轮: 反向对冲恒放行 (避免反转锁死)"):
    val s = stratMarket(ratchet = true)
    val sm = StateManager(Iterable(sym), orderTimeoutMs = 5000)
    sm.apply(IncomeEvent(0, 0, EventData.BalanceUpdate(Balance(ex, ccy, 0.0, 0))))
    sm.apply(IncomeEvent(0, 0, EventData.GreeksUpdate(Greeks(ex, ccy, 1.5, 0.05, -0.5, 1.0, 0))))
    feedFill(s, sm, Side.Short, 100.0, 0.5, 1) // 锚 Short@100, 仓位 -0.5
    // 期权 delta 翻负 -> 需反向 Long: net = -1.5 + (-0.5) = -2.0
    sm.apply(IncomeEvent(2, 2, EventData.GreeksUpdate(Greeks(ex, ccy, -1.5, 0.05, -0.5, 1.0, 2))))
    val up = tradeEv(101.0, 12); sm.apply(up) // 价更高, 若按同向逻辑会被挡; 反向应放行
    assertEquals(placedOrder(s.onEvent(up, sm)).side, Side.Long)

  test("棘轮关闭 (默认) -> 同向不受价位限制"):
    val (sm, _) = setup(rawDelta = 1.5, gamma = 0.05, tradePrice = 100.0)
    val s = stratMarket(ratchet = false)
    feedFill(s, sm, Side.Short, 100.0, 0.5, 1)
    val up = tradeEv(101.0, 10); sm.apply(up) // 同向且价不利, 但棘轮关 -> 照常对冲
    assertEquals(placedOrder(s.onEvent(up, sm)).side, Side.Short)

  // ---- 方向自适应带 (AdaptiveBandScaler 接入) ----

  test("自适应带: 对冲收紧后, 原本在带内的小漂移也触发对冲"):
    val scaler = AdaptiveBandScaler(floor = 0.5, shrinkPerHedge = 0.5, recoverPerMin = 0.10)
    scaler.onHedge(Side.Short, 0) // Short 乘子 -> 0.5
    val s = TargetDeltaHedgeStrategy(
      ex, sym, ccy,
      execution = MarketHedgeExecution(ex, sym),
      overlay = MacdBiasOverlay(tiltMoveRatio = 0.0),
      bandMoveRatio = 0.004, minBand = 0.005, bandScaler = Some(scaler),
    )
    // gammaScale=5; 基础带=0.02; 收紧后=0.01; net 0.015 > 0.01 -> 对冲
    val (sm, ev) = setup(rawDelta = 0.015, gamma = 0.05, tradePrice = 100.0)
    assertEquals(placedOrder(s.onEvent(ev, sm)).side, Side.Short)
    // 对照: 无 scaler 时 0.015 < 基础带 0.02 -> 不对冲
    val s2 = TargetDeltaHedgeStrategy(
      ex, sym, ccy,
      execution = MarketHedgeExecution(ex, sym),
      overlay = MacdBiasOverlay(tiltMoveRatio = 0.0),
      bandMoveRatio = 0.004, minBand = 0.005,
    )
    val (sm2, ev2) = setup(rawDelta = 0.015, gamma = 0.05, tradePrice = 100.0)
    assertEquals(s2.onEvent(ev2, sm2), Vector.empty)

  // ---- netDelta 1min KAMA 滤波 (DeltaKamaFilter 接入) ----

  /** 市价执行 + KAMA 滤波版 (tilt=0 关方向 overlay) */
  private def stratKama(bandMoveRatio: Double = 0.004, minBand: Double = 0.01) =
    TargetDeltaHedgeStrategy(
      ex, sym, ccy,
      execution = MarketHedgeExecution(ex, sym),
      overlay = MacdBiasOverlay(tiltMoveRatio = 0.0),
      bandMoveRatio = bandMoveRatio, minBand = minBand,
      deltaFilter = Some(DeltaKamaFilter()),
    )

  test("KAMA 滤波: 预热未满 -> triggerDelta 回退 netDelta, 行为同 baseline (越带即对冲)"):
    // 单事件 -> KAMA 未预热 (value=None) -> trigger==real -> 与 baseline 一致越带下单
    val (sm, ev) = setup(rawDelta = 0.5, gamma = 0.05, tradePrice = 100.0)
    val o = placedOrder(stratKama().onEvent(ev, sm))
    assertEquals(o.side, Side.Short)
    near(o.quantity, 0.5)

  test("KAMA 滤波: 平滑值在带内但真实残差越带 -> 确认门挡住不对冲"):
    val s = stratKama() // band = max(0.01, 0.004*0.05*100) = 0.02
    val sm = StateManager(Iterable(sym), orderTimeoutMs = 5000)
    sm.apply(IncomeEvent(0, 0, EventData.BalanceUpdate(Balance(ex, ccy, 0.0, 0))))
    sm.apply(IncomeEvent(0, 0, EventData.GreeksUpdate(Greeks(ex, ccy, 0.0, 0.05, -0.5, 1.0, 0))))
    // 预热: 15 分钟 netDelta≈0 的逐分钟成交 -> KAMA 收敛到 0 (14 根已收盘 bar, >erPeriod)
    for k <- 1 to 15 do s.onEvent(tradeEv(100.0, k * 60_000L), sm)
    // 真实 delta 突跳到 0.5 (>band)，但 KAMA(已收盘 bar 全为 0) 仍≈0 (<band) -> 平滑门否决
    sm.apply(IncomeEvent(0, 0, EventData.GreeksUpdate(Greeks(ex, ccy, 0.5, 0.05, -0.5, 1.0, 0))))
    val spike = tradeEv(100.0, 16 * 60_000L); sm.apply(spike)
    assertEquals(s.onEvent(spike, sm), Vector.empty, "平滑残差在带内 -> 被确认门挡住")
    // 对照: 无 filter 同样真实敞口 0.5 -> 正常对冲
    val (sm2, ev2) = setup(rawDelta = 0.5, gamma = 0.05, tradePrice = 100.0)
    assertEquals(placedOrder(stratMarket().onEvent(ev2, sm2)).side, Side.Short)

  test("greeks 未就绪 (无 cashBal) -> 不动作"):
    val sm = StateManager(Iterable(sym), orderTimeoutMs = 5000)
    sm.apply(IncomeEvent(0, 0, EventData.GreeksUpdate(Greeks(ex, ccy, 0.5, 0.05, -0.5, 1.0, 0))))
    val ev = tradeEv(100.0, 1)
    sm.apply(ev)
    assertEquals(strat().onEvent(ev, sm), Vector.empty)
