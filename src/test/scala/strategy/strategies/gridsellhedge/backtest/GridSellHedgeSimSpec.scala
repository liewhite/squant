package strategy.strategies.gridsellhedge.backtest

import hft.option.OptionRight
import strategy.strategies.gridsellhedge.logic.DynamicHedgeBand

/** GridSellHedgeSim 集成单测 (3s 轮询 + maker 对冲, 越过行权价即开 / 回落超过阈值即平)。
  *
  * 预热 2 小时 (恒 2000), 之后首个 ≥3s 的轮询种下 call@2100/put@1900。
  * maker 成交: 上一轮挂的买单在窗口最低 ≤ 限价时成交、卖单在窗口最高 ≥ 限价时成交, 成交于限价。 */
class GridSellHedgeSimSpec extends munit.FunSuite:
  private val hr = 3_600_000L
  private val base = 2 * hr

  private def cfg(band: DynamicHedgeBand.Params = DynamicHedgeBand.Params(), slip: Double = 0.0) = GridConfig(
    spacing = 100.0, tenorDays = 30.0, nodeContracts = 1.0,
    rvWindowHours = 2, warmupHours = 2, pollIntervalMs = 3000,
    makerOffsetPct = 0.0001, optFeeRate = 0.0, perpFeeRate = 0.0, slippagePct = slip,
  )

  private def run(c: GridConfig, path: Seq[(Double, Long)]): GridSellHedgeSim =
    val sim = GridSellHedgeSim(c)
    sim.onPrice(2000.0, 0); sim.onPrice(2000.0, hr); sim.onPrice(2000.0, base)
    path.foreach { case (px, off) => sim.onPrice(px, base + off) }
    sim

  // 价升到 call@2100 之上 (越过行权价 -> 开), 随后回落使买单成交
  private val openPath = Seq((2000.0, 3000L), (2110.0, 6000L), (2105.0, 8000L), (2115.0, 9000L), (2115.0, 12000L))
  // 开仓成交后价跌破 K−threshold -> 挂平仓单, 随后上抽成交
  private val closePath = openPath ++ Seq((2085.0, 15000L), (2087.0, 17000L), (2086.0, 18000L))

  test("网格 bracket: 每次轮询补卖当前价上下档, 已持仓不重卖"):
    assertEquals(run(cfg(), Seq((2000.0, 3000L))).stats.sold, 2)                    // 种子 call@2100 + put@1900
    assertEquals(run(cfg(), Seq((2000.0, 3000L), (2150.0, 6000L))).stats.sold, 4)   // 价到 2150 -> 新增 call@2200 + put@2100

  test("越过行权价即开: 价升过 call@2100 后回落使买单成交 -> 净标的 > 0"):
    val sim = run(cfg(), openPath)
    assert(sim.stats.hedgeOpens >= 1, s"越过行权价后回落, 买单应成交, 实际 opens=${sim.stats.hedgeOpens}")
    assert(sim.netUnderlying > 0.0, s"短 call 备兑为多标的, netPos 应>0, 实际 ${sim.netUnderlying}")

  test("强趋势中被动买单不成交, 且挂单期间阈值不放大"):
    val up = Seq((2000.0, 3000L), (2110.0, 6000L), (2140.0, 9000L), (2170.0, 12000L), (2200.0, 15000L))
    val sim = run(cfg(), up)
    assertEquals(sim.stats.hedgeOpens, 0, "单调上行无回落 -> maker 买单不成交")
    assertEquals(sim.thresholdOf(OptionRight.Call, 2100.0), Some(0.004), "未成交则离场阈值不放大")

  test("阈值随成交放大 (非挂单): 越过行权价成交后 call@2100 离场阈值 ×1.2"):
    val sim = run(cfg(), openPath)
    assert(sim.stats.hedgeOpens >= 1)
    assertEquals(sim.thresholdOf(OptionRight.Call, 2100.0), Some(0.004 * 1.2))

  test("回落超过阈值即平: 成交后价跌破 K−threshold -> 平仓单成交"):
    val sim = run(cfg(), closePath)
    assert(sim.stats.hedgeOpens >= 1, "先开仓")
    assert(sim.stats.hedgeCloses >= 1, s"价回落超过离场阈值应平仓, 实际 closes=${sim.stats.hedgeCloses}")

  test("到期结算: OTM 期权到期作废收满权利金≈0, 档位释放后同轮补卖"):
    val c = cfg().copy(tenorDays = 0.05) // ~1.2h 到期
    val sim = GridSellHedgeSim(c)
    sim.onPrice(2000.0, 0); sim.onPrice(2000.0, hr); sim.onPrice(2000.0, base)
    for k <- 1 to 90 do sim.onPrice(2000.0, base + k * 60_000L)
    val st = sim.stats
    assertEquals(st.expired, 2, "首批 call@2100/put@1900 到期")
    assertEquals(st.sold, 4, "到期同轮释放并补卖 -> 共卖 4")
    assert(math.abs(st.optRealized) < 1.0, s"OTM 作废收满权利金≈0, 实际 ${st.optRealized}")
    assertEquals(sim.openPositions, 2, "补卖后仍持 2 档")

  test("滑点一致性: slip 只作显式成本, pnl 差异 = slip×Σ|qty|×成交价 (已实现/MTM 口径一致)"):
    val s0 = run(cfg(slip = 0.0), closePath)
    val sS = run(cfg(slip = 0.001), closePath)
    val notional = s0.fills.map { case (_, _, px, q) => math.abs(q) * px }.sum
    assert(s0.fills.nonEmpty, "应至少有开+平成交")
    val pnl0 = s0.finalEquity - cfg().initialBalance
    val pnlS = sS.finalEquity - cfg().initialBalance
    assertEqualsDouble(pnl0 - pnlS, 0.001 * notional, 1e-6)

  test("平稳无趋势: 档位之间 ±$1 抖动不越过行权价, 权益近似持平"):
    // 中心 2050 (非网格线) -> 上下档 call@2100/put@2000, ±$1 抖动不触及, 不开对冲
    val jitter = (1 to 60).map(i => (2050.0 + (if i % 2 == 0 then 1.0 else -1.0), 3000L * i))
    val sim = run(cfg(), jitter)
    assertEquals(sim.stats.hedgeOpens, 0, "档位间小幅抖动不越过 call@2100/put@2000")
    assert(math.abs(sim.finalEquity - cfg().initialBalance) < 50.0, s"平稳期权益应近似持平, 实际 ${sim.finalEquity - cfg().initialBalance}")
