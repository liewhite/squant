package strategy.strategies.gridsellhedge.logic

import hft.option.OptionRight
import strategy.strategies.gridsellhedge.logic.DynamicHedgeBand.{Params, State}

/** DynamicHedgeBand 阈值动态单测: 风险方向 · 衰减/下限 · 越过放大 · 平仓重置时钟。 */
class DynamicHedgeBandSpec extends munit.FunSuite:
  private val p = Params() // 初值 0.4% / ×1.2 / ×0.9 / 1h
  private val hr = 3_600_000L

  test("风险方向: call 用 (S−K)/K, put 用 (K−S)/K"):
    assertEqualsDouble(DynamicHedgeBand.riskyDistance(OptionRight.Call, 2000, 2005), 0.0025, 1e-9)
    assertEqualsDouble(DynamicHedgeBand.riskyDistance(OptionRight.Put, 2000, 1995), 0.0025, 1e-9)

  test("initial: 阈值=初值 0.4%, 时钟=now"):
    val s = DynamicHedgeBand.initial(123L, p)
    assertEqualsDouble(s.threshold, 0.004, 1e-12)
    assertEquals(s.lastDecayTs, 123L)

  test("decayed: 未触发满 1 小时 → 阈值 ×0.9, 时钟前移"):
    val s0 = State(threshold = 0.0048, lastDecayTs = 0)
    val s1 = DynamicHedgeBand.decayed(s0, hr, p)
    assertEqualsDouble(s1.threshold, 0.00432, 1e-9) // 0.0048*0.9
    assertEquals(s1.lastDecayTs, hr)

  test("decayed: 不足 1 小时不变"):
    val s0 = State(threshold = 0.0048, lastDecayTs = 0)
    assertEquals(DynamicHedgeBand.decayed(s0, hr - 1, p), s0)

  test("decayed: 多小时后不低于初始阈值 0.4%"):
    val s0 = State(threshold = 0.0048, lastDecayTs = 0)
    val s1 = DynamicHedgeBand.decayed(s0, 3 * hr, p) // 0.0048*0.9^3 < 0.004
    assertEqualsDouble(s1.threshold, 0.004, 1e-9)

  test("expandOnOpen: 阈值 ×1.2"):
    val s0 = State(threshold = 0.004, lastDecayTs = 0)
    assertEqualsDouble(DynamicHedgeBand.expandOnOpen(s0, p).threshold, 0.0048, 1e-9)

  test("resetClock: 平仓后衰减时钟回到 now"):
    val s0 = State(threshold = 0.005, lastDecayTs = 0)
    assertEquals(DynamicHedgeBand.resetClock(s0, 7 * hr).lastDecayTs, 7 * hr)
