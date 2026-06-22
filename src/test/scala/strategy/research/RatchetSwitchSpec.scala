package strategy.research

import hft.strategy.{OutcomeEvent, Strategy}

/** 棘轮开关单测：初始启用 / 大反弹关闭 / 关闭期下杀重启(返回 true) / 阈值边界。 */
class RatchetSwitchSpec extends munit.FunSuite:

  test("初始启用"):
    assert(RatchetSwitch().enabled)

  test("自低点反弹 ≥disablePct -> 关闭"):
    val s = RatchetSwitch(disablePct = 0.04, rearmPct = 0.04)
    s.update(100.0); s.update(95.0) // lo=95, 阈值 95*1.04=98.8
    assertEquals(s.update(98.0), false); assert(s.enabled)  // +3.2% 未到
    assertEquals(s.update(99.0), false); assert(!s.enabled) // ≥98.8 -> 关闭

  test("关闭后下杀 ≥rearmPct -> 重新启用并返回 true"):
    val s = RatchetSwitch(disablePct = 0.04, rearmPct = 0.04)
    s.update(100.0); s.update(95.0); s.update(99.0) // 关闭, peak=99
    s.update(105.0) // peak=105, 重启阈值 105*0.96=100.8
    assertEquals(s.update(101.0), false); assert(!s.enabled) // 未到
    assertEquals(s.update(100.0), true); assert(s.enabled)   // ≤100.8 -> 重启

  test("启用期持续下跌不触发关闭 (只跟最低点)"):
    val s = RatchetSwitch(disablePct = 0.04, rearmPct = 0.04)
    for p <- List(100.0, 98.0, 96.0, 94.0, 92.0) do assertEquals(s.update(p), false)
    assert(s.enabled)
