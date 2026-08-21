package strategy.utils

import hft.domain.Side
import strategy.utils.PositionSizing.{targetQty, orderFor}

/** 离散仓位原语单测：按权益折算目标币数 (多空对称) + 由 gap 推市价单 (含反手一次跨零)。 */
class PositionSizingSpec extends munit.FunSuite:

  test("targetQty: dir×leverage×权益/价; 平/非法价/非法权益 -> 0; 多空对称"):
    assertEqualsDouble(targetQty(1, 1.0, 100000.0, 2000.0), 50.0, 1e-9)   // 满仓 = 权益/价
    assertEqualsDouble(targetQty(1, 2.0, 100000.0, 2000.0), 100.0, 1e-9)  // 2× 杠杆
    assertEqualsDouble(targetQty(-1, 1.0, 80000.0, 2000.0), -40.0, 1e-9)  // 空头对称, 权益缩小->敞口同步缩小
    assertEqualsDouble(targetQty(0, 1.0, 100000.0, 2000.0), 0.0, 1e-9)    // 平
    assertEqualsDouble(targetQty(1, 1.0, 100000.0, 0.0), 0.0, 1e-9)       // 非法价
    assertEqualsDouble(targetQty(1, 1.0, -5.0, 2000.0), 0.0, 1e-9)        // 非法权益

  test("orderFor: gap≥minQty 才下单, side 由 gap 符号定; 反手一次跨零"):
    assertEquals(orderFor(50.0, 0.0, 0.001), Some((Side.Long, 50.0)))     // 开多
    assertEquals(orderFor(0.0, 50.0, 0.001), Some((Side.Short, 50.0)))    // 平多
    assertEquals(orderFor(-40.0, 50.0, 0.001), Some((Side.Short, 90.0)))  // 多->空, 一次卖 90 跨零
    assertEquals(orderFor(50.0, 50.0005, 0.001), None)                    // gap<minQty -> 不动
