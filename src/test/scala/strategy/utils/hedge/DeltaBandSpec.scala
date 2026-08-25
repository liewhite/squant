package strategy.utils.hedge

import hft.domain.Coin
import hft.TestUnits.given

/** DeltaBand 单测：MACD 顺势侧收紧的方向语义、对称退化、参数校验。 */
class DeltaBandSpec extends munit.FunSuite:
  private val base = Coin(0.4)

  test("MACD 看多 -> 空头敞口侧减半 (逆势侧收紧, 更快消除空头敞口)"):
    val (up, down) = DeltaBand.macdTightened(base, 0.5).bands(DeltaCtx(Coin.Zero, macdDir = 1))
    assertEquals(up, base)             // 多头敞口 = 顺势, 不收紧
    assertEquals(down, Coin(0.2))      // 空头敞口 = 逆势, 减半

  test("MACD 看空 -> 多头敞口侧减半 (镜像)"):
    val (up, down) = DeltaBand.macdTightened(base, 0.5).bands(DeltaCtx(Coin.Zero, macdDir = -1))
    assertEquals(up, Coin(0.2))
    assertEquals(down, base)

  test("MACD 预热不足/持平 -> 对称, 不猜方向"):
    val (up, down) = DeltaBand.macdTightened(base, 0.5).bands(DeltaCtx(Coin.Zero, macdDir = 0))
    assertEquals(up, base)
    assertEquals(down, base)

  test("收紧系数 1.0 退化为对称死区 (等价 symmetric)"):
    val a = DeltaBand.macdTightened(base, 1.0).bands(DeltaCtx(Coin.Zero, 1))
    val b = DeltaBand.symmetric(base).bands(DeltaCtx(Coin.Zero, 1))
    assertEquals(a, b)
    assertEquals(a, (base, base))

  test("阈值/系数非法 -> 抛错 (不静默跑一个没有死区的对冲)"):
    intercept[IllegalArgumentException](DeltaBand.macdTightened(Coin(0.0)).bands(DeltaCtx(Coin.Zero, 0)))
    intercept[IllegalArgumentException](DeltaBand.macdTightened(base, 0.0).bands(DeltaCtx(Coin.Zero, 0)))
    intercept[IllegalArgumentException](DeltaBand.macdTightened(base, 1.5).bands(DeltaCtx(Coin.Zero, 0)))
