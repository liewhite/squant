package strategy.utils.hedge

import hft.domain.Coin
import hft.TestUnits.given

/** DeltaBand 单测：判据是真实敞口、两个信号只缩放阈值、敞口上界、方向语义、参数校验。 */
class DeltaBandSpec extends munit.FunSuite:
  private val base = Coin(0.4)

  /** ER 缺省给 None (体制系数 1.0), 单独测 MACD 那一维 */
  private def ctx(macdDir: Int, er: Option[Double] = None) = DeltaCtx(Coin.Zero, macdDir, er)

  test("MACD 看多 -> 空头敞口侧减半 (逆势侧收紧, 更快消除空头敞口)"):
    val (up, down) = DeltaBand.adaptive(base, macdTighten = 0.5).bands(ctx(1))
    assertEquals(up, base)             // 多头敞口 = 顺势, 不收紧
    assertEquals(down, Coin(0.2))      // 空头敞口 = 逆势, 减半

  test("MACD 看空 -> 多头敞口侧减半 (镜像)"):
    val (up, down) = DeltaBand.adaptive(base, macdTighten = 0.5).bands(ctx(-1))
    assertEquals(up, Coin(0.2))
    assertEquals(down, base)

  test("MACD 预热不足/持平 -> 对称, 不猜方向"):
    val (up, down) = DeltaBand.adaptive(base, macdTighten = 0.5).bands(ctx(0))
    assertEquals(up, base)
    assertEquals(down, base)

  test("收紧系数 1.0 退化为固定死区 (等价 fixed)"):
    val a = DeltaBand.adaptive(base, macdTighten = 1.0).bands(ctx(1))
    val b = DeltaBand.fixed(base).bands(ctx(1))
    assertEquals(a, b)
    assertEquals(a, (base, base))

  // ---------- 体制缩放 (ER): 判据不变, 只动阈值 ----------

  test("ER→0 (震荡) 放宽阈值 -> 少对冲"):
    val b = DeltaBand.adaptive(base, macdTighten = 1.0, chopWiden = 2.0, trendTighten = 0.5)
    assertEquals(b.bands(ctx(0, Some(0.0))), (Coin(0.8), Coin(0.8)))

  test("ER→1 (趋势) 收紧阈值 -> 尽快跟上"):
    val b = DeltaBand.adaptive(base, macdTighten = 1.0, chopWiden = 2.0, trendTighten = 0.5)
    assertEquals(b.bands(ctx(0, Some(1.0))), (Coin(0.2), Coin(0.2)))

  test("ER 居中 -> 线性插值 (无跳变, 避免阈值附近抖动)"):
    val b = DeltaBand.adaptive(base, macdTighten = 1.0, chopWiden = 2.0, trendTighten = 0.5)
    val (up, _) = b.bands(ctx(0, Some(0.5)))
    assertEquals(up, Coin(0.4 * 1.25)) // 2.0 与 0.5 的中点 = 1.25
    // 单调: ER 越大阈值越小
    val seq = Seq(0.0, 0.25, 0.5, 0.75, 1.0).map(er => b.bands(ctx(0, Some(er)))._1.value)
    assertEquals(seq, seq.sorted.reverse)

  test("ER 预热不足 -> 体制系数 1.0 (用基准阈值, 不猜体制)"):
    val b = DeltaBand.adaptive(base, macdTighten = 1.0, chopWiden = 2.0, trendTighten = 0.5)
    assertEquals(b.bands(ctx(0, None)), (base, base))

  test("两个信号相乘叠加"):
    val b = DeltaBand.adaptive(base, macdTighten = 0.5, chopWiden = 2.0, trendTighten = 0.5)
    // 震荡 (×2) + 看多 (空头侧再 ×0.5)
    assertEquals(b.bands(ctx(1, Some(0.0))), (Coin(0.8), Coin(0.4)))

  test("**真实敞口有上界**: 任何信号组合下阈值都不超过 base × chopWiden"):
    val b = DeltaBand.adaptive(base, macdTighten = 0.5, chopWiden = 2.0, trendTighten = 0.5)
    val cap = base.scaled(2.0)
    for
      dir <- Seq(-1, 0, 1)
      er <- Seq(None, Some(0.0), Some(0.3), Some(0.7), Some(1.0))
    do
      val (up, down) = b.bands(DeltaCtx(Coin.Zero, dir, er))
      assert(up <= cap && down <= cap, s"dir=$dir er=$er -> ($up,$down) 超过上界 $cap")

  test("fixed: 任何信号都不改变阈值 (校验口径的基准)"):
    val f = DeltaBand.fixed(base)
    for dir <- Seq(-1, 0, 1); er <- Seq(None, Some(0.0), Some(1.0)) do
      assertEquals(f.bands(DeltaCtx(Coin.Zero, dir, er)), (base, base))

  test("阈值/系数非法 -> 抛错 (不静默跑一个没有死区的对冲)"):
    intercept[IllegalArgumentException](DeltaBand.adaptive(Coin(0.0)).bands(ctx(0)))
    intercept[IllegalArgumentException](DeltaBand.adaptive(base, macdTighten = 0.0).bands(ctx(0)))
    intercept[IllegalArgumentException](DeltaBand.adaptive(base, macdTighten = 1.5).bands(ctx(0)))
    intercept[IllegalArgumentException](DeltaBand.adaptive(base, chopWiden = 0.9).bands(ctx(0)))
    intercept[IllegalArgumentException](DeltaBand.adaptive(base, trendTighten = 0.0).bands(ctx(0)))
    intercept[IllegalArgumentException](DeltaBand.adaptive(base, trendTighten = 1.5).bands(ctx(0)))
