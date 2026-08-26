package strategy.utils.hedge

import hft.TestUnits.given
import hft.domain.{Coin, Price}
import hft.option.BlackScholes

/** DeltaBand 单测：阈值以"预测波动范围"为单位、方向决定不对称、两个 clamp 各管一头、参数校验。 */
class DeltaBandSpec extends munit.FunSuite:
  private val hour = 60 * 60_000L

  /** 一个刻意好算的场景: |gamma|=0.01, spot=3000, σ=1.0(年化), τ=1年 -> range = 30 */
  private val year = BlackScholes.MillisPerYear.toLong
  private def ctx(dir: Int, sigma: Option[Double] = Some(1.0), gamma: Double = 0.01, spot: Double = 3000.0) =
    DeltaCtx(Coin.Zero, dir, Coin(gamma), Price(spot), sigma)

  private def band(tight: Double = 0.5, loose: Double = 2.0, horizon: Long = year,
                   min: Coin = Coin(0.0001), max: Coin = Coin(1e9)) =
    DeltaBand.volScaled(tight, loose, horizon, min, max)

  test("阈值 = |gamma| × 现价 × σ × √(τ/年) × 倍数"):
    // range = 0.01 × 3000 × 1.0 × √1 = 30
    val (up, down) = band().bands(ctx(0))
    assertEquals(up, Coin(30.0), "方向未知 -> 两侧都是 1.0 倍 range")
    assertEquals(down, Coin(30.0))

  test("方向未知 -> 对称 (不猜)"):
    val (up, down) = band().bands(ctx(0))
    assertEquals(up, down)

  test("预测上涨 -> 空头敞口侧收紧 (漂移把敞口推向那边, 要尽快跟上)"):
    // 空头跨式: 价格涨 -> delta 更负 -> 账户偏空 -> 需买入 -> 收紧空头侧
    val (up, down) = band(tight = 0.5, loose = 2.0).bands(ctx(1))
    assertEquals(down, Coin(15.0), "空头侧 = 0.5 × 30")
    assertEquals(up, Coin(60.0), "多头侧 = 2.0 × 30, 放到噪声之外")

  test("预测下跌 -> 镜像"):
    val (up, down) = band(tight = 0.5, loose = 2.0).bands(ctx(-1))
    assertEquals(up, Coin(15.0))
    assertEquals(down, Coin(60.0))

  test("波动翻倍 -> 阈值翻倍 (对冲频率才不会变四倍)"):
    val b = band()
    val (a1, _) = b.bands(ctx(0, sigma = Some(0.5)))
    val (a2, _) = b.bands(ctx(0, sigma = Some(1.0)))
    assertEquals(a2.value, a1.value * 2, s"σ 翻倍阈值应翻倍: $a1 -> $a2")

  test("gamma 翻倍 -> 阈值翻倍 (敞口扩散更快, 带子按比例放宽以控成本)"):
    val b = band()
    val (g1, _) = b.bands(ctx(0, gamma = 0.01))
    val (g2, _) = b.bands(ctx(0, gamma = 0.02))
    assertEquals(g2.value, g1.value * 2)

  test("时间尺度按 √t 缩放 (扩散是布朗的, 不是线性的)"):
    val one = band(horizon = hour).bands(ctx(0))._1.value
    val four = band(horizon = 4 * hour).bands(ctx(0))._1.value
    assert(math.abs(four - one * 2.0) < 1e-9, s"4 倍时间应给 2 倍阈值: $one -> $four")

  test("maxTheta 是敞口硬上界: 高波动时咬住, 那正是该咬的时候"):
    val b = band(tight = 0.5, loose = 2.0, max = Coin(20.0))
    val (up, down) = b.bands(ctx(1))
    assertEquals(up, Coin(20.0), "放宽侧本该 60, 被风险上限压到 20")
    assertEquals(down, Coin(15.0), "收紧侧 15 未触顶, 不受影响")

  test("minTheta 防波动塌缩: σ→0 时不退化成逐笔抖动"):
    val b = band(min = Coin(0.05))
    assertEquals(b.bands(ctx(0, sigma = Some(0.0)))._1, Coin(0.05))
    assertEquals(b.bands(ctx(0, gamma = 0.0))._1, Coin(0.05), "无期权持仓 -> gamma=0 -> 同样取下限")

  test("σ 未就绪 -> 取下限 (宁可对冲频繁, 也不按一个不存在的波动率放宽敞口)"):
    val b = band(min = Coin(0.05))
    assertEquals(b.bands(ctx(1, sigma = None)), (Coin(0.05), Coin(0.05)))

  test("gamma 取绝对值: 多头组合 (正 gamma) 与空头 (负 gamma) 同样定阈值"):
    val b = band()
    assertEquals(b.bands(ctx(0, gamma = 0.01))._1, b.bands(ctx(0, gamma = -0.01))._1)

  test("fixed: 任何观测都不改变阈值 (校验口径的基准)"):
    val f = DeltaBand.fixed(Coin(0.4))
    for dir <- Seq(-1, 0, 1); sigma <- Seq(None, Some(0.1), Some(5.0)) do
      assertEquals(f.bands(ctx(dir, sigma)), (Coin(0.4), Coin(0.4)))

  test("参数非法 -> 抛错 (不静默跑一个没有死区的对冲)"):
    intercept[IllegalArgumentException](DeltaBand.fixed(Coin(0.0)))
    intercept[IllegalArgumentException](band(tight = 0.0))
    intercept[IllegalArgumentException](band(tight = 1.5))
    intercept[IllegalArgumentException](band(loose = 0.9))
    intercept[IllegalArgumentException](band(horizon = 0))
    intercept[IllegalArgumentException](band(min = Coin(0.0)))
    intercept[IllegalArgumentException](band(min = Coin(1.0), max = Coin(0.5)))
