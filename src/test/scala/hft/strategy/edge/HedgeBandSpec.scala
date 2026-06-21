package hft.strategy.edge

/** 对冲带策略 (纯函数) 单测：对称基线、波动体制收放、方向不对称、复合，及共享因子的 clamp 行为。 */
class HedgeBandSpec extends munit.FunSuite:
  private def ctx(volRatio: Double = 1.0, bias: Int = 0, maBias: Int = 0, atr: Double = 1.0): HedgeCtx =
    HedgeCtx(px = 100.0, center = 100.0, atr = atr, volRatio = volRatio, macdBias = bias, maBias = maBias)

  private def near(a: Double, b: Double): Unit = assert(math.abs(a - b) < 1e-9, s"expected $b got $a")
  private def bandsNear(got: (Double, Double), up: Double, down: Double): Unit =
    near(got._1, up); near(got._2, down)

  test("SymmetricAtrBand: 上下恒为 atrMult×ATR"):
    bandsNear(SymmetricAtrBand(2.0).bands(ctx(atr = 1.5)), 3.0, 3.0)

  test("VolRegimeBand: volRatio=1 -> 不变; 放大 -> 收窄; 平静 -> 放宽"):
    val b = VolRegimeBand(2.0, minFactor = 0.5, maxFactor = 2.0)
    bandsNear(b.bands(ctx(volRatio = 1.0)), 2.0, 2.0)
    bandsNear(b.bands(ctx(volRatio = 2.0)), 1.0, 1.0) // factor 0.5
    bandsNear(b.bands(ctx(volRatio = 0.5)), 4.0, 4.0) // factor 2.0

  test("VolRegimeBand: 因子 clamp 到 [minFactor,maxFactor]"):
    val b = VolRegimeBand(2.0, minFactor = 0.5, maxFactor = 2.0)
    bandsNear(b.bands(ctx(volRatio = 10.0)), 1.0, 1.0)  // 1/10 clamp 到 0.5
    bandsNear(b.bands(ctx(volRatio = 0.1)), 4.0, 4.0)   // 1/0.1=10 clamp 到 2.0

  test("DirectionalBand: 偏多上带宽下带窄, 偏空镜像, 中性对称"):
    val b = DirectionalBand(2.0, skew = 0.5)
    bandsNear(b.bands(ctx(bias = 0)), 2.0, 2.0)
    bandsNear(b.bands(ctx(bias = 2)), 3.0, 1.0)   // s=+0.5 -> up 2*1.5, down 2*0.5
    bandsNear(b.bands(ctx(bias = -2)), 1.0, 3.0)  // s=-0.5 镜像
    bandsNear(b.bands(ctx(bias = 1)), 2.5, 1.5)   // s=+0.25

  test("CompositeBand: 体制收放后再方向不对称"):
    val b = CompositeBand(2.0, minFactor = 0.5, maxFactor = 2.0, skew = 0.5)
    // volRatio=2 -> base 2*0.5=1; bias=2 -> s=0.5 -> up 1.5, down 0.5
    bandsNear(b.bands(ctx(volRatio = 2.0, bias = 2)), 1.5, 0.5)
    // 中性时退化为对称基线
    bandsNear(b.bands(ctx(volRatio = 1.0, bias = 0)), 2.0, 2.0)

  test("MaSideBand: 均线上卖单(上带)更远, 均线下更近, 买单(下带)不变"):
    val b = MaSideBand(2.0, skew = 0.5)
    bandsNear(b.bands(ctx(maBias = 0)), 2.0, 2.0)   // 均线预热中 -> 对称基线
    bandsNear(b.bands(ctx(maBias = 1)), 3.0, 2.0)   // 均线上: 上带 2*1.5, 下带不变
    bandsNear(b.bands(ctx(maBias = -1)), 1.0, 2.0)  // 均线下: 上带 2*0.5, 下带不变

  test("HedgeBand 共享因子: clamp / regimeFactor / skewOf"):
    near(HedgeBand.clamp(5.0, 0.5, 2.0), 2.0)
    near(HedgeBand.clamp(0.1, 0.5, 2.0), 0.5)
    near(HedgeBand.clamp(1.0, 0.5, 2.0), 1.0)
    near(HedgeBand.regimeFactor(0.0, 0.5, 2.0), 1.0) // 非法 volRatio -> 中性
    near(HedgeBand.skewOf(0, 0.5), 0.0)
    near(HedgeBand.skewOf(2, 0.5), 0.5)
    near(HedgeBand.skewOf(-2, 0.5), -0.5)
