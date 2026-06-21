package hft.indicator

/** RealizedVol 指标单测：年化公式、滚动短/长窗口就绪与取值、volRatio 体制比、预热不足返回 None。 */
class RealizedVolSpec extends munit.FunSuite:
  private val hour = 3_600_000L
  private val barsPerYearHourly = 365.0 * 24.0 // = MillisPerYear/hour

  private def near(a: Double, b: Double, eps: Double = 1e-6): Unit =
    assert(math.abs(a - b) < eps, s"expected $b got $a")

  test("annualized: 恒定对数收益 r -> |r|·sqrt(barsPerYear)"):
    val r = 0.01
    near(RealizedVol.annualized(Vector.fill(5)(r), barsPerYearHourly), r * math.sqrt(barsPerYearHourly))
    near(RealizedVol.annualized(Vector.empty, barsPerYearHourly), 0.0)
    near(RealizedVol.annualized(Vector(0.01), barsPerYearHourly), 0.0) // < 2 个

  test("滚动窗口: 恒定收益下 rvShort=rvLong, volRatio≈1"):
    val k = new KlineSeries(hour, 64) with RealizedVol:
      override protected def rvShortBars = 3
      override protected def rvLongBars = 6
    val r = 0.01
    // 在每个 bucket 喂一笔, 第 i 笔价 = 1000·exp(r·i); 喂到 bucket 8 -> 收盘 7 根 -> 6 个收益
    (0 to 7).foreach(i => k.update(i.toLong * hour, 1000.0 * math.exp(r * i)))
    assert(k.rvShort.isDefined && k.rvLong.isDefined)
    near(k.rvShort.get, r * math.sqrt(barsPerYearHourly), 1e-9)
    near(k.rvLong.get, r * math.sqrt(barsPerYearHourly), 1e-9)
    near(k.volRatio.get, 1.0, 1e-9)

  test("近端波动放大 -> volRatio>1"):
    val k = new KlineSeries(hour, 64) with RealizedVol:
      override protected def rvShortBars = 3
      override protected def rvLongBars = 6
    // 前 3 个收益 r 小, 后 3 个收益 R 大 -> 短窗(取最后3)=R, 长窗(6)介于 -> ratio>1
    val small = 0.005; val big = 0.02
    val rets = Seq(small, small, small, big, big, big)
    var px = 1000.0
    k.update(0L, px) // 开 bar0
    rets.zipWithIndex.foreach { case (rr, i) => px *= math.exp(rr); k.update((i + 1).toLong * hour, px) }
    k.update(7L * hour, px) // 再跨一根, 收盘第 7 根 -> 凑满 6 个收益 (rvLong)
    assert(k.volRatio.get > 1.0, s"expected volRatio>1, got ${k.volRatio.get}")
    near(k.rvShort.get, big * math.sqrt(barsPerYearHourly), 1e-9)

  test("非正价格 / 首根无前收 -> 不污染收益序列"):
    val k = new KlineSeries(hour, 64) with RealizedVol:
      override protected def rvShortBars = 2
      override protected def rvLongBars = 3
    // 含 0 价的根: 跨该根的收益 (log) 应被跳过, 不入序列, 不抛异常
    val prices = Seq(1000.0, 0.0, 1010.0, 1020.0, 1030.0, 1040.0)
    prices.zipWithIndex.foreach { case (p, i) => k.update(i.toLong * hour, p) }
    // 即便喂了含 0 的根, 也不应抛 (NaN/Inf 防护); 取值有限
    k.rvShort.foreach(v => assert(v.isFinite && v >= 0.0, s"rvShort 非有限: $v"))

  test("预热不足 -> None"):
    val k = new KlineSeries(hour, 64) with RealizedVol:
      override protected def rvShortBars = 3
      override protected def rvLongBars = 6
    (0 to 2).foreach(i => k.update(i.toLong * hour, 1000.0 + i)) // 仅 2 个收益 < 短窗 3
    assertEquals(k.rvShort, None)
    assertEquals(k.rvLong, None)
    assertEquals(k.volRatio, None)
