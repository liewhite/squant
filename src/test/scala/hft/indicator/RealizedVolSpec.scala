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

  test("annualized: 样本不足 / barsPerYear 非正 -> 抛错, 不返回 0"):
    // 0 是合法的波动率取值, 用它表示"算不出"会让调用方分不清市场静止与没数据
    intercept[IllegalArgumentException](RealizedVol.annualized(Vector.empty, barsPerYearHourly))
    intercept[IllegalArgumentException](RealizedVol.annualized(Vector(0.01), barsPerYearHourly))
    intercept[IllegalArgumentException](RealizedVol.annualized(Vector.fill(5)(0.01), 0.0))

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

  test("非正收盘价 -> 抛错并带上 bar 时间, 不静默缩短窗口"):
    val k = new KlineSeries(hour, 64) with RealizedVol:
      override protected def rvShortBars = 2
      override protected def rvLongBars = 3
    // 跳过坏根会让年化窗口在不告知调用方的前提下变短, 且下一根也跟着丢
    val e = intercept[IllegalArgumentException]:
      Seq(1000.0, 0.0, 1010.0, 1020.0).zipWithIndex.foreach((p, i) => k.update(i.toLong * hour, p))
    assert(e.getMessage.contains("收盘价必须为正"), e.getMessage)

  test("首根无前收 -> 只是没有收益可推, 不报错"):
    val k = new KlineSeries(hour, 64) with RealizedVol:
      override protected def rvShortBars = 2
      override protected def rvLongBars = 3
    Seq(1000.0, 1010.0, 1020.0).zipWithIndex.foreach((p, i) => k.update(i.toLong * hour, p))
    assert(k.rvShort.forall(v => v.isFinite && v >= 0.0))

  test("窗口小于 2 根收益 -> 构造即拒绝"):
    intercept[IllegalArgumentException]:
      new KlineSeries(hour, 64) with RealizedVol:
        override protected def rvShortBars = 1

  test("预热不足 -> None"):
    val k = new KlineSeries(hour, 64) with RealizedVol:
      override protected def rvShortBars = 3
      override protected def rvLongBars = 6
    (0 to 2).foreach(i => k.update(i.toLong * hour, 1000.0 + i)) // 仅 2 个收益 < 短窗 3
    assertEquals(k.rvShort, None)
    assertEquals(k.rvLong, None)
    assertEquals(k.volRatio, None)

  // 由价格序列 (最旧->最新) 构造: 相邻对数收益恰为 rets
  private def pricesFor(rets: Seq[Double], p0: Double = 1000.0): Vector[Double] =
    rets.scanLeft(p0)((p, r) => p * math.exp(r)).toVector

  test("annualizedFromPrices: 对零求和口径, 恒定收益 r -> |r|·sqrt(barsPerYear)"):
    val r = 0.01
    val prices = pricesFor(Seq.fill(5)(r))
    near(RealizedVol.annualizedFromPrices(prices, barsPerYearHourly).get, r * math.sqrt(barsPerYearHourly))

  test("annualizedFromPrices: 收益数不足 -> None (不是 0)"):
    assertEquals(RealizedVol.annualizedFromPrices(Vector(1000.0), barsPerYearHourly), None)
    assertEquals(RealizedVol.annualizedFromPrices(Vector(1000.0, 1010.0), barsPerYearHourly), None) // 1 个收益

  test("annualizedFromPrices: 非正价格 -> 抛错并指出位置"):
    val e = intercept[IllegalArgumentException]:
      RealizedVol.annualizedFromPrices(Vector(1000.0, 0.0, 1010.0, 1020.0), barsPerYearHourly)
    assert(e.getMessage.contains("第 1 个元素"), e.getMessage)

  test("annualizedSampleStdFromPrices: 去均值口径, 恒定收益 -> 0 (与对零求和不同)"):
    val prices = pricesFor(Seq.fill(5)(0.01))
    near(RealizedVol.annualizedSampleStdFromPrices(prices, barsPerYearHourly).get, 0.0) // 方差为 0
    assertEquals(RealizedVol.annualizedSampleStdFromPrices(pricesFor(Seq(0.01)), barsPerYearHourly), None) // < 2 收益

  test("annualizedSampleStdFromPrices: 两收益 a,b -> |a-b|/√2 · sqrt(barsPerYear)"):
    val a = 0.01; val b = 0.03
    val expected = (math.abs(a - b) / math.sqrt(2.0)) * math.sqrt(barsPerYearHourly)
    near(RealizedVol.annualizedSampleStdFromPrices(pricesFor(Seq(a, b)), barsPerYearHourly).get, expected)
