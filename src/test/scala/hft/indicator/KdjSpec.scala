package hft.indicator

/** KDJ trait 单测：预热门控、单边趋势方向、K/D 范围；以及多指标同时 mix-in。 */
class KdjSpec extends munit.FunSuite:
  private val hour = 3_600_000L

  test("预热不足 (< period 根已收盘) -> 方向 0"):
    val k = new KlineSeries(hour, 200) with Kdj
    (0 until 5).foreach(h => k.update(h.toLong * hour, 100.0 + h))
    assertEquals(k.kdjDirection, 0)

  test("持续上涨 -> K 高且 K>D (看多 +1)"):
    val k = new KlineSeries(hour, 200) with Kdj
    (0 until 30).foreach(h => k.update(h.toLong * hour, 100.0 + h * 1.0))
    val (dk, dd, _) = k.kdjValues
    assertEquals(k.kdjDirection, 1)
    assert(dk > dd, s"K=$dk D=$dd")
    assert(dk > 50.0 && dk <= 100.0, s"K=$dk")

  test("持续下跌 -> K 低且 K<D (看空 -1)"):
    val k = new KlineSeries(hour, 200) with Kdj
    (0 until 30).foreach(h => k.update(h.toLong * hour, 1000.0 - h * 1.0))
    val (dk, dd, _) = k.kdjValues
    assertEquals(k.kdjDirection, -1)
    assert(dk < dd, s"K=$dk D=$dd")
    assert(dk >= 0.0 && dk < 50.0, s"K=$dk")

  test("多指标同时 mix-in: with Macd with Kdj 各自独立可用"):
    val k = new KlineSeries(hour, 200) with Macd with Kdj
    (0 until 80).foreach(h => k.update(h.toLong * hour, 100.0 + h * 1.0))
    // 上涨趋势：两指标都应看多
    assertEquals(k.macdDirection, 1)
    assertEquals(k.kdjDirection, 1)
    assert(k.macdHistogram > 0)
    assert(k.kdjValues._1 > k.kdjValues._2)
