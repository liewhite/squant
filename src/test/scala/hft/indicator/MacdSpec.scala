package hft.indicator

/** MACD trait 单测 (混入 KlineSeries)：预热门控、单边趋势方向、盘中动态 = 整列收盘价参考实现。 */
class MacdSpec extends munit.FunSuite:
  private val hour = 3_600_000L
  private def series = new KlineSeries(hour, 200) with Macd

  test("预热不足 (< slow+signal 根已收盘) -> 方向 0"):
    val k = series
    // 30 次跨桶 -> 29 根已收盘 (最后一根盘中) < 35
    (0 until 30).foreach(h => k.update(h.toLong * hour, 100.0 + h))
    assertEquals(k.macdDirection, 0)

  test("持续上涨 -> 看多 (+1); 持续下跌 -> 看空 (-1)"):
    val up = series
    (0 until 80).foreach(h => up.update(h.toLong * hour, 100.0 + h * 1.0))
    assertEquals(up.macdDirection, 1)
    assert(up.macdHistogram > 0)

    val down = series
    (0 until 80).foreach(h => down.update(h.toLong * hour, 1000.0 - h * 1.0))
    assertEquals(down.macdDirection, -1)
    assert(down.macdHistogram < 0)

  test("盘中动态 MACD = 把(已收盘+盘中)收盘价整列跑 EMA 的参考实现 (freeze/dynamic 一致)"):
    val closes = (0 until 40).map(i => 100.0 + math.sin(i * 0.37) * 8 + i * 0.1)
    val k = series
    closes.zipWithIndex.foreach((c, i) => k.update(i.toLong * hour, c))

    // 参考：与 Macd 完全相同的 EMA 递推 (首根播种, signal@首根=0)，整列含盘中根
    var ef = 0.0; var es = 0.0; var esig = 0.0
    closes.zipWithIndex.foreach { (c, i) =>
      if i == 0 then { ef = c; es = c; esig = 0.0 }
      else
        val k2 = 2.0 / 13; ef = c * k2 + ef * (1 - k2)
        val k3 = 2.0 / 27; es = c * k3 + es * (1 - k3)
        val k4 = 2.0 / 10; esig = (ef - es) * k4 + esig * (1 - k4)
    }
    assert(math.abs(k.macdLine - (ef - es)) < 1e-9, s"line ${k.macdLine} vs ${ef - es}")
    assert(math.abs(k.macdSignalLine - esig) < 1e-9, s"signal ${k.macdSignalLine} vs $esig")
    assert(math.abs(k.macdHistogram - ((ef - es) - esig)) < 1e-9)
