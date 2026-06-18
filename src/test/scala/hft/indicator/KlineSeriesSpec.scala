package hft.indicator

/** K 线工具单测：分桶 OHLC、跨周期固定上一根、盘中动态根、MACD 预热与方向、缓存上限。 */
class KlineSeriesSpec extends munit.FunSuite:
  private val hour = 3_600_000L

  test("同周期内刷新盘中根 OHLC, 不收盘"):
    val k = KlineSeries(hour, maxBars = 100)
    k.update(0, 100.0, 1.0)
    k.update(1000, 105.0, 2.0)
    k.update(2000, 95.0, 1.0)
    assertEquals(k.closedBars, 0) // 未跨周期
    val cur = k.current.get
    assertEquals(cur.open, 100.0)
    assertEquals(cur.high, 105.0)
    assertEquals(cur.low, 95.0)
    assertEquals(cur.close, 95.0)
    assertEquals(cur.volume, 4.0)
    assert(!cur.closed)

  test("跨周期固定上一根 (closed=true) 并开新盘中根"):
    val k = KlineSeries(hour, maxBars = 100)
    k.update(0, 100.0, 1.0)
    k.update(hour / 2, 110.0, 1.0)
    k.update(hour + 1, 120.0, 1.0) // 跨入第 2 小时
    assertEquals(k.closedBars, 1)
    val closed = k.bars.last
    assert(closed.closed)
    assertEquals(closed.close, 110.0) // 第 1 根收盘 = 跨周期前最后一笔
    assertEquals(k.current.get.open, 120.0)

  test("缓存上限 maxBars: 超出丢弃最旧"):
    val k = KlineSeries(hour, maxBars = 3)
    (0 to 9).foreach(h => k.update(h.toLong * hour, 100.0 + h, 1.0))
    assertEquals(k.bars.size, 3) // 仅保留最近 3 根已收盘 (第 10 根盘中)
    assertEquals(k.bars.head.openTime / hour, 6L) // 最旧为第 7 根 (index6)

  test("MACD 预热不足 -> 方向 0; 持续上涨 -> 看多; 持续下跌 -> 看空"):
    val up = KlineSeries(hour, 200)
    (0 until 30).foreach(h => { up.update(h.toLong * hour, 100.0 + h); up.update(h.toLong * hour + hour - 1, 100.0 + h) })
    assertEquals(up.macdDirection, 0) // 仅 29 根已收盘 < 35

    val up2 = KlineSeries(hour, 200)
    (0 until 80).foreach(h => up2.update(h.toLong * hour, 100.0 + h * 1.0))
    assertEquals(up2.macdDirection, 1)
    assert(up2.macdHistogram > 0)

    val down = KlineSeries(hour, 200)
    (0 until 80).foreach(h => down.update(h.toLong * hour, 1000.0 - h * 1.0))
    assertEquals(down.macdDirection, -1)
    assert(down.macdHistogram < 0)
