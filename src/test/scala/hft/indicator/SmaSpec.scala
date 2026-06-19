package hft.indicator

/** SMA 单测：预热前 None；满足周期后等于最近 N 根已收盘 close 的均值 (不含盘中根)。 */
class SmaSpec extends munit.FunSuite:

  /** 每根 bar 一个值 (bar 间隔=1ms，ts 递增确保每次 update 跨周期收盘上一根)。 */
  private def feed(values: Seq[Double], period: Int): KlineSeries & Sma =
    val s = new KlineSeries(1L, 1000) with Sma:
      override protected def smaPeriod: Int = period
    values.zipWithIndex.foreach { case (v, i) => s.update(i.toLong, v) }
    s

  test("预热不足 (已收盘 bar < period) -> None"):
    // 喂 period 个值 -> 只有 period-1 根已收盘 (末根盘中) -> 不足
    val s = feed((1 to 5).map(_.toDouble), period = 5)
    assertEquals(s.sma, None)

  test("满足周期后 = 最近 period 根已收盘 close 均值"):
    // 喂 1..7 (7 个值 -> 6 根已收盘: 1..6, 末值 7 盘中)。period=3 -> 最近 3 根已收盘 = 4,5,6 -> 均值 5
    val s = feed((1 to 7).map(_.toDouble), period = 3)
    assertEquals(s.sma, Some(5.0))

  test("滚动窗口只保留最近 period 根"):
    // 1..101 -> 100 根已收盘 (1..100)，period=60 -> 最近 60 根 = 41..100 -> 均值 70.5
    val s = feed((1 to 101).map(_.toDouble), period = 60)
    assertEquals(s.sma, Some((41 to 100).sum.toDouble / 60))

  test("常数序列 SMA = 常数"):
    val s = feed(Seq.fill(100)(42.0), period = 60)
    assertEquals(s.sma, Some(42.0))
