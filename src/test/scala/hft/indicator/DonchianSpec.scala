package hft.indicator

/** Donchian 通道单测：预热前 None；满窗后给出近 N 根已收盘 bar 的高/低；滚动剔除旧 bar。 */
class DonchianSpec extends munit.FunSuite:

  /** 每个值一根 bar (间隔=1 -> 每次 update 收盘上一根)，donchianBars 可调。 */
  private def feed(values: Seq[Double], n: Int): KlineSeries & Donchian =
    val s = new KlineSeries(1L, n) with Donchian:
      override protected def donchianBars: Int = n
    values.zipWithIndex.foreach { case (v, i) => s.update(i.toLong, v) }
    s

  test("预热不足 (已收盘 bar < donchianBars) -> None"):
    val s = feed(Seq(1.0, 2.0, 3.0), n = 5) // 仅 2 根已收盘
    assertEquals(s.windowHigh, None)
    assertEquals(s.windowLow, None)

  test("满窗后给出窗口高/低 (仅已收盘 bar, 不含盘中根)"):
    // 喂 1..6: 收盘 1,2,3,4,5; 第 6 仍盘中。窗口 n=5 -> 高=5 低=1
    val s = feed((1 to 6).map(_.toDouble), n = 5)
    assertEquals(s.windowHigh, Some(5.0))
    assertEquals(s.windowLow, Some(1.0))

  test("滚动: 旧极值移出窗口后不再计入"):
    // n=3, 喂 10,1,2,3,4,5(盘中) -> 已收盘 10,1,2,3,4; 近 3 根=2,3,4 -> 高4 低2 (10 已移出)
    val s = feed(Seq(10.0, 1.0, 2.0, 3.0, 4.0, 5.0), n = 3)
    assertEquals(s.windowHigh, Some(4.0))
    assertEquals(s.windowLow, Some(2.0))
