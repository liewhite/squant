package hft.indicator

/** ATR 单测：预热前 None；满期后给出 Wilder 平滑值 (对照手算参考)。
  *
  * 单值喂入 (high=low=close=price)，故 TR_t = |close_t − close_{t-1}|，便于手算验证。
  */
class AtrSpec extends munit.FunSuite:

  private def feed(values: Seq[Double], period: Int): KlineSeries & Atr =
    val s = new KlineSeries(1L, values.size + 2) with Atr:
      override protected def atrPeriod: Int = period
    values.zipWithIndex.foreach { case (v, i) => s.update(i.toLong, v) }
    s

  test("预热不足 (已收盘 TR < atrPeriod) -> None"):
    // 喂 4 个值 -> 收盘 3 根 -> 仅 2 个 TR (首根无前收)，period=3 不足
    val s = feed(Seq(100.0, 110.0, 105.0, 115.0), period = 3)
    assertEquals(s.atr, None)

  test("满期后 Wilder 平滑 (对照手算)"):
    // 收盘 100,110,105,115,120,130 (末值 125 盘中); TR=10,5,10,5,10
    // seed=(10+5+10)/3=8.3333; +5 -> (8.3333*2+5)/3=7.2222; +10 -> (7.2222*2+10)/3=8.14815
    val s = feed(Seq(100.0, 110.0, 105.0, 115.0, 120.0, 130.0, 125.0), period = 3)
    val got = s.atr.getOrElse(fail("atr should be ready"))
    assert(math.abs(got - 8.14815) < 1e-4, s"expected≈8.14815 got $got")

  test("恒定价 -> ATR=0"):
    val s = feed(Seq.fill(10)(50.0), period = 3)
    assertEquals(s.atr, Some(0.0))
