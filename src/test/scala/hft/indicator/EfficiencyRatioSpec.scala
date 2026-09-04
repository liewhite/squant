package hft.indicator

/** 效率比 ER 单测：预热不足 -> None；单调趋势→≈1、来回震荡→≈0；盘中值与它收盘后定下的值
  * 出自同一份公式，且比收盘值早一根就绪。
  *
  * 这些用例原先寄居在 `KamaSpec` 里 —— ER 当初是 KAMA 的一个内部读数。现在 KAMA 那条均线
  * 没有消费者、已删除，而 ER 有 (`DeltaHedgeStrategy` 拿它选报价方式)，所以判据搬到这里。
  */
class EfficiencyRatioSpec extends munit.FunSuite:

  /** 每根 bar 一个值 (bar 间隔取 1，确保每次 update 都开新 bar 并收盘上一根)。 */
  private def feed(values: Seq[Double], period: Int = 10): KlineSeries & EfficiencyRatio =
    val s = new KlineSeries(1L, 1000) with EfficiencyRatio:
      override protected def erPeriod: Int = period
    values.zipWithIndex.foreach { case (v, i) => s.update(i.toLong, v) }
    s

  private def series(period: Int): KlineSeries & EfficiencyRatio =
    new KlineSeries(1000L, 1000) with EfficiencyRatio:
      override protected def erPeriod: Int = period

  test("效率比 ER: 趋势→≈1, 震荡→≈0, 预热不足→None"):
    assertEquals(feed(Seq(1.0, 2.0, 3.0)).efficiencyRatio, None) // 预热不足
    val up = feed((0 to 40).map(_.toDouble))
    assert(clue(up.efficiencyRatio.get) > 0.95, "单调趋势 ER 应≈1")
    val osc = feed((0 until 60).map(i => if i % 2 == 0 then 99.0 else 101.0))
    assert(clue(osc.efficiencyRatio.get) < 0.2, "来回震荡 ER 应≈0")

  test("盘中 ER 对跳空敏感 (净位移变大 -> 判为单边)"):
    val s = series(3)
    // 来回折返 -> ER 低
    (0 to 12).foreach(i => s.update(i * 1000L, if i % 2 == 0 then 99.0 else 101.0))
    val chopEr = s.efficiencyRatio.get
    // 同一根 bar 内跳空 -> 净位移远大于路径 -> ER 升高
    s.update(12 * 1000L + 500, 200.0)
    val spikeEr = s.efficiencyRatio.get
    assert(spikeEr > chopEr, s"跳空应抬高 ER: $chopEr -> $spikeEr")
    assert(spikeEr > 0.8, s"单边特征明显时 ER 应接近 1, 实为 $spikeEr")

  test("盘中值比收盘值早一根就绪 (试算窗口多算上盘中那个点)"):
    val s = series(3)
    // 4 根已收盘 -> 收盘窗口刚好 4 个点 = erPeriod+1 -> 两者都就绪
    (0 to 4).foreach(i => s.update(i * 1000L, i.toDouble))
    assert(s.efficiencyRatio.nonEmpty && s.efficiencyRatioAtClose.nonEmpty)
    // 只喂 3 根已收盘: 收盘窗口 3 个点不足, 试算窗口 4 个点够
    val t = series(3)
    (0 to 3).foreach(i => t.update(i * 1000L, i.toDouble))
    assertEquals(t.efficiencyRatioAtClose, None, "已收盘窗口不足")
    assert(t.efficiencyRatio.nonEmpty, "试算窗口够 -> 盘中值早一根就绪")
