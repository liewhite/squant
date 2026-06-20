package hft.indicator

/** KAMA 单测：预热前 None；单调趋势中快速贴近收盘 (ER→1)；来回震荡中几乎不动 (ER→0)。 */
class KamaSpec extends munit.FunSuite:

  /** 每根 bar 一个值 (用足够大的 bar 间隔确保每次 update 跨周期收盘上一根)。 */
  private def feed(values: Seq[Double], erPeriod: Int = 10): KlineSeries & Kama =
    val s = new KlineSeries(1L, 1000) with Kama:
      override protected def kamaErPeriod: Int = erPeriod
    // ts = i 递增、bar 间隔=1 -> 每个新值都开新 bar 并收盘上一根
    values.zipWithIndex.foreach { case (v, i) => s.update(i.toLong, v) }
    s

  test("预热不足 (已收盘 bar < erPeriod+1) -> None"):
    // 喂 erPeriod 个值后只有 erPeriod-1 根已收盘 (末根仍盘中) -> 不足
    val s = feed((1 to 5).map(_.toDouble), erPeriod = 10)
    assertEquals(s.kama, None)

  test("单调趋势: ER→1, KAMA 接近快线、贴近最新收盘"):
    // 0,1,2,...,40 单调上升: 净位移=路径长度 -> ER=1 -> SC=fastSC²=(2/3)²≈0.444
    val s = feed((0 to 40).map(_.toDouble), erPeriod = 10)
    val k = s.kama.get
    // 最近一根已收盘 close = 39 (末值 40 仍盘中)；快速跟随应与之相差很小
    assert(clue(math.abs(k - 39.0)) < 3.0, s"趋势中 KAMA=$k 应贴近收盘 39")

  test("来回震荡: ER→0, KAMA 几乎不动、停在中枢附近"):
    // 在 100±1 来回: 净位移≈0、路径长 -> ER≈0 -> SC=slowSC²=(2/31)²≈0.0042 -> 基本不动
    val osc = (0 until 60).map(i => if i % 2 == 0 then 99.0 else 101.0)
    val s = feed(osc, erPeriod = 10)
    val k = s.kama.get
    assert(clue(math.abs(k - 100.0)) < 1.0, s"震荡中 KAMA=$k 应停在中枢 100 附近")

  test("负值/跨零序列: KAMA 仍有界且按 ER 自适应 (netDelta 实际常为负)"):
    // 单调下行到负: 0,-1,...,-40 -> ER=1 -> 贴近最新已收盘 -39
    val down = feed((0 to 40).map(-_.toDouble))
    assert(clue(math.abs(down.kama.get - (-39.0))) < 3.0, "负向趋势 KAMA 应贴近 -39")
    // 跨零来回: -1,+1,-1,... -> ER≈0、慢线分量主导 -> 始终有界于输入域 [-1,1] (凸组合不发散, 即修复的崩溃点)
    val osc = feed((0 until 60).map(i => if i % 2 == 0 then -1.0 else 1.0))
    assert(clue(math.abs(osc.kama.get)) <= 1.0, "跨零震荡 KAMA 应有界于 [-1,1], 不发散")

  test("效率比 ER: 趋势→≈1, 震荡→≈0, 预热不足→None"):
    assertEquals(feed(Seq(1.0, 2.0, 3.0)).efficiencyRatio, None) // 预热不足
    val up = feed((0 to 40).map(_.toDouble))
    assert(clue(up.efficiencyRatio.get) > 0.95, "单调趋势 ER 应≈1")
    val osc = feed((0 until 60).map(i => if i % 2 == 0 then 99.0 else 101.0))
    assert(clue(osc.efficiencyRatio.get) < 0.2, "来回震荡 ER 应≈0")

  test("趋势的 KAMA 步进幅度 >> 震荡 (自适应区分度)"):
    val up = feed((0 to 40).map(_.toDouble))
    val osc = feed((0 until 60).map(i => if i % 2 == 0 then 99.0 else 101.0))
    // 趋势中 KAMA 已远离起点基线 0；震荡中仍贴近 100 起点
    assert(up.kama.get > 30.0, "趋势 KAMA 应大幅推进")
    assert(math.abs(osc.kama.get - 100.0) < 1.0, "震荡 KAMA 应基本停滞")
