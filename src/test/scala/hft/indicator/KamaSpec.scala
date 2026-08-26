package hft.indicator

/** KAMA 单测：预热前 None；单调趋势中快速贴近收盘 (ER→1)；来回震荡中几乎不动 (ER→0)；
  * 盘中动态值与它收盘后定下的值一致 (同一份公式)。 */
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

  // ---------- 收盘固定 + 盘中动态 ----------

  test("盘中根立刻反映到 kama, 不必等收盘 (拿它做对冲判据时那段冻结就是裸敞口)"):
    val s = new KlineSeries(1000L, 1000) with Kama:
      override protected def kamaErPeriod: Int = 3
    // 12 根已收盘 bar 稳在 100 (ts 每根 +1000)
    (0 to 12).foreach(i => s.update(i * 1000L, 100.0))
    val beforeSpike = s.kama.get
    // **同一根 bar 内**跳到 200
    s.update(12 * 1000L + 500, 200.0)
    val afterSpike = s.kama.get
    assert(afterSpike > beforeSpike, s"盘中跳空应立刻推动 kama: $beforeSpike -> $afterSpike")
    assertEquals(s.kamaAtClose.get, beforeSpike, "已收盘值不该被盘中变动改写")

  test("盘中试算 == 该值真正收盘后的结果 (同一份公式, 不是两套算术)"):
    def series(erPeriod: Int) = new KlineSeries(1000L, 1000) with Kama:
      override protected def kamaErPeriod: Int = erPeriod
    val live = series(3)
    val closed = series(3)
    val hist = Seq(100.0, 101.0, 103.0, 102.0, 105.0, 108.0, 107.0, 110.0)
    hist.zipWithIndex.foreach { case (v, i) =>
      live.update(i * 1000L, v); closed.update(i * 1000L, v)
    }
    // live: 最后一根**盘中**收在 120; closed: 让 120 那根真正收盘
    live.update(hist.size * 1000L, 120.0)
    closed.update(hist.size * 1000L, 120.0)
    closed.update((hist.size + 1) * 1000L, 120.0) // 跨周期 -> 120 那根收盘
    assertEquals(live.kama.get, closed.kamaAtClose.get, "盘中试算与收盘落地必须是同一个数")
    assertEquals(live.efficiencyRatio.get, closed.efficiencyRatioAtClose.get)

  test("盘中 ER 对跳空敏感 (净位移变大 -> 判为单边)"):
    val s = new KlineSeries(1000L, 1000) with Kama:
      override protected def kamaErPeriod: Int = 3
    // 来回折返 -> ER 低
    (0 to 12).foreach(i => s.update(i * 1000L, if i % 2 == 0 then 99.0 else 101.0))
    val chopEr = s.efficiencyRatio.get
    // 同一根 bar 内跳空 -> 净位移远大于路径 -> ER 升高
    s.update(12 * 1000L + 500, 200.0)
    val spikeEr = s.efficiencyRatio.get
    assert(spikeEr > chopEr, s"跳空应抬高 ER: $chopEr -> $spikeEr")
    assert(spikeEr > 0.8, s"单边特征明显时 ER 应接近 1, 实为 $spikeEr")

  test("盘中值比收盘值早一根就绪 (试算窗口多算上盘中那个点)"):
    val s = new KlineSeries(1000L, 1000) with Kama:
      override protected def kamaErPeriod: Int = 3
    // 4 根已收盘 -> 收盘窗口刚好 4 个点 = erPeriod+1 -> 两者都就绪
    (0 to 4).foreach(i => s.update(i * 1000L, i.toDouble))
    assert(s.kama.nonEmpty && s.kamaAtClose.nonEmpty)
    // 只喂 3 根已收盘: 收盘窗口 3 个点不足, 试算窗口 4 个点够
    val t = new KlineSeries(1000L, 1000) with Kama:
      override protected def kamaErPeriod: Int = 3
    (0 to 3).foreach(i => t.update(i * 1000L, i.toDouble))
    assertEquals(t.kamaAtClose, None, "已收盘窗口不足")
    assert(t.kama.nonEmpty, "试算窗口够 -> 盘中值早一根就绪")
