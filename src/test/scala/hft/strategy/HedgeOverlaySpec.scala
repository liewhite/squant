package hft.strategy

/** 方向 overlay 单测：
  *   - KamaTrendOverlay：预热前中性宽带 / 上升趋势顺势正目标且带收紧 / 下降趋势负目标 / 震荡目标≈0 宽带留敞口。
  *   - MacdBiasOverlay：带宽乘子恒 1 / tilt=0 恒中性 / 水上 tilt>0 正目标。
  */
class HedgeOverlaySpec extends munit.FunSuite:

  /** 逐根喂价 (默认 1min 间隔，每根都跨周期收盘上一根) */
  private def feed(o: HedgeOverlay, prices: Seq[Double], barMs: Long = 60_000L): Unit =
    prices.zipWithIndex.foreach { case (p, i) => o.update((i + 1).toLong * barMs, p, 1.0) }

  // ---- KamaTrendOverlay ----

  test("KamaTrend 预热不足: target=0, 带宽取 chop 乘子 (ER=0 -> 宽)"):
    val o = KamaTrendOverlay(barIntervalMs = 60_000L,bandChopMult = 2.0, bandTrendMult = 0.5)
    feed(o, Seq(100.0, 101.0, 102.0)) // 不足 erPeriod+1
    assertEquals(o.targetDelta(10.0), 0.0)
    assertEquals(o.bandMult, 2.0)

  test("KamaTrend 上升趋势: price>KAMA, dir=+1 -> target>0; ER高 -> 带收紧(<chop)"):
    val o = KamaTrendOverlay(barIntervalMs = 60_000L,tiltMoveRatio = 0.01, bandChopMult = 2.0, bandTrendMult = 0.5)
    feed(o, (0 to 40).map(100.0 + _)) // 单调升, ER≈1, 价在 KAMA 上方
    assert(clue(o.targetDelta(10.0)) > 0.0, "上升趋势顺势目标正 delta")
    assert(clue(o.bandMult) < 2.0, "趋势中带收紧、及时对冲")

  test("KamaTrend 下降趋势: price<KAMA, dir=-1 -> target<0"):
    val o = KamaTrendOverlay(barIntervalMs = 60_000L,tiltMoveRatio = 0.01)
    feed(o, (0 to 40).map(200.0 - _)) // 单调降
    assert(clue(o.targetDelta(10.0)) < 0.0, "下降趋势顺势目标负 delta")

  test("KamaTrend 盘中 tick 跌破 KAMA -> dir 立即翻负 (不等收盘, 锁定及时完全对冲)"):
    val o = KamaTrendOverlay(barIntervalMs = 60_000L, tiltMoveRatio = 0.01, dirDeadband = 0.003)
    feed(o, (0 to 40).map(100.0 + _)) // 升势暖机, KAMA 抬高、price 在其上方 -> dir=+1
    assert(o.targetDelta(10.0) > 0.0, "升势中目标正")
    // 单笔盘中 tick 大幅跌破 KAMA: dir 用最新 tick 价 vs 已收盘 KAMA -> 立即翻负 (不等 bar 收盘)
    o.update(42L * 60_000L, 100.0, 1.0)
    assert(clue(o.targetDelta(10.0)) < 0.0, "盘中跌破 KAMA 即翻负 -> 回落侧目标朝中性/转空")

  test("KamaTrend 震荡: ER≈0 -> target≈0 且带宽≈chop (宽, 留敞口)"):
    val o = KamaTrendOverlay(barIntervalMs = 60_000L,tiltMoveRatio = 0.01, bandChopMult = 2.0, bandTrendMult = 0.5)
    feed(o, (0 until 60).map(i => if i % 2 == 0 then 99.0 else 101.0))
    assert(clue(math.abs(o.targetDelta(10.0))) < 0.02, "震荡顺势幅度≈0 (ER≈0)")
    assert(clue(o.bandMult) > 1.5, "震荡带宽接近 chop (宽)")

  // ---- MacdBiasOverlay ----

  test("MacdBias: 带宽乘子恒 1 / tilt=0 恒中性"):
    val o = MacdBiasOverlay(tiltMoveRatio = 0.0)
    feed(o, (1 to 60).map(100.0 + _), barMs = 3_600_000L)
    assertEquals(o.bandMult, 1.0)
    assertEquals(o.targetDelta(10.0), 0.0)

  test("MacdBias: 上升趋势 + tilt>0 -> target>0"):
    val o = MacdBiasOverlay(tiltMoveRatio = 0.005)
    feed(o, (1 to 60).map(100.0 + _), barMs = 3_600_000L) // 60 根 1h 升 bar 暖机 MACD
    assert(clue(o.targetDelta(10.0)) > 0.0, "MACD 水上目标正 delta")
