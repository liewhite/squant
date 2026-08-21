package strategy.strategies.makerhedge.logic

import strategy.utils.hedge.HedgeCtx

/** AsymHedgeBand (顺势/逆势不对称带, 方向信号注入) 纯函数单测：byMa / byMacdBar 两种趋势信号、买卖双向取值、镜像。 */
class AsymHedgeBandSpec extends munit.FunSuite:
  private def ctx(maBias: Int = 0, macdHistDir: Int = 0, atr: Double = 1.0): HedgeCtx =
    HedgeCtx(px = 100.0, center = 100.0, atr = atr, volRatio = 1.0, macdBias = 0, maBias = maBias, macdHistDir = macdHistDir)

  private def near(a: Double, b: Double): Unit = assert(math.abs(a - b) < 1e-9, s"expected $b got $a")
  private def bandsNear(got: (Double, Double), up: Double, down: Double): Unit =
    near(got._1, up); near(got._2, down)

  test("byMa 卖方(顺势紧逆势松): 均线上 上紧下松, 均线下 上松下紧"):
    val b = AsymHedgeBand.byMa(trendSideMult = 1.0, counterTrendMult = 2.0)
    bandsNear(b.bands(ctx(maBias = 1, atr = 1.0)), 1.0, 2.0)   // 均线上: 顺势(上)紧1ATR、逆势(下)松2ATR
    bandsNear(b.bands(ctx(maBias = 0, atr = 1.0)), 1.0, 2.0)   // 未就绪(>=0)同均线上
    bandsNear(b.bands(ctx(maBias = -1, atr = 2.0)), 4.0, 2.0)  // 均线下: 逆势(上)松2*2、顺势(下)紧1*2

  test("byMa 买方(顺势松逆势紧): 均线上 上松下紧, 均线下 上紧下松"):
    val b = AsymHedgeBand.byMa(trendSideMult = 2.0, counterTrendMult = 1.0)
    bandsNear(b.bands(ctx(maBias = 1, atr = 1.0)), 2.0, 1.0)   // 均线上: 顺势(正delta,上)松2ATR、逆势(负delta,下)紧1ATR
    bandsNear(b.bands(ctx(maBias = -1, atr = 1.0)), 1.0, 2.0)  // 均线下镜像

  test("symmetric: 上下恒为 widthMult×ATR, 无方向 (任何信号取值都对称)"):
    val b = AsymHedgeBand.symmetric(1.0)
    bandsNear(b.bands(ctx(maBias = 1, macdHistDir = 1, atr = 1.5)), 1.5, 1.5)   // 看多信号下仍对称
    bandsNear(b.bands(ctx(maBias = -1, macdHistDir = -1, atr = 2.0)), 2.0, 2.0) // 看空信号下仍对称

  test("byMacdBar 卖方: 1h MACD 柱升→上带紧(负delta 1ATR对冲), 柱降→镜像"):
    val b = AsymHedgeBand.byMacdBar(trendSideMult = 1.0, counterTrendMult = 2.0)
    bandsNear(b.bands(ctx(macdHistDir = 1, atr = 1.0)), 1.0, 2.0)   // 柱升: 上带紧1ATR、下带松2ATR
    bandsNear(b.bands(ctx(macdHistDir = 0, atr = 1.0)), 1.0, 2.0)   // 持平/未就绪(>=0)同柱升
    bandsNear(b.bands(ctx(macdHistDir = -1, atr = 2.0)), 4.0, 2.0)  // 柱降镜像: 上带松2*2、下带紧1*2
