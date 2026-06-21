package hft.strategy.edge

/** 复合带：同时叠加波动体制收放 ([[VolRegimeBand]]) 与方向不对称 ([[DirectionalBand]])——
  * 基宽 = [[atrMult]]×ATR×regimeFactor，再按 MACD 方向上下不对称。两个 edge 旋钮组合，
  * 既在波动放大时更细对冲、又顺势让利。复用 [[HedgeBand]] 的共享因子 (单一数据源、不重复实现)。 */
final class CompositeBand(
    atrMult: Double = 2.0,
    minFactor: Double = 0.5,
    maxFactor: Double = 2.0,
    skew: Double = 0.5,
) extends HedgeBand:
  def bands(ctx: HedgeCtx): (Double, Double) =
    val b = atrMult * ctx.atr * HedgeBand.regimeFactor(ctx.volRatio, minFactor, maxFactor)
    val s = HedgeBand.skewOf(ctx.macdBias, skew)
    (b * (1.0 + s), b * (1.0 - s))
