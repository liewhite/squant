package strategy.bandhedge.logic

import strategy.utils.hedge.{HedgeBand, HedgeCtx}

/** 波动体制自适应带 (对称)：基宽 = [[atrMult]]×ATR，再乘 [[HedgeBand.regimeFactor]]——
  * 近端波动放大 (volRatio>1) 时收窄、更频繁对冲以更细兑现实现方差；平静时放宽、减少摩擦。
  * 因子 clamp 到 [[minFactor]]..[[maxFactor]]。对应需求"最近窗口 RV 放大时同步收紧对冲阈值"。 */
final class VolRegimeBand(
    atrMult: Double = 2.0,
    minFactor: Double = 0.5,
    maxFactor: Double = 2.0,
) extends HedgeBand:
  def bands(ctx: HedgeCtx): (Double, Double) =
    val b = atrMult * ctx.atr * HedgeBand.regimeFactor(ctx.volRatio, minFactor, maxFactor)
    (b, b)
