package strategy.research

import strategy.live.{HedgeBand, HedgeCtx}

/** 方向不对称带：基宽 = [[atrMult]]×ATR，按 MACD 方向 [[HedgeBand.skewOf]] 上下不对称——
  * 偏多时上带放宽 (顺势的多头 delta 多跑一会、兑现趋势)、下带收窄 (逆势快速对冲)，偏空镜像。
  * [[skew]]∈(0,1) 控制不对称强度 (0=退化为对称，建议 ≤0.8 保证两侧带宽恒正)。
  * 把趋势 alpha 叠加到 gamma scalping 上，是"用均线/MACD 给方向、让两边挂单距离不对称"的实现。 */
final class DirectionalBand(
    atrMult: Double = 2.0,
    skew: Double = 0.5,
) extends HedgeBand:
  def bands(ctx: HedgeCtx): (Double, Double) =
    val b = atrMult * ctx.atr
    val s = HedgeBand.skewOf(ctx.macdBias, skew)
    (b * (1.0 + s), b * (1.0 - s))
