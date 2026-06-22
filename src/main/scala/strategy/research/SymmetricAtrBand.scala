package strategy.research

import strategy.live.{HedgeBand, HedgeCtx}

/** 基线对冲带：上下对称、宽度恒为 [[atrMult]]×ATR (复刻 [[hft.strategy.AtrTakeHedgeStrategy]] 的触发)。
  * 作为各 edge 变体的对照组——edge 必须跑赢它才算真有优势。 */
final class SymmetricAtrBand(atrMult: Double = 2.0) extends HedgeBand:
  def bands(ctx: HedgeCtx): (Double, Double) =
    val b = atrMult * ctx.atr
    (b, b)
