package hft.strategy.edge

/** 均线分界的不对称带：以 MA(默认 MA20) 为界，只调制**卖单侧 (上带)**——
  *   - 均线上 (maBias>0)：卖单距离更大 (上带 ×(1+skew))，不急于卖、让多头 delta 顺势多跑；
  *   - 均线下 (maBias<0)：反之，卖单距离更小 (上带 ×(1−skew))，趋势向下时快速对冲掉多头 delta。
  * 买单侧 (下带) 保持基线 atrMult×ATR 不变 (用户规则只约束卖单距离)。maBias 未就绪 (均线预热中)
  * 时退化为对称基线。[[skew]]∈(0,1) 控制强度。 */
final class MaSideBand(
    atrMult: Double = 2.0,
    skew: Double = 0.5,
) extends HedgeBand:
  def bands(ctx: HedgeCtx): (Double, Double) =
    val base = atrMult * ctx.atr
    val up = base * (1.0 + skew * ctx.maBias) // maBias=+1 -> 1+skew; -1 -> 1-skew; 0 -> 1
    (up, base)
