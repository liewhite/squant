package strategy.makerhedge.logic
import strategy.utils.hedge.{HedgeBand, HedgeCtx}

/** 均线分界、上下不对称的对冲带——专为**空头跨式 (卖方, 负 gamma)** 设计 (但对买方同样可用)。
  *
  * 空头跨式下：价格上行→净 delta 转负→对冲需**买 (多单)**；下行→净 delta 转正→对冲需**卖 (空单)**。
  * 规则 (来自需求):
  *   - **均线上** (maBias≥0, 预期续涨)：对**上涨**对冲更积极 (上带 = [[tightMult]]×ATR, 价格每涨 ~1ATR 即买单对冲),
  *     对**回落**更宽容 (下带 = [[looseMult]]×ATR, 回落 ~2ATR 才卖单对冲) —— 紧跟上涨、不追小回落。
  *   - **均线下** (maBias<0)：反之 (下带紧、上带松)。
  *
  * 越紧的一侧对冲越频繁 (负 gamma 成本越高但敞口控制越严)，体现"顺势方向严控、逆势方向容忍"的不对称。 */
final class MaAsymHedgeBand(
    tightMult: Double = 1.0,
    looseMult: Double = 2.0,
) extends HedgeBand:
  def bands(ctx: HedgeCtx): (Double, Double) =
    val tight = tightMult * ctx.atr
    val loose = looseMult * ctx.atr
    if ctx.maBias >= 0 then (tight, loose) // 均线上: 上紧(追涨)、下松(容忍回落)
    else (loose, tight)                    // 均线下: 上松、下紧(追跌)
