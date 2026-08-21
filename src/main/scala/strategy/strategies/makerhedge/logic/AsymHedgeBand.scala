package strategy.strategies.makerhedge.logic
import strategy.utils.hedge.{HedgeBand, HedgeCtx}

/** 顺势 / 逆势两侧不对称的对冲带——趋势方向由注入的 [[trendDir]] 决定 (返回 ≥0 视为顺势=上行侧):
  *   - 顺势侧带宽 = [[trendSideMult]]×ATR、逆势侧带宽 = [[counterTrendMult]]×ATR;
  *   - trendDir<0 (顺势=下行) 时上下镜像。
  *
  * "用什么信号判趋势"是唯一可变轴 (均线位置 / MACD 柱斜率 / …)，故抽象为 `HedgeCtx => Int` 注入,
  * 各信号只是 [[trendDir]] 取值不同 —— 真实抽象, 无 `if maBias / if macdBias` 分发 (开放封闭)。
  *
  * 两种用法 (谁紧谁松取决于策略意图)：
  *   - **卖方** (空头跨式, 负 gamma)：顺势侧**紧** (trend=1ATR)、逆势侧松 (counter=2ATR) ——
  *     危险的趋势方向严控敞口、对小幅逆势回撤容忍, 减少负 gamma 追价。
  *   - **买方** (多头跨式, 正 gamma)：顺势侧**松** (trend=2ATR)、逆势侧紧 (counter=1ATR) ——
  *     让顺势盈利的 delta 多跑 (晚对冲), 逆势快速对冲锁定、重置 gamma。
  */
final class AsymHedgeBand(
    trendSideMult: Double,
    counterTrendMult: Double,
    trendDir: HedgeCtx => Int,
) extends HedgeBand:
  def bands(ctx: HedgeCtx): (Double, Double) =
    val trend = trendSideMult * ctx.atr
    val counter = counterTrendMult * ctx.atr
    if trendDir(ctx) >= 0 then (trend, counter) // 顺势=上行: 上带=顺势侧
    else (counter, trend)                       // 顺势=下行: 上下镜像

object AsymHedgeBand:
  /** 趋势由 **1h MACD 柱斜率**决定: 柱较前一根上升 (macdHistDir≥0) → 顺势=上行侧。
    * 卖方语义: 柱升 → 上带=trend(紧)、下带=counter(松); 柱降镜像。 */
  def byMacdBar(trendSideMult: Double, counterTrendMult: Double): AsymHedgeBand =
    AsymHedgeBand(trendSideMult, counterTrendMult, _.macdHistDir)

  /** 趋势由**价相对均线**决定: 均线上 (maBias≥0) → 顺势=上行侧。 */
  def byMa(trendSideMult: Double, counterTrendMult: Double): AsymHedgeBand =
    AsymHedgeBand(trendSideMult, counterTrendMult, _.maBias)

  /** **对称带** (上下均 widthMult×ATR, 无任何方向性) —— 纯 delta 阈值对冲基准。
    * 上=下 故 trendDir 取值无关 (恒走"顺势"分支即得对称); iv=rv 时理论上应接近**盈亏平衡** (公平定价的
    * 对称 delta 对冲, 期权腿被对冲腿对冲, 期望≈0), 用作校验回测口径正确性的 sanity baseline。 */
  def symmetric(widthMult: Double): AsymHedgeBand =
    AsymHedgeBand(widthMult, widthMult, _ => 0)
