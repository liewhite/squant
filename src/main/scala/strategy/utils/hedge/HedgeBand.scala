package strategy.utils.hedge

/** 一次对冲决策的上下文 (纯数据)：当前价、对冲中心、就绪的 ATR，以及供"找 edge"的体制/方向信号。
  *
  * 信号字段在指标未预热时由策略以**中性默认**填充 (volRatio=1.0、macdBias=0)，使各 [[HedgeBand]]
  * 在预热期自然退化为对称基线，避免半成品信号污染。**每个 band 实现只取其所需的信号字段** (如
  * AsymHedgeBand.byMa 用 maBias、byMacdBar 用 macdHistDir)，互不耦合。
  *
  * @param px        最新中间价
  * @param center    对冲中心 (上次成交价/起点)
  * @param atr       已就绪的 ATR(>0)
  * @param volRatio  近端/基线 实现波动比 (>1 波动放大、<1 平静；未就绪=1.0)
  * @param macdBias  MACD 柱方向强度 ∈ {-2,-1,0,1,2} (>0 偏多、<0 偏空；未就绪=0)
  * @param maBias    价相对均线的位置 = sign(px − MA) ∈ {-1,0,1} (>0 均线上、<0 均线下；未就绪=0)
  * @param macdHistDir MACD 柱斜率 = sign(柱[t] − 柱[t-1]) ∈ {-1,0,1} (柱较前一根**升**=+1、**降**=-1；未就绪/持平=0)
  */
final case class HedgeCtx(
    px: Double,
    center: Double,
    atr: Double,
    volRatio: Double,
    macdBias: Int,
    maBias: Int,
    macdHistDir: Int = 0,
)

/** **对冲带策略** —— 把"何时对冲"抽象为上/下行两侧的价格带宽 (绝对价距，均 >0)：
    *   - `px − center > upBand`   价格上行越带 -> 需对冲 (净多则卖)
    *   - `center − px > downBand` 价格下行越带 -> 需对冲 (净空则买)
    *
    * 这是寻找买方 edge 的**唯一可调旋钮**：期权腿盈亏由 IV/路径/到期固定，策略只能通过对冲腿的
    * 触发时机与上下不对称来改变总盈亏。各实现是纯函数 (ctx -> 带宽)，便于独立单测；新 edge 思路
    * 以新增 [[HedgeBand]] 实现表达，不改既有代码 (开放封闭)。
    */
trait HedgeBand:
  def bands(ctx: HedgeCtx): (Double, Double)
