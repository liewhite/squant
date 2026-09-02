package strategy.utils.hedge

import hft.domain.{BBO, Price, Side, TimeInForce}

/** 一次对冲报价的**方式** —— 挂在哪、用什么 TIF、给多久成交。
  *
  * 抽出来是因为"何时对冲"与"怎么对冲"是两个独立的轴：[[DeltaBand]] 回答前者，本类回答后者。
  * 从前后者被写死成一对构造参数（一个偏移 + 一个重挂间隔），于是"震荡时慢慢挂、单边时立刻吃"
  * 这种按体制切换的执行方式根本表达不出来。
  *
  * 每个实现**完整控制自己的三件事**（报价、TIF、超时），调用方不需要知道自己拿到的是哪一种 ——
  * 没有 `if crossing then ... else ...` 这样的分发。
  */
sealed trait QuoteStyle:
  /** 挂单的时间条件 */
  def tif: TimeInForce

  /** 给这张单多久成交；超过就撤掉重挂 */
  def ttlMs: Long

  /** **紧迫度** —— 越大越倾向立即成交。
    *
    * 用途只有一个：在簿上那张单还没到超时，但体制已经切换到更紧迫的方式时，要不要抢在超时之前
    * 换掉它。只允许"由缓到急"抢占（见 [[QuoteLeg.step]]）—— 反向抢占没有收益，还会让 ER 在
    * 阈值附近抖动时不停地撤单重挂。
    */
  def urgency: Int

  /** 这张单该挂什么价。
    *
    * 盘口是**必需**的，不是可选：两个调用方都是在拿到盘口之后才走到这里 (见
    * `MakerHedgeStrategy.manage` 与 `DeltaHedgeStrategy`)。从前签名是
    * `(bbo: Option[BBO], fallbackPx: Price)`，那条降级分支是给一个不存在的状态预留的防御 ——
    * 而"降级基准"本身还是从那条据说缺失的盘口算出来的中间价。 */
  def limitPrice(side: Side, bbo: BBO): Price

  /** 诊断用短名，进下单 comment 与日志 */
  def label: String

object QuoteStyle:
  /** **被动**：挂在对手价**之外** `offsetPct`，PostOnly，给 `ttlMs` 慢慢成交。
    *
    * 卖挂 `bestAsk·(1+off)`、买挂 `bestBid·(1−off)` —— 对手盘外侧，保证 PostOnly 不会被拒。
    * 用在敞口平缓（效率比 ER 低、行情来回折返）的时候：不急，省下 taker 费与价差。
    * 代价是可能一直不成交，所以 `ttlMs` 给得长（分钟级）反而合理 —— 频繁重挂只是换个价再等。
    */
  def passive(offsetPct: Double, ttlMs: Long): QuoteStyle = Passive(offsetPct, ttlMs)

  /** **跨价**：穿过盘口 `offsetPct`，GTC 限价，只给 `ttlMs`（秒级）。
    *
    * 卖挂 `bestBid·(1−off)`、买挂 `bestAsk·(1+off)` —— 越过对手价，到达即成交（成交价是盘口的
    * 最优价，不是这个挂单价；`offsetPct` 只是**穿透余量**，容忍下单到到达之间盘口移动）。
    * 用在敞口单边（效率比高、行情走出方向）的时候：负 gamma 组合最怕的就是"该对冲时没冲上"，
    * 这时手续费与价差远比裸敞口便宜。
    *
    * 用限价而不是市价，是为了给出**最差成交价的上限** —— 盘口瞬间抽空时市价单会吃穿多档。
    */
  def crossing(offsetPct: Double, ttlMs: Long): QuoteStyle = Crossing(offsetPct, ttlMs)

  private final case class Passive(offsetPct: Double, ttlMs: Long) extends QuoteStyle:
    require(offsetPct >= 0.0, s"被动挂单偏移须 >= 0, 实为 $offsetPct")
    require(ttlMs > 0, s"挂单存活时间须 > 0ms, 实为 $ttlMs")
    def tif: TimeInForce = TimeInForce.PostOnly
    def urgency: Int = 0
    def label: String = f"被动+${offsetPct * 100}%.3f%%/${ttlMs}ms"
    def limitPrice(side: Side, bbo: BBO): Price =
      side match // 对手价**外**移
        case Side.Short => bbo.askPrice.scaled(1.0 + offsetPct)
        case Side.Long  => bbo.bidPrice.scaled(1.0 - offsetPct)

  private final case class Crossing(offsetPct: Double, ttlMs: Long) extends QuoteStyle:
    require(offsetPct >= 0.0, s"跨价穿透余量须 >= 0, 实为 $offsetPct")
    require(ttlMs > 0, s"挂单存活时间须 > 0ms, 实为 $ttlMs")
    def tif: TimeInForce = TimeInForce.GTC // PostOnly 会被拒 —— 跨价的整个用意就是要吃单
    def urgency: Int = 1
    def label: String = f"跨价-${offsetPct * 100}%.3f%%/${ttlMs}ms"
    def limitPrice(side: Side, bbo: BBO): Price =
      side match // 穿过对手价
        case Side.Short => bbo.bidPrice.scaled(1.0 - offsetPct)
        case Side.Long  => bbo.askPrice.scaled(1.0 + offsetPct)
