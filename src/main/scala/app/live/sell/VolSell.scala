package app.live.sell
import app.live.option.*

/** 卖方决策的**可测试编排** (取数 + 定量 + 选腿)，与调度/IO 分离。plan 只读交易所、不下单; execute 才下单。 */
object VolSell:
  final case class Config(
      symbol: String = "ETHUSDT",
      baseCoin: String = "ETH",
      targetDays: Int = 21,
      gridHigh: Double = 2.0,
      gridLow: Double = 1.0,
      baseQty: Double = 1.0,
      bars2w: Int = 2 * 7 * 24 * 12,
      maxQty: Double = Double.MaxValue, // 单腿张数硬上限 (sanity, 防 scale bug 误下巨单)
  )

  /** 一条待下单腿 (PostOnly 卖)。price=None 表示无报价 (plan 不会产出 None 腿, 见下) */
  final case class Leg(symbol: String, qty: Double, price: Double, orderLinkId: String, right: OptionRight)

  /** @param atmStrike        选中的 ATM 行权价
    * @param candidateStrikes 目标到期下**全部可选行权价** (升序, 供下单前列出对照, 印证选了最近 ATM) */
  final case class Decision(
      mult: Double, rvPrev: Double, rvThis: Double, spot: Double,
      expiryMs: Long, atmStrike: Double, candidateStrikes: Seq[Double], legs: Seq[Leg],
  )

  /** 取数+决策, 产出**完整两腿** (call+put) 或 Left。任一腿数量不合规/无卖一报价 -> 整体 Left
    * (绝不只下单腿, 避免裸方向敞口)。orderLinkId 由当周决策锚点派生 -> 幂等 (重启/重试不重复下单)。
    * 目标到期 = 决策周五 + targetDays (默认 21 天后那个周五的周度期权), 在该到期内取最近 ATM 跨式。 */
  def plan(ex: OptionsExchange, cfg: Config, nowMs: Long): Either[String, Decision] =
    for
      closes <- ex.underlyingCloses5m(cfg.symbol, cfg.bars2w)
      _ <- Either.cond(closes.sizeIs >= cfg.bars2w * 9 / 10, (), s"K线不足 ${closes.size}/${cfg.bars2w} (<90%), 跳过避免RV失真")
      spot <- closes.lastOption.toRight("无现价")
      chain <- ex.optionChain(cfg.baseCoin)
      targetMs = SellVolPlan.targetExpiryMs(nowMs, cfg.targetDays)
      straddle <- SellVolPlan.selectStraddle(chain, nowMs, spot, targetMs).toRight("未找到 ~目标到期 ATM 跨式")
      (call, put) = straddle
      candidateStrikes = chain.iterator.filter(_.expiryMs == call.expiryMs).map(_.strike).distinct.toVector.sorted
      period = SellVolPlan.currentDecisionTime(nowMs)
      (mult, rvPrev, rvThis) = SellVolPlan.decideMultiplier(closes, cfg.gridHigh, cfg.gridLow)
      callLeg <- leg(ex, call, cfg.baseQty * mult, cfg.maxQty, period)
      putLeg <- leg(ex, put, cfg.baseQty * mult, cfg.maxQty, period)
    yield Decision(mult, rvPrev, rvThis, spot, call.expiryMs, call.strike, candidateStrikes, Seq(callLeg, putLeg))

  private def leg(ex: OptionsExchange, inst: OptionInstrument, rawQty: Double, maxQty: Double, periodMs: Long): Either[String, Leg] =
    for
      qty <- SellVolPlan.quantizeQty(rawQty, inst.qtyStep, inst.minQty)
        .toRight(s"${inst.symbol} 数量 $rawQty 不合规 (step=${inst.qtyStep} min=${inst.minQty})")
      _ <- Either.cond(qty <= maxQty, (), s"${inst.symbol} 数量 $qty 超硬上限 $maxQty -> 整体跳过 (疑似 scale bug)")
      askOpt <- ex.optionBestAsk(inst.symbol)
      ask <- askOpt.toRight(s"${inst.symbol} 无卖一报价, 跳过 (不市价砸)")
    yield
      val tag = if inst.right == OptionRight.Call then "c" else "p"
      Leg(inst.symbol, qty, ask, s"vs-$periodMs-$tag".take(36), inst.right)

  /** 下单 (PostOnly 卖在卖一)。返回每腿结果。 */
  def execute(ex: OptionsExchange, d: Decision): Seq[(Leg, Either[String, String])] =
    d.legs.map(l => l -> ex.sellOption(l.symbol, l.qty, Some(l.price), l.orderLinkId))
