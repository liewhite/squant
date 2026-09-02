package strategy.strategies.volsell.logic
import strategy.utils.option.*

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

  /** 一条待下单腿。price=卖价, postOnly=true 做 maker / false 走 taker(IOC), 由 [[SellVolPlan.sellQuote]] 按价差定。 */
  final case class Leg(symbol: String, qty: Double, price: Double, postOnly: Boolean, orderLinkId: String, right: OptionRight)

  /** @param callStrike       宽跨 call 行权 (>spot 最近)
    * @param putStrike        宽跨 put 行权 (<spot 最近)
    * @param candidateStrikes 目标到期下**全部可选行权价** (升序, 供下单前列出对照) */
  final case class Decision(
      mult: Double, rvPrev: Double, rvThis: Double, spot: Double,
      expiryMs: Long, callStrike: Double, putStrike: Double, candidateStrikes: Seq[Double], legs: Seq[Leg],
  )

  /** 取数+决策, 产出**完整两腿** (call+put) 或 Left。任一腿数量不合规/无盘口报价 -> 整体 Left
    * (绝不只下单腿, 避免裸方向敞口)。orderLinkId 由决策锚点派生 -> 幂等 (重启/重试不重复下单)。
    * 决策锚点: 常规=本周五, **runNow=上周五**; 目标到期 = 锚点 + targetDays (默认 21 天后那个周五的周度期权),
    * 在该到期内取**当前价位最近的宽跨 (strangle)**; 卖价按价差选对手价(taker)或中价−offset(maker)。 */
  def plan(ex: OptionsExchange, cfg: Config, nowMs: Long, runNow: Boolean = false): Either[String, Decision] =
    for
      closes <- ex.underlyingCloses5m(cfg.symbol, cfg.bars2w)
      _ <- Either.cond(closes.sizeIs >= cfg.bars2w * 9 / 10, (), s"K线不足 ${closes.size}/${cfg.bars2w} (<90%), 跳过避免RV失真")
      spot <- closes.lastOption.toRight("无现价")
      chain <- ex.optionChain(cfg.baseCoin)
      anchor = SellVolPlan.decisionAnchor(nowMs, runNow)
      targetMs = SellVolPlan.targetExpiryMs(anchor, cfg.targetDays)
      strangle <- SellVolPlan.selectStrangle(chain, nowMs, spot, targetMs).toRight("未找到 ~目标到期 价外宽跨 (spot 超出行权范围?)")
      (call, put) = strangle
      candidateStrikes = chain.iterator.filter(_.expiryMs == call.expiryMs).map(_.strike).distinct.toVector.sorted
      sizing <- SellVolPlan
        .decideMultiplier(closes, cfg.gridHigh, cfg.gridLow)
        .toRight(s"收盘价 ${closes.size} 根估不出两周 RV, 无法定仓位倍数")
      callLeg <- leg(ex, call, cfg.baseQty * sizing.mult, cfg.maxQty, anchor)
      putLeg <- leg(ex, put, cfg.baseQty * sizing.mult, cfg.maxQty, anchor)
    yield Decision(
      sizing.mult, sizing.rvPrev, sizing.rvThis, spot,
      call.expiryMs, call.strike, put.strike, candidateStrikes, Seq(callLeg, putLeg),
    )

  private def leg(ex: OptionsExchange, inst: OptionInstrument, rawQty: Double, maxQty: Double, periodMs: Long): Either[String, Leg] =
    for
      qty <- SellVolPlan.quantizeQty(rawQty, inst.qtyStep, inst.minQty)
        .toRight(s"${inst.symbol} 数量 $rawQty 不合规 (step=${inst.qtyStep} min=${inst.minQty})")
      _ <- Either.cond(qty <= maxQty, (), s"${inst.symbol} 数量 $qty 超硬上限 $maxQty -> 整体跳过 (疑似 scale bug)")
      quoteOpt <- ex.optionQuote(inst.symbol)
      quote <- quoteOpt.toRight(s"${inst.symbol} 无两边盘口报价, 跳过 (无法定价)")
    yield
      val (price, postOnly) = SellVolPlan.sellQuote(quote)
      val tag = if inst.right == OptionRight.Call then "c" else "p"
      Leg(inst.symbol, qty, price, postOnly, s"vs-$periodMs-$tag".take(36), inst.right)

  /** 下单 (按 leg.postOnly 选 maker/taker)。返回每腿结果。 */
  def execute(ex: OptionsExchange, d: Decision): Seq[(Leg, Either[String, String])] =
    d.legs.map(l => l -> ex.sellOption(l.symbol, l.qty, l.price, l.postOnly, l.orderLinkId))
