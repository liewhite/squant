package strategy.strategies.crossspread.logic

import hft.domain.*

/** 跨所对敲的**纯决策** —— 无 IO、不读墙钟、不碰总线，因此可以脱离框架单测。
  *
  * ## 判据分两层，缺一不可
  *
  *   1. **偏离**：这一对的价差偏离它自己的中枢多少 (`deviationBps`，由
  *      [[CrossSpreadDetector]] 给)。这一层回答"它凭什么会回来" —— 持续存在的价差是结构性的
  *      (资金费、参与者、上市时间差)，照着它开仓等来的不是回归而是持仓成本
  *      (见 `docs/structural-edge-experiment.md` 的负结果)。
  *   2. **可执行的边**：此刻真的吃得到多少 (`edgeBps` = 卖腿买一 / 买腿卖一)。这一层回答
  *      "吃得动吗"。**必须用 bid/ask，不能用中价** —— 中价差比可执行的边**系统性地多出
  *      两边价差的均值**，在决策边界上恰好是最要紧的地方。
  *
  * 两层都过了还要过第三道：**边必须盖住来回四腿的成本**。开仓两腿是 taker，将来平仓还有两腿。
  * 只看边不看成本的阈值，会在"看起来有 8bp"的地方反复付掉 10bp 手续费。
  *
  * ## 它不承诺"锁定"
  *
  * 两条腿是两个交易所的两个永续，不会互相结算。成交之后手里是「短 rich、长 cheap」，
  * 方向上平，但**仓位没有消失**：
  *
  * {{{
  *   P&L = edge − 平仓时的价差 − 四腿成本
  * }}}
  *
  * 所以"偏离"那一层不是可选的锦上添花，它是这笔交易能不能赚的**全部依据**。
  */
object ArbPlan:

  /** 一条腿此刻的盘口。 */
  final case class LegQuote(instrument: Instrument, bid: Price, ask: Price):
    require(bid.value > 0 && ask.value > 0, s"$instrument 的盘口必须两边都有正报价: bid=$bid ask=$ask")

  /** 对敲参数。**危险方向的字段一律没有默认值。** */
  final case class Config(
      /** 偏离门槛 (bp)：价差偏离中枢多少才动手。 */
      minDeviationBps: Double,
      /** 来回四腿的成本 (bp)：开仓两腿 taker + 平仓两腿，加滑点。
        *
        * **没有默认值** —— 它是你的费率档的事实 (VIP 等级、返佣、抵扣都会改它)，框架替你猜一个,
        * 猜出来的仍是一份没有依据的数字, 而这个数字直接决定"看起来有边"的时候到底赚不赚。 */
      roundTripCostBps: Double,
      /** 扣掉成本之后至少还要剩多少 (bp) 才值得动手。 */
      minProfitBps: Double,
      /** 单腿下单量 (币本位)。 */
      qtyPerLeg: Coin,
      /** 两腿报价的最大年龄 (ms)：超过它这两个价不可比 —— 一边现价配另一边几十秒前的价,
        * 差出来的里面掺着这段时间对方走过的路。 */
      maxQuoteAgeMs: Long,
      /** **false = 只算不下单。** 真实下单必须由配置显式开启 —— 与本仓库其它实盘策略同一姿态。 */
      enableOrders: Boolean = false,
  ):
    def validated: Config =
      require(minDeviationBps > 0, s"minDeviationBps 须为正, 实为 $minDeviationBps")
      require(roundTripCostBps >= 0, s"roundTripCostBps 须 >= 0, 实为 $roundTripCostBps")
      require(minProfitBps > 0, s"minProfitBps 须为正 —— 0 意味着'刚好打平也做', 那是白担腿风险")
      require(qtyPerLeg > Coin.Zero, s"qtyPerLeg 须为正, 实为 $qtyPerLeg")
      require(maxQuoteAgeMs > 0, s"maxQuoteAgeMs 须为正, 实为 $maxQuoteAgeMs")
      this

    /** 边至少要到这里才动手。 */
    def requiredEdgeBps: Double = roundTripCostBps + minProfitBps

  /** 不动手的理由 —— **每一条都要能说出口**。
    *
    * 用 enum 而不是 `Option`/`Boolean`: 一个"没开仓"的 None 在日志里等于什么都没说, 而这些
    * 理由彼此完全不同 (盘口没到 vs 边不够 vs 上一轮还没配平)。分不开的话, 一个"从来不开仓"
    * 的策略你查不出它卡在哪一步。 */
  enum Skip:
    /** 这一对上还有未配平的量 —— 先把它处理掉, 不能在裸敞口上再叠一层。 */
    case Unbalanced(excess: Coin, on: Instrument)
    /** 上一轮的两条 IOC 还没都回来。 */
    case InFlight
    case DeviationTooSmall(deviationBps: Double, required: Double)
    case EdgeBelowCost(edgeBps: Double, required: Double)
    case QuotesTooOld(ageMs: Long, maxAgeMs: Long)
    case NoQuote(instrument: Instrument)
    /** enableOrders = false: 算出来了但不下单。 */
    case OrdersDisabled(edgeBps: Double)

    def describe: String = this match
      case Unbalanced(excess, on)          => f"上一轮未配平 (${excess.value}%.6f 裸在 $on), 先处理它"
      case InFlight                        => "上一轮的 IOC 还没都回来"
      case DeviationTooSmall(dev, req)     => f"偏离 $dev%.1fbp 未到门槛 $req%.1fbp"
      case EdgeBelowCost(edge, req)        => f"可执行边 $edge%.1fbp 盖不住来回成本+利润门槛 $req%.1fbp"
      case QuotesTooOld(age, max)          => s"两腿报价最旧 ${age}ms 超过 ${max}ms, 此刻两个价不可比"
      case NoQuote(instrument)             => s"$instrument 此刻没有两边盘口"
      case OrdersDisabled(edge)            => f"enableOrders=false: 本该对敲 (边 $edge%.1fbp), 只记录不下单"

  /** 决定要下的两条腿。 */
  final case class Legs(sell: Order, buy: Order, sellPrice: Price, buyPrice: Price, edgeBps: Double, qty: Coin):
    def orders: Vector[Order] = Vector(sell, buy)

  /** 可执行的边 (bp) = 1e4 × ln(卖腿买一 / 买腿卖一)。
    *
    * 与 [[CrossSpreadDetector]] 的 `crossEdgeBps` 同一个量、同一份算法入口 ([[logRatioBps]]) ——
    * 这里重算是因为要用**此刻**的盘口, 而信号载荷里那份是产生信号那一刻的。 */
  def edgeBps(sell: LegQuote, buy: LegQuote): Double =
    CrossSpreadDetector.logRatioBps(sell.bid, buy.ask)

  /** 决策。`held` 是这一对上尚未配平的量 (None = 已配平)。 */
  def plan(
      cfg: Config,
      signal: SpreadDislocation,
      richQuote: Option[LegQuote],
      cheapQuote: Option[LegQuote],
      unbalanced: Option[(Coin, Instrument)],
      inFlight: Boolean,
      newClientOrderId: Exchange => String,
  ): Either[Skip, Legs] =
    for
      _ <- unbalanced.toLeft(()).left.map((excess, on) => Skip.Unbalanced(excess, on))
      _ <- Either.cond(!inFlight, (), Skip.InFlight)
      _ <- Either.cond(
        signal.deviationBps >= cfg.minDeviationBps,
        (),
        Skip.DeviationTooSmall(signal.deviationBps, cfg.minDeviationBps),
      )
      _ <- Either.cond(
        signal.quoteAgeMs <= cfg.maxQuoteAgeMs,
        (),
        Skip.QuotesTooOld(signal.quoteAgeMs, cfg.maxQuoteAgeMs),
      )
      sellLeg <- richQuote.toRight(Skip.NoQuote(signal.rich))
      buyLeg <- cheapQuote.toRight(Skip.NoQuote(signal.cheap))
      edge = edgeBps(sellLeg, buyLeg)
      _ <- Either.cond(edge >= cfg.requiredEdgeBps, (), Skip.EdgeBelowCost(edge, cfg.requiredEdgeBps))
      _ <- Either.cond(cfg.enableOrders, (), Skip.OrdersDisabled(edge))
    yield Legs(
      // 卖贵的一边: IOC 限价挂在它的买一 —— 主动吃单, 价内即成, 不留挂单。
      sell = ioc(sellLeg.instrument, Side.Short, sellLeg.bid, cfg.qtyPerLeg, newClientOrderId),
      buy = ioc(buyLeg.instrument, Side.Long, buyLeg.ask, cfg.qtyPerLeg, newClientOrderId),
      sellPrice = sellLeg.bid,
      buyPrice = buyLeg.ask,
      edgeBps = edge,
      qty = cfg.qtyPerLeg,
    )

  private def ioc(
      instrument: Instrument,
      side: Side,
      price: Price,
      qty: Coin,
      newClientOrderId: Exchange => String,
  ): Order =
    Order(
      id = "",
      exchange = instrument.exchange,
      symbol = instrument.symbol,
      side = side,
      // IOC 而不是市价: 限价把**最差成交价**钉死在决策时看到的那个价上。市价单在薄盘上会吃穿
      // 多档, 而这笔交易的全部利润只有几个 bp —— 一次穿档就把它连本带利吃掉。
      orderType = OrderType.Limit(price, TimeInForce.IOC),
      quantity = qty,
      reduceOnly = false,
      clientOrderId = newClientOrderId(instrument.exchange),
    )

  /** 两腿都终态之后的结果。 */
  enum Outcome:
    /** 两腿成交量在容差内一致 —— 这才是想要的形态。 */
    case Balanced(qty: Coin)
    /** **一条腿成得比另一条多**: 多出来的那部分是裸方向敞口, 必须平掉。
      *
      * 这不是边角情况, 而是这类策略的主要风险: 两条腿是两次独立的 REST 往返, 中间那个窗口
      * 里对手盘会动, IOC 于是可能一条全成、一条部分成或整单取消。 */
    case Naked(excess: Coin, on: Instrument, closeSide: Side)
    /** 两腿都没成交 —— IOC 没吃到, 没有敞口, 什么都不用做。 */
    case Missed

  /** 对账: 两腿各成交了多少, 得出要不要平、平哪边、平多少。
    *
    * @param tolerance 视为"配平"的容差。取两腿的最小下单步长即可 —— 比它还小的残量在交易所
    *                  也发不出去, 追着平会陷入"发不出去 -> 还是不平 -> 再发"的循环。
    */
  def reconcile(
      sellLeg: Instrument,
      sellFilled: Coin,
      buyLeg: Instrument,
      buyFilled: Coin,
      tolerance: Coin,
  ): Outcome =
    require(sellFilled >= Coin.Zero && buyFilled >= Coin.Zero, s"成交量不能为负: $sellFilled / $buyFilled")
    require(tolerance > Coin.Zero, s"容差须为正, 实为 $tolerance")
    val diff = sellFilled - buyFilled
    if sellFilled.isZero && buyFilled.isZero then Outcome.Missed
    else if diff.abs <= tolerance then Outcome.Balanced(Coin(math.min(sellFilled.value, buyFilled.value)))
    // 卖腿成得多 -> 净空 -> 买回来平掉多出的部分
    else if diff > Coin.Zero then Outcome.Naked(diff, sellLeg, Side.Long)
    // 买腿成得多 -> 净多 -> 卖掉多出的部分
    else Outcome.Naked(diff.abs, buyLeg, Side.Short)
