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
      /** 单腿仓位上限 (币本位)。**没有默认值。**
        *
        * 本策略**只开仓不平仓**, 而检测器的冷却默认 60s —— 一次持续 30 分钟的偏离能开出几十份,
        * 每份付两腿 taker 费, 且没有任何退出路径。上限是这个缺口唯一的刹车, 所以它必须由
        * 调用方明说, 而不是给一个"看着安全"的默认值。 */
      maxPositionPerLeg: Coin,
      /** **false = 只算不下单。** 真实下单必须由配置显式开启 —— 与本仓库其它实盘策略同一姿态。 */
      enableOrders: Boolean = false,
  ):
    def validated: Config =
      require(minDeviationBps > 0, s"minDeviationBps 须为正, 实为 $minDeviationBps")
      require(roundTripCostBps >= 0, s"roundTripCostBps 须 >= 0, 实为 $roundTripCostBps")
      require(minProfitBps > 0, s"minProfitBps 须为正 —— 0 意味着'刚好打平也做', 那是白担腿风险")
      require(qtyPerLeg > Coin.Zero, s"qtyPerLeg 须为正, 实为 $qtyPerLeg")
      require(maxPositionPerLeg >= qtyPerLeg, s"maxPositionPerLeg ($maxPositionPerLeg) 至少要能装下一份 ($qtyPerLeg)")
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
    /** 信号里的腿不属于本实例 —— 检测器按 ticker 的**全部两两组合**出信号, 三家所就有三对,
      * 而本实例只声明了其中一些腿。在没声明的标的上下单会拿不到回报, 也没做过启动对齐。 */
    case ForeignLeg(leg: Instrument)
    /** 已经到单腿仓位上限。 */
    case AtPositionLimit(held: Coin, limit: Coin)
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
      case ForeignLeg(leg)                 => s"信号里的 $leg 不属于本实例声明的腿"
      case AtPositionLimit(held, limit)    => f"单腿仓位 ${held.value}%.6f 已到上限 ${limit.value}%.6f"
      case InFlight                        => "上一轮的 IOC 还没都回来"
      case DeviationTooSmall(dev, req)     => f"偏离 $dev%.1fbp 未到门槛 $req%.1fbp"
      case EdgeBelowCost(edge, req)        => f"可执行边 $edge%.1fbp 盖不住来回成本+利润门槛 $req%.1fbp"
      case QuotesTooOld(age, max)          => s"两腿报价最旧 ${age}ms 超过 ${max}ms, 此刻两个价不可比"
      case NoQuote(instrument)             => s"$instrument 此刻没有两边盘口"
      case OrdersDisabled(edge)            => f"enableOrders=false: 本该对敲 (边 $edge%.1fbp), 只记录不下单"

  /** 决定要下的两条腿。 */
  final case class Legs(sell: Order, buy: Order, edgeBps: Double):
    def orders: Vector[Order] = Vector(sell, buy)
    def qty: Coin = sell.quantity
    /** 日志用 —— 价格就在 Order 里, 不另存一份。 */
    def priceOf(order: Order): Price = order.orderType match
      case OrderType.Limit(px, _) => px
      case OrderType.Market       => Price.Zero

  /** 可执行的边 (bp) = 1e4 × ln(卖腿买一 / 买腿卖一)。
    *
    * 与 [[CrossSpreadDetector]] 的 `crossEdgeBps` 同一个量、同一份算法入口 ([[logRatioBps]]) ——
    * 这里重算是因为要用**此刻**的盘口, 而信号载荷里那份是产生信号那一刻的。 */
  def edgeBps(sell: LegQuote, buy: LegQuote): Double =
    CrossSpreadDetector.logRatioBps(sell.bid, buy.ask)

  /** 决策。
    *
    * @param positionOf       某条腿的**当前仓位** (来自框架的账本, 空头为负)。配平与仓位上限都
    *                         从它派生 —— 不在策略内存里另记一本: 那本账重启就没了, 而裸仓位
    *                         还在交易所, 于是重启后会在裸敞口上继续开新仓。
    *
    *                         传函数而不是值: 未声明的腿上读仓位会被框架直接拒
    *                         (`SymbolState.positionSize` 对未声明交易所抛错 —— 那道守卫是对的),
    *                         所以必须**先过腿校验再读**。
    * @param minOrderOf       某条腿的**最小可发量**。容差取它: 比它还小的残量在交易所根本发不出去,
    *                         判成"未配平"只会让平腿单被精度拒, 然后策略卡死在那里。
    */
  def plan(
      cfg: Config,
      signal: SpreadDislocation,
      declaredLegs: Set[Instrument],
      richQuote: Option[LegQuote],
      cheapQuote: Option[LegQuote],
      positionOf: Instrument => Coin,
      inFlight: Boolean,
      minOrderOf: Instrument => Coin,
  ): Either[Skip, Legs] =
    for
      // **腿校验必须最先做**: 后面每一步都要读这两条腿的仓位, 而未声明的腿上读仓位会被框架拒。
      _ <- Either.cond(declaredLegs.contains(signal.rich), (), Skip.ForeignLeg(signal.rich))
      _ <- Either.cond(declaredLegs.contains(signal.cheap), (), Skip.ForeignLeg(signal.cheap))
      _ <- Either.cond(!inFlight, (), Skip.InFlight)
      richPos = positionOf(signal.rich)
      cheapPos = positionOf(signal.cheap)
      _ <- netExposure(signal.rich, richPos, signal.cheap, cheapPos, minOrderOf)
        .toLeft(())
        .left
        .map(naked => Skip.Unbalanced(naked.excess, naked.leg))
      _ <- Either.cond(
        richPos.abs + cfg.qtyPerLeg <= cfg.maxPositionPerLeg && cheapPos.abs + cfg.qtyPerLeg <= cfg.maxPositionPerLeg,
        (),
        Skip.AtPositionLimit(Coin(math.max(richPos.abs.value, cheapPos.abs.value)), cfg.maxPositionPerLeg),
      )
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
      sell = ioc(sellLeg.instrument, Side.Short, sellLeg.bid, cfg.qtyPerLeg),
      buy = ioc(buyLeg.instrument, Side.Long, buyLeg.ask, cfg.qtyPerLeg),
      edgeBps = edge,
    )

  private def ioc(instrument: Instrument, side: Side, price: Price, qty: Coin): Order =
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
      // **空串** —— clientOrderId 由 `StrategyRunner.prepareIntent` 统一分配, 策略这里填什么都会被
      // 覆写。曾经在这里自己生成并拿它关联回报, 后果是一条回报都匹配不上: 策略永久卡在
      // "上一轮还在途", 而腿不平的检测与平腿代码一次都跑不到。
      clientOrderId = "",
    )

  /** 两腿仓位不抵消的部分 —— **裸方向敞口**。
    *
    * @param excess    要平掉多少
    * @param leg       在哪条腿上平 (取绝对仓位大的那条 —— 平它才是真正在减敞口)
    * @param closeSide 平仓方向
    */
  final case class Naked(excess: Coin, leg: Instrument, closeSide: Side)

  /** 由两腿的**当前仓位**算裸敞口。配平 = 短 rich 与长 cheap 相互抵消, 净额为零。
    *
    * 这是"有没有配平"的**唯一判据**, 不在策略内存里另记一本: 那本账重启就没了, 而裸仓位还在
    * 交易所。它同时天然覆盖"人工干预过"这种情形。
    *
    * 容差取**要平的那条腿的最小可发量**: 比它还小的残量在交易所根本发不出去, 判成未配平只会让
    * 平腿单被精度拒, 然后策略卡死。注意不是两腿步长的较小者 —— 那个方向是反的。
    */
  def netExposure(
      rich: Instrument,
      richPos: Coin,
      cheap: Instrument,
      cheapPos: Coin,
      minOrderOf: Instrument => Coin,
  ): Option[Naked] =
    val net = richPos + cheapPos
    // 平绝对仓位大的那条 —— 平小的那条是在**加**敞口
    val leg = if richPos.abs >= cheapPos.abs then rich else cheap
    val legPos = if richPos.abs >= cheapPos.abs then richPos else cheapPos
    val tolerance = minOrderOf(leg)
    require(tolerance > Coin.Zero, s"$leg 的最小可发量必须为正, 实为 $tolerance")
    if net.abs < tolerance then None
    else Some(Naked(net.abs, leg, if legPos > Coin.Zero then Side.Short else Side.Long))
