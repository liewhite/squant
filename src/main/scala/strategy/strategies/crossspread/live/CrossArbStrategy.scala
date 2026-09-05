package strategy.strategies.crossspread.live

import hft.domain.*
import hft.event.Topics
import hft.strategy.{Strategy, StrategyContext, StrategyHandlers}
import org.slf4j.LoggerFactory
import strategy.strategies.crossspread.logic.*

/** 跨所对敲 —— 价差**偏离中枢**达到阈值时两腿同时下 IOC。
  *
  * 一个实例负责**一个 ticker**（它在各家所的全部腿）。不是一个实例负责一个 `VenuePair`：
  * 同一个 ticker 在三家所上有三个 pair，而它们共享腿，三个实例会在
  * [[hft.engine.InstrumentClaims]] 上撞车（同一个 (账户, 标的) 只能有一个主人）。
  *
  * ## 「同时」是做不到的，这一点决定了这个类的形状
  *
  * 两条腿是两次**独立**的 REST 往返，去两个交易所。中间那个窗口里对手盘会动，于是 IOC 可能
  * 一条全成、一条部分成、一条整单取消。**腿不平是常态而不是异常**，所以：
  *
  *   - 下单之后进入 `inFlight`，两条 IOC 都到终态才结算，期间不再开新仓；
  *   - 结算按 [[ArbPlan.reconcile]]：配平就完事，不平就把多出来的那部分**立刻市价平掉**；
  *   - 平不掉（拒单/又只成一半）就**继续报错并停在未配平状态**，不再开新仓 —— 在裸敞口上
  *     叠一层的后果比停下来严重得多。
  *
  * 平掉那一步用**市价**而不是 IOC 限价：此刻手里已经是裸方向敞口，"少赚"远好过"敞着"。
  * 开仓那一步相反，用 IOC 限价把最差成交价钉死 —— 这笔交易全部利润只有几个 bp，
  * 一次穿档就连本带利吃掉。**两处方向相反, 因为要害不同。**
  *
  * ## 它凭什么赚
  *
  * 判据是**偏离中枢**，不是"价差大"。持续存在的价差是结构性的（资金费、参与者、上市时间差），
  * 照着它开仓等来的不是回归而是持仓成本。成交之后 `P&L = edge − 平仓时的价差 − 四腿成本`，
  * 所以"它会回到中枢"这条假设就是这笔交易的全部依据。详见 [[ArbPlan]]。
  *
  * **本策略只开仓，不管平仓。** 开出来的是「短 rich、长 cheap」的价差仓位，什么时候平、
  * 按什么价平，是另一个决定 —— 目前没有实现，需要人工处理。启动日志会把这件事说明。
  */
final class CrossArbStrategy(
    ticker: Ticker,
    /** 这个 ticker 在各家所的腿。至少两条，否则无所谓跨所。 */
    legs: Set[Instrument],
    cfg: ArbPlan.Config,
    /** 各腿的最小下单步长 —— 对账容差取它，见 [[ArbPlan.reconcile]]。 */
    stepOf: Instrument => Coin,
    override val orderTimeoutMs: Long,
) extends Strategy:
  require(legs.sizeIs >= 2, s"$ticker 至少要两条腿才谈得上跨所, 实为 ${legs.size}")
  require(legs.map(_.exchange).sizeIs == legs.size, s"$ticker 的腿必须分属不同交易所: $legs")

  private val logger = LoggerFactory.getLogger(classOf[CrossArbStrategy])
  private val checked = cfg.validated

  /** 本轮的两条腿及其 clientOrderId。 */
  private final case class Round(sell: Instrument, sellCid: String, buy: Instrument, buyCid: String)

  /** 本轮 —— 空 = 不在途。 */
  private var round: Option[Round] = None
  /** 还没到终态的 clientOrderId。空且 round 非空 = 可以结算了。 */
  private var pending: Set[String] = Set.empty
  /** 各腿的已成交量: 终态回报的 `filledQuantity` 是权威值 (不自己累加 Fill —— 那是第二本账)。 */
  private var filled: Map[String, Coin] = Map.empty
  /** 尚未配平的裸量 —— **非空就不再开新仓**。 */
  private var unbalanced: Option[(Coin, Instrument)] = None

  override def handlers: StrategyHandlers =
    StrategyHandlers.empty
      // 信号来自 detector（它算中枢与 z），本策略不重算 —— 那份判据只该有一处
      .custom(SpreadDislocations, Set(ticker))((signal, ctx, now) => onSignal(signal, ctx))
      // 两腿的盘口：下单价要用**此刻**的 bid/ask，不能用信号载荷里那份中价
      .market(Topics.Bbo, legs)((_, _, _) => Vector.empty)
      .own(Topics.OrderUpdate)((update, ctx, now) => onOrderUpdate(update, ctx))

  private def quoteOf(instrument: Instrument, ctx: StrategyContext): Option[ArbPlan.LegQuote] =
    for
      state <- ctx.state.symbolState(instrument.symbol)
      bbo <- state.bbo(instrument.exchange)
      if bbo.bidPrice.value > 0 && bbo.askPrice.value > 0
    yield ArbPlan.LegQuote(instrument, bbo.bidPrice, bbo.askPrice)

  private def onSignal(signal: SpreadDislocation, ctx: StrategyContext) =
    ArbPlan.plan(
      checked,
      signal,
      richQuote = quoteOf(signal.rich, ctx),
      cheapQuote = quoteOf(signal.cheap, ctx),
      unbalanced = unbalanced,
      inFlight = round.nonEmpty,
      newClientOrderId = _.newClientOrderId,
    ) match
      case Left(skip) =>
        // 每一个不开仓的理由都说出口 —— 否则一个"从来不开仓"的策略你查不出它卡在哪一步
        logger.info(s"[对敲 $ticker] 不动手: ${skip.describe}")
        Vector.empty
      case Right(legsToPlace) =>
        round = Some(Round(
          signal.rich, legsToPlace.sell.clientOrderId,
          signal.cheap, legsToPlace.buy.clientOrderId,
        ))
        pending = Set(legsToPlace.sell.clientOrderId, legsToPlace.buy.clientOrderId)
        filled = Map.empty
        logger.warn(
          f"[对敲 $ticker] 卖 ${signal.rich} @${legsToPlace.sellPrice.value}%.6f / " +
            f"买 ${signal.cheap} @${legsToPlace.buyPrice.value}%.6f " +
            f"量=${legsToPlace.qty.value}%.6f 边=${legsToPlace.edgeBps}%.1fbp 偏离=${signal.deviationBps}%.1fbp"
        )
        // 一次 place 传两条: 框架按交易所拆成两条下单意图 (跨所决策不能挤在一条里)
        ctx.place(legsToPlace.orders, f"cross_arb_open | edge=${legsToPlace.edgeBps}%.1fbp")

  private def onOrderUpdate(update: OrderUpdate, ctx: StrategyContext) =
    update.clientOrderId.filter(pending.contains) match
      case None => Vector.empty // 不是本轮在途的腿 (人工单、上一轮的残留回报、平仓单的回报)
      case Some(cid) if !update.status.isTerminal => Vector.empty
      case Some(cid) =>
        filled += cid -> update.filledQuantity
        pending -= cid
        if pending.nonEmpty then Vector.empty // 还有一条没回来, 等它
        else settle(ctx)

  /** 两条 IOC 都到终态 —— 结算这一轮。 */
  private def settle(ctx: StrategyContext) =
    val r = round.getOrElse(
      // round 与 pending 的生命周期错开了, 那是本类自己的 bug。不静默返回:
      // 那会让一个真实的裸敞口无人处理。
      throw IllegalStateException(s"[对敲 $ticker] 两腿都终态了却没有本轮记录 —— 状态机 bug")
    )
    val sellFilled = filled.getOrElse(r.sellCid, Coin.Zero)
    val buyFilled = filled.getOrElse(r.buyCid, Coin.Zero)
    round = None
    filled = Map.empty
    val tolerance = Coin(math.min(stepOf(r.sell).value, stepOf(r.buy).value))
    ArbPlan.reconcile(r.sell, sellFilled, r.buy, buyFilled, tolerance) match
      case ArbPlan.Outcome.Missed =>
        logger.info(s"[对敲 $ticker] 两腿都没成交 (IOC 没吃到), 无敞口")
        Vector.empty
      case ArbPlan.Outcome.Balanced(qty) =>
        logger.warn(f"[对敲 $ticker] 两腿配平, 各 ${qty.value}%.6f —— 已持有价差仓位, **平仓需人工**")
        Vector.empty
      case ArbPlan.Outcome.Naked(excess, on, closeSide) =>
        // 立刻市价平掉多出来的那部分: 此刻是裸方向敞口, "少赚"远好过"敞着"。
        unbalanced = Some((excess, on))
        logger.error(
          f"!!! [对敲 $ticker] 腿不平: $on 多出 ${excess.value}%.6f " +
            f"(卖腿成 ${sellFilled.value}%.6f / 买腿成 ${buyFilled.value}%.6f) -> 市价平掉"
        )
        ctx.place(
          Order("", on.exchange, on.symbol, closeSide, OrderType.Market, excess,
            reduceOnly = true, on.exchange.newClientOrderId),
          f"cross_arb_unwind | excess=${excess.value}%.6f",
        )
