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
    /** 各腿的**最小可发量** (币本位) —— 配平容差取它。比它还小的残量在交易所根本发不出去,
      * 判成"未配平"只会让平腿单被精度拒, 然后策略卡死。见 [[ArbPlan.netExposure]]。 */
    minOrderOf: Instrument => Coin,
) extends Strategy:
  require(legs.sizeIs >= 2, s"$ticker 至少要两条腿才谈得上跨所, 实为 ${legs.size}")
  require(legs.map(_.exchange).sizeIs == legs.size, s"$ticker 的腿必须分属不同交易所: $legs")

  private val logger = LoggerFactory.getLogger(classOf[CrossArbStrategy])
  private val checked = cfg.validated

  /** 策略此刻在做什么。
    *
    * **不记"成交了多少"** —— 那本账在框架的柜台里 (经启动对齐、经账本对账)，策略再记一份
    * 只会在重启后丢失，而裸仓位还在交易所。配平与否一律由 [[ArbPlan.netExposure]] 从
    * `positionSize` 派生。这里只记"我发出去的单回来了没有"。
    */
  private enum Phase:
    case Idle
    /** 两条 IOC 已发，`pending` 是还没到终态的腿。 */
    case Opening(rich: Instrument, cheap: Instrument, pending: Set[Instrument])
    /** 正在平掉裸敞口。 */
    case Unwinding(leg: Instrument, rich: Instrument, cheap: Instrument)

  private var phase: Phase = Phase.Idle

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

  /** 某条腿的当前仓位 —— **框架的账本**, 不是策略自己记的。 */
  private def positionOf(instrument: Instrument, ctx: StrategyContext): Coin =
    ctx.state.symbolState(instrument.symbol).map(_.positionSize(instrument.exchange)).getOrElse(Coin.Zero)

  private def onSignal(signal: SpreadDislocation, ctx: StrategyContext) =
    ArbPlan.plan(
      checked,
      signal,
      declaredLegs = legs,
      richQuote = quoteOf(signal.rich, ctx),
      cheapQuote = quoteOf(signal.cheap, ctx),
      positionOf = positionOf(_, ctx),
      inFlight = phase != Phase.Idle,
      minOrderOf = minOrderOf,
    ) match
      case Left(reason) =>
        // 每一个不开仓的理由都说出口 —— 否则一个"从来不开仓"的策略你查不出它卡在哪一步。
        // 未配平与在途要用 warn: 它们持续出现意味着**策略已经停摆**, 那不是 INFO 级的事。
        reason match
          case _: ArbPlan.Skip.Unbalanced | ArbPlan.Skip.InFlight | _: ArbPlan.Skip.AtPositionLimit =>
            logger.warn(s"[对敲 $ticker] 停摆中: ${reason.describe}")
          case _ => logger.info(s"[对敲 $ticker] 不动手: ${reason.describe}")
        Vector.empty
      case Right(legsToPlace) =>
        phase = Phase.Opening(signal.rich, signal.cheap, Set(signal.rich, signal.cheap))
        logger.warn(
          f"[对敲 $ticker] 卖 ${signal.rich} @${legsToPlace.priceOf(legsToPlace.sell).value}%.6f / " +
            f"买 ${signal.cheap} @${legsToPlace.priceOf(legsToPlace.buy).value}%.6f " +
            f"量=${legsToPlace.qty.value}%.6f 边=${legsToPlace.edgeBps}%.1fbp 偏离=${signal.deviationBps}%.1fbp"
        )
        // 一次 place 传两条: 框架按交易所拆成两条下单意图 (跨所决策不能挤在一条里)
        ctx.place(legsToPlace.orders, f"cross_arb_open | edge=${legsToPlace.edgeBps}%.1fbp")

  /** 回报按 **(交易所, 标的)** 认领, 不按 clientOrderId。
    *
    * clientOrderId 由 `StrategyRunner.prepareIntent` 在处理器返回之后统一分配, 策略这边看到的
    * 是覆写前的值 —— 拿它去匹配回报, 一条都匹配不上。按腿认领成立的前提是**同一条腿上同一时刻
    * 只有一张在途单**, 而这由 [[Phase]] 保证 (非 Idle 不开新仓)。
    */
  private def onOrderUpdate(update: OrderUpdate, ctx: StrategyContext) =
    val leg = Instrument(update.exchange, update.symbol)
    if !update.status.isTerminal then Vector.empty
    else
      phase match
        case Phase.Opening(rich, cheap, pending) if pending.contains(leg) =>
          val rest = pending - leg
          if rest.nonEmpty then
            phase = Phase.Opening(rich, cheap, rest)
            Vector.empty
          else settle(rich, cheap, ctx)
        case Phase.Unwinding(unwound, rich, cheap) if unwound == leg =>
          // 平腿单回来了 —— **必须复查**。平成功了才回 Idle; 没平掉就不能装作没事。
          settleUnwind(rich, cheap, ctx)
        case _ => Vector.empty // 不是本策略此刻在等的回报

  /** 两条 IOC 都到终态 —— 按**框架的仓位**看有没有配平。 */
  private def settle(rich: Instrument, cheap: Instrument, ctx: StrategyContext) =
    ArbPlan.netExposure(rich, positionOf(rich, ctx), cheap, positionOf(cheap, ctx), minOrderOf) match
      case None =>
        phase = Phase.Idle
        logger.warn(
          f"[对敲 $ticker] 两腿配平 (${positionOf(rich, ctx).value}%.6f / ${positionOf(cheap, ctx).value}%.6f) " +
            "—— 已持有价差仓位, **平仓需人工**"
        )
        Vector.empty
      case Some(naked) => unwind(naked, rich, cheap, ctx)

  /** 平掉裸敞口: **市价**。此刻手里是裸方向敞口, "少赚"远好过"敞着" —— 与开仓用 IOC 限价
    * 钉死最差成交价的取向相反, 因为要害不同。 */
  private def unwind(naked: ArbPlan.Naked, rich: Instrument, cheap: Instrument, ctx: StrategyContext) =
    phase = Phase.Unwinding(naked.leg, rich, cheap)
    logger.error(
      f"!!! [对敲 $ticker] 腿不平: ${naked.leg} 上净敞口 ${naked.excess.value}%.6f -> 市价平掉 (${naked.closeSide})"
    )
    ctx.place(
      Order("", naked.leg.exchange, naked.leg.symbol, naked.closeSide, OrderType.Market, naked.excess,
        reduceOnly = true, clientOrderId = ""),
      f"cross_arb_unwind | excess=${naked.excess.value}%.6f",
    )

  /** 平腿单终态之后复查。**平不掉就终止进程** —— 与本仓库的失败模型一致:
    * 结果不确定/收拾不了就停下来, 由人处理, 而不是留一个软状态让策略在裸敞口上继续跑。
    * 从前这里没有任何代码, 于是"平不掉就停住"只存在于类文档里。 */
  private def settleUnwind(rich: Instrument, cheap: Instrument, ctx: StrategyContext) =
    ArbPlan.netExposure(rich, positionOf(rich, ctx), cheap, positionOf(cheap, ctx), minOrderOf) match
      case None =>
        phase = Phase.Idle
        logger.warn(s"[对敲 $ticker] 裸敞口已平掉, 恢复")
        Vector.empty
      case Some(still) =>
        throw IllegalStateException(
          f"[对敲 $ticker] 平腿之后仍有裸敞口 ${still.excess.value}%.6f 在 ${still.leg} —— " +
            "平不掉的敞口不能靠策略自己扛着继续跑, 停机由人处理 (重启后启动对齐会看到真实仓位)"
        )
