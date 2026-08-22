package hft.sim

import hft.actor.{Actor, ActorContext}
import hft.domain.*
import hft.event.{AnyEvent, Event, Interest, Topic, Topics}
import hft.strategy.{AccountOutcome, OrderIntent, OutcomeEvent}
import org.slf4j.LoggerFactory

/** 虚拟柜台内部的延迟命令 —— "过一会儿才到达撮合"的那件事。
  *
  * 走总线而不是自己起一条内部队列：这样命令仍由 actor 线程串行消费，柜台状态的写者
  * 依旧只有一个。key 是账户，因此只有本柜台会收到自己的命令。
  */
private[sim] enum CounterCommand(val account: AccountId):
  case OrderArrived(override val account: AccountId, order: Order, orderId: OrderId) extends CounterCommand(account)
  case CancelArrived(override val account: AccountId, ref: OrderRef) extends CounterCommand(account)

private[sim] object CounterCommands extends Topic[AccountId, CounterCommand]("paperCounterCommand"):
  def keyOf(payload: CounterCommand): AccountId = payload.account

/** 虚拟柜台：在真实 gateway **旁边**并行跑一个模拟账户。
  *
  * 与 [[SimulatedExchange]] 的区别是定位而非撮合 —— 后者**替换**整个 gateway (策略对真假
  * 无感知)，本类则与实盘同时存在：两边看同一份真实行情、跑同一份策略逻辑，只有账户不同。
  * 撮合内核 ([[SimState]]) 是同一个。
  *
  * ## 为什么建模延迟
  *
  * 影子盘存在的理由是"预测实盘表现"。若它没有下单在途与回报回传的延迟，就会系统性偏
  * 乐观，据此得出的结论无法外推到实盘。延迟用 [[ActorContext.scheduleEvent]] 表达：
  * 定时器只把发布推迟到点，撮合仍在 actor 线程串行进行。
  *
  * ## 如实声明的偏差
  *
  * 撮合不建模**队列位置**：只要行情越过挂单价就算成交。成交**价格**是对的，成交**机会**
  * 偏多 —— 真实盘口里排在后面的单可能根本轮不到。所以影子盘的成交率与盈亏系统性偏高，
  * 拿它做晋升判据时门槛要留余量。晋升后实盘与影子并行，两边成交率之差正是校准这个偏差
  * 的数据。
  */
final class PaperCounter(
    val account: AccountId,
    exchange: Exchange,
    config: SimConfig,
    /** 撮合要用它把订单从交易所格式还原成币本位 (见 [[SimState.onOrderArrived]]) */
    symbolMetas: Map[(Exchange, Symbol), SymbolMeta],
    /** 净值刷新间隔：与实盘的 [[hft.engine.AccountRefresher]] 对齐，让两边的净值同频 */
    equityRefreshMs: Long = 1000,
) extends Actor:
  require(account != AccountId.Live, s"虚拟柜台不能占用实盘账户: $account")

  private val logger = LoggerFactory.getLogger(classOf[PaperCounter])
  private var state: SimState = SimState.empty(account, symbolMetas, config.initialBalanceUsdt, config.makerFeeRate, config.takerFeeRate)
  private var ctx: ActorContext = scala.compiletime.uninitialized
  private var orderIdSeq: Long = 0L
  private var lastEquityAt: Timestamp = 0L

  override def name: String = s"paper-counter@$account"

  /** 行情全量收：柜台是基础设施，哪个标的会被交易由策略决定，它不必也不该预先知道。
    * 下单意图只收自己账户的 —— 实盘的意图不该进虚拟柜台。
    */
  override def interests: Set[Interest] = Set(
    Interest.Keyed(OrderIntent, Set(account)),
    Interest.Keyed(CounterCommands, Set(account)),
    Interest.All(Topics.Bbo),
    Interest.All(Topics.Trade),
    Interest.All(Topics.MarkPrice),
    Interest.All(Topics.Clock),
  )

  override def onStart(context: ActorContext): Unit =
    ctx = context
    logger.info(s"paper counter started: $account balance=${config.initialBalanceUsdt}")

  override def onEvent(event: AnyEvent, now: Timestamp): Vector[AnyEvent] =
    // 行情即时进撮合 (柜台用实时行情)，回报按 ex->strat 延迟回传给策略 ——
    // 如实建模"基于稍陈旧的价格挂单、订单在途期间行情已经动了"
    if event.is(Topics.Bbo) || event.is(Topics.Trade) || event.is(Topics.MarkPrice) then
      applyMatching(state.onMarket(exchange, event, now), forwardMarket = false)

    event.as(OrderIntent).foreach(intent => onIntent(intent, now))

    event.as(CounterCommands).foreach {
      case CounterCommand.OrderArrived(_, order, orderId) =>
        applyMatching(state.onOrderArrived(exchange, order, orderId, now), forwardMarket = false)
      case CounterCommand.CancelArrived(_, ref) =>
        applyMatching(state.onCancelArrived(exchange, ref, now), forwardMarket = false)
    }

    event.as(Topics.Clock).foreach(_ => publishEquity(now))
    Vector.empty

  private def onIntent(intent: AccountOutcome, now: Timestamp): Unit = intent.outcome match
    case OutcomeEvent.PlaceOrders(orders, comment) =>
      orders.foreach { order =>
        orderIdSeq += 1
        val orderId = s"paper-$orderIdSeq"
        logger.debug(s"[$account] order in flight: ${order.symbol} ${order.side} qty=${order.quantity} ($comment)")
        ctx.scheduleEvent(
          config.orderToExchangeDelayMs,
          Event.local(CounterCommands, CounterCommand.OrderArrived(account, order, orderId)),
        )
      }
    case OutcomeEvent.CancelOrder(_, _, ref) =>
      ctx.scheduleEvent(
        config.orderToExchangeDelayMs,
        Event.local(CounterCommands, CounterCommand.CancelArrived(account, ref)),
      )

  /** 落地撮合结果：回报按 ex->strat 延迟发回策略。
    *
    * 第一个元素是被转发的行情本身 (柜台替身模式下要转发给策略)，这里策略直接从总线读
    * 真实行情，不需要柜台再转一份 —— 转了就是重复投递。
    */
  private def applyMatching(transfer: (SimState, Vector[AnyEvent]), forwardMarket: Boolean): Unit =
    val (next, replies) = transfer
    state = next
    val toSend = if forwardMarket then replies else replies.filterNot(isMarketEcho)
    toSend.foreach(ev => ctx.scheduleEvent(config.exchangeToStrategyDelayMs, ev))

  private def isMarketEcho(ev: AnyEvent): Boolean =
    ev.is(Topics.Bbo) || ev.is(Topics.Trade) || ev.is(Topics.MarkPrice)

  /** 周期发布本账户净值 —— 策略的杠杆闸门读它。
    *
    * 也是 Paper 账户的"启动对齐"：它从零开始，没有历史仓位与挂单要恢复，
    * 唯一需要的初值就是净值，而周期刷新天然覆盖了这一点 (策略订阅后一个节拍内就能读到)。
    */
  private def publishEquity(now: Timestamp): Unit =
    if now - lastEquityAt >= equityRefreshMs then
      lastEquityAt = now
      val info = AccountInfo(account, exchange, state.ledger.equity(state.markOf), state.ledger.notional(state.markOf))
      ctx.publish(Event.local(Topics.AccountInfo, info))

  /** 本账户当前的账本快照 (供绩效统计与测试) */
  def ledger: Ledger = state.ledger
