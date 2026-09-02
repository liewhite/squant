package hft.sim

import hft.actor.ActorSystem
import hft.domain.*
import hft.event.Commands.{MarketSubscription, MarketSubscriptionRequest}
import hft.event.{AnyEvent, CommandHandler, CommandTopic, Event, EventBus, Interest, Topic, Topics}
import hft.exchange.{MarketFeed, TradingGateway}
import org.slf4j.LoggerFactory

/** 虚拟柜台的延迟与初始资金配置。
  *
  * @param exchangeToStrategyDelayMs 交易所 -> 策略 的单向延迟 (行情/订单回报/成交回流的延迟)
  * @param orderToExchangeDelayMs    下单 -> 交易所 的单向延迟 (下单/撤单到达撮合的延迟)
  * @param initialBalanceUsdt        初始账户现金 (USDT)
  */
final case class SimConfig(
    exchangeToStrategyDelayMs: Long = 50,
    orderToExchangeDelayMs: Long = 30,
    initialBalanceUsdt: Double = 10_000.0,
    /** maker 手续费率 (resting 单被越价成交)，0.0002 = 0.02%。默认 0 = 不计费 */
    makerFeeRate: Double = 0.0,
    /** taker 手续费率 (到达即吃单成交)，0.0005 = 0.05%。默认 0 = 不计费 */
    takerFeeRate: Double = 0.0,
)

/** 上游行情抵达柜台 —— 从私有总线的消费线程串行化回 actor 线程的那一跳。
  *
  * 走总线而不是直接调用：柜台状态的写者必须只有 actor 线程一个。key 是账户，
  * 因此只有本柜台会收到自己的上游行情。
  */
private[sim] final case class UpstreamMarket(account: AccountId, event: AnyEvent)

private[sim] object UpstreamMarkets extends Topic[AccountId, UpstreamMarket]("simUpstreamMarket"):
  def keyOf(payload: UpstreamMarket): AccountId = payload.account

/** 虚拟柜台 (替身)：**整个交易所**的替代品 —— 行情面与交易面都由它扮演。
  *
  * 与 [[PaperCounter]] 的区别是定位而非撮合：后者与实盘**并存** (两边看同一份真实行情、
  * 跑同一份逻辑，只有账户不同)，本类则**替换**掉真实交易所的两个插件，策略对真假无感知。
  * 撮合内核 ([[SimState]]) 是同一个。
  *
  * ## 为什么行情要经过它，而不是让真实行情插件直接上总线
  *
  * 撮合用**实时**行情，策略看**延迟**行情 —— 如实建模"基于陈旧价格挂单、订单在途期间
  * 行情已经动了"。同一条行情要有两个到达时刻，就不能只有一条路径：
  *
  * {{{
  *   上游真实行情源 ──> 私有总线 ──> 本柜台 ──┬──> 撮合 (实时)
  *                                          └──> 延迟 ──> 主总线 ──> 策略
  * }}}
  *
  * 上游行情源装在**私有总线**上，主总线上看不见它 —— 否则策略会先收到那份没有延迟的。
  * 行情订阅指令则反向穿过来：主总线 -> 本柜台 -> 私有总线 -> 上游。
  *
  * ## 线程模型
  *
  * 所有命令 (上游行情、下单到达、撤单到达) 都经**主总线**串行进入本柜台的邮箱，
  * 由唯一的 actor 线程消费 —— 它是 [[SimState]] 的唯一写者，也是回流事件的唯一发布者。
  * 于是"状态变更顺序 == 回流顺序"天然成立 (一张订单的 Pending 必早于其 Filled)，无需锁。
  * 延迟只由定时器负责"把发布推迟到点"，不触碰状态。
  */
final class SimulatedExchange(
    /** 上游真实行情源。装在私有总线上，由本柜台独占 —— 它的输出只经本柜台的延迟通道外流 */
    upstream: MarketFeed,
    /** 本所合约规格：撮合前按交易所精度对齐，与真实柜台同一份判据 */
    metas: Map[Symbol, SymbolMeta],
    config: SimConfig = SimConfig(),
    /** 本柜台服务的账户。作为实盘替身时是 [[AccountId.Live]] (策略对真假无感知)。
      * 无默认值，理由同 [[hft.exchange.TradingGateway.account]] */
    override val account: AccountId,
) extends TradingGateway:
  private val logger = LoggerFactory.getLogger(classOf[SimulatedExchange])

  override def exchange: Exchange = upstream.exchange
  override def name: String = s"simulated-exchange@$target"
  override protected def accountRefreshMs: Long = 1000

  /** 唯一写者是 actor 线程；净值刷新线程只读快照 —— 不可变状态 + @volatile 即可, 无需锁 */
  @volatile private var state: SimState =
    SimState.empty(account, config.initialBalanceUsdt, config.makerFeeRate, config.takerFeeRate)
  private var orderIdSeq: Long = 0L

  /** 上游行情源专属的私有总线：主总线上看不见它发的行情 */
  private val rawBus = EventBus()

  /** 除下单与对齐之外，本柜台还要接**行情订阅指令** —— 它扮演的是整个交易所，
    * 行情面归它管。指令原样转给私有总线上的上游。 */
  override protected def extraCommandHandlers: Set[CommandHandler] = Set(
    CommandHandler.command(MarketSubscription, exchange)
  )

  override protected def extraInterests: Set[Interest] = Set(
    Interest.Keyed(UpstreamMarkets, Set(account)),
    Interest.Keyed(CounterCommands, Set(account)),
  )

  override protected def connect(): Unit =
    val rawSystem = childSystem(rawBus)
    // **收上游的一切**, 而不是按内置行情 topic 枚举着收: 用户自定义的行情源同样是一等公民
    // (见 Subscription.marketStreams), 枚举着收会让它在这里静默消失 —— 契约校验通过、
    // 订阅指令也转下去了, 数据却没人接着往外送。
    val fromUpstream = manage(rawBus.subscribeAll()) { mailbox =>
      mailbox.close()
      mailbox.done()
    }
    // 私有总线的消费线程只做一件事：把行情投回主总线上本柜台自己的键，
    // 于是它重新回到 actor 线程手里 —— 状态的写者仍然只有一个。
    fork {
      fromUpstream.events.foreach { event =>
        // 跳过指令: 它是从主总线**流进来**的 (见 onOther), 原样转回去会让订阅指令
        // 在两条总线之间无限弹跳。
        if !event.topic.isInstanceOf[CommandTopic[?, ?]] then
          publish(Event.local(UpstreamMarkets, UpstreamMarket(account, event)))
      }
    }
    // 先建立中继再启动源，避免源在 connect 中立即发布的首批行情落入订阅窗口之前。
    rawSystem.spawn(upstream)
    logger.info(
      s"虚拟柜台 (替身) 启动: $target order->ex=${config.orderToExchangeDelayMs}ms " +
        s"ex->strat=${config.exchangeToStrategyDelayMs}ms 初始资金=${config.initialBalanceUsdt}"
    )

  override protected def onOther(event: AnyEvent, now: Timestamp): Vector[AnyEvent] =
    event.as(MarketSubscription).foreach { request =>
      // 原样转给上游 —— 去重由上游的 MarketFeed 基类负责, 这里不必再做一遍
      rawBus.publish(Event.local(MarketSubscription, MarketSubscriptionRequest(request.exchange, request.kinds)))
    }
    event.as(UpstreamMarkets).foreach { arrival =>
      // 本柜台**替换**整个交易所, 策略的行情只有这一个来源 -> 要原样转发 (撮合本身不回显)。
      // 先转发后撮合: 同一个单线程定时器按提交序 FIFO, 故策略必先看到行情再看到它引发的成交。
      emit(Counter.toStrategy(config, arrival.event))
      matchNow(CounterInput.Market(arrival.event), now)
    }
    event.as(CounterCommands).foreach(command => matchNow(command.input, now))
    Vector.empty

  // ==================== 撮合 ====================

  override protected def metaOf(symbol: Symbol): SymbolMeta =
    metas.getOrElse(symbol, sys.error(s"虚拟柜台没有 $symbol 的合约规格, 无法撮合 (装配时未加载?)"))

  override protected def placeAligned(order: Order, now: Timestamp): Unit =
    orderIdSeq += 1
    enqueue(Counter.inbound(config, CounterInput.OrderArrived(order, orderIdSeq.toString)))

  override protected def cancelOrder(symbol: Symbol, ref: OrderRef, now: Timestamp): Unit =
    enqueue(Counter.inbound(config, CounterInput.CancelArrived(ref)))

  /** 落地一次撮合转移：新状态 + 按各自延迟回传的回报。
    * 延迟语义来自 [[Counter]] (与回测、影子盘同一份)，这里只负责用真实定时器等到点。 */
  private def matchNow(input: CounterInput, now: Timestamp): Unit =
    val (next, replies) = Counter.step(state, exchange, input, now, config)
    state = next
    replies.foreach { out =>
      out.value.as(Topics.Fill).foreach(f => logger.info(s"[SIM] fill ${f.side} ${f.symbol} qty=${f.size} @ ${f.price}"))
      emit(out)
    }

  private def emit(out: Delayed[AnyEvent]): Unit = schedule(out.delayMs, out.value)

  /** 兑现一条延迟输入：在途结束后作为命令回到本柜台的邮箱 */
  private def enqueue(cmd: Delayed[CounterInput]): Unit =
    schedule(cmd.delayMs, Event.local(CounterCommands, CounterCommand(account, cmd.value)))

  // ==================== 对齐 ====================

  /** 替身账户同样从零开始 —— 没有"历史"可言，如实报告当下的账本 */
  /** 虚拟柜台的账本与挂单簿都在自己手里, 读它们本就是原子的 */
  override protected def syncSnapshot(symbols: Set[Symbol]): TradingGateway.AccountSnapshot =
    TradingGateway.AccountSnapshot(positions, restingOrders(symbols))

  /** 本柜台当前的持仓快照 (供测试与绩效统计) */
  def positions: Vector[Position] = state.ledger.openPositions(exchange)

  /** 当前挂单簿里还有几张单 (供测试观察撮合进度) */
  def restingCount: Int = state.resting.size

  private def restingOrders(symbols: Set[Symbol]): Vector[OrderUpdate] =
    state.resting.values.filter(o => symbols.contains(o.symbol)).map { o =>
      OrderUpdate(
        account, o.orderId, Some(o.clientOrderId), exchange, o.symbol, o.side,
        OrderStatus.Pending, o.limitPrice, o.quantity, Coin.Zero, o.reduceOnly, nowMs,
      )
    }.toVector

  override protected def currentAccountInfo(): AccountInfo = state.accountInfo(exchange)

  /** 本账户当前的账本快照 (供绩效统计与测试) */
  def ledger: Ledger = state.ledger
