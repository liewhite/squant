package hft.sim

import hft.actor.ActorContext
import hft.domain.*
import hft.event.{AnyEvent, Event, Interest, Topic, Topics}
import hft.exchange.TradingGateway
import org.slf4j.LoggerFactory

/** 虚拟柜台内部的延迟命令 —— "过一会儿才到达撮合"的那件事。
  *
  * 走总线而不是自己起一条内部队列：这样命令仍由 actor 线程串行消费，柜台状态的写者
  * 依旧只有一个。key 是账户，因此只有本柜台会收到自己的命令。
  *
  * 载荷直接用共用的 [[CounterInput]]，本类型只给它套一个账户路由键 —— 撮合命令的形态
  * 与回测、实盘替身是同一份。
  */
private[sim] final case class CounterCommand(account: AccountId, input: CounterInput)

private[sim] object CounterCommands extends Topic[AccountId, CounterCommand]("paperCounterCommand"):
  def keyOf(payload: CounterCommand): AccountId = payload.account

/** 虚拟柜台：在真实 gateway **旁边**并行跑一个模拟账户。
  *
  * 与 [[SimulatedExchange]] 的区别是定位而非撮合 —— 后者**替换**整个 gateway (策略对真假
  * 无感知)，本类则与实盘同时存在：两边看同一份真实行情、跑同一份策略逻辑，只有账户不同。
  * 撮合内核 ([[SimState]]) 是同一个。
  *
  * 不需要 `SymbolMeta`：本柜台从总线收 [[OrderIntent]]，那里的数量已是币本位。
  * 张数换算只发生在 exchange 适配层，而本柜台不在那条路径上 (替身 [[SimulatedExchange]]
  * 才要，它扮演的是收 `ExchangeOrder` 的交易所)。
  *
  * ## 为什么建模延迟
  *
  * 影子盘存在的理由是"预测实盘表现"。若它没有下单在途与回报回传的延迟，就会系统性偏
  * 乐观，据此得出的结论无法外推到实盘。延迟用 [[ActorContext.scheduleEvent]] 表达：
  * 定时器只把发布推迟到点，撮合仍在 actor 线程串行进行。
  *
  * ## 如实声明的偏差
  *
  * 撮合不建模**队列位置**，也不建模**盘口深度**，两侧朝相反方向近似（见 [[Matcher]]）：
  * maker 要价格**严格穿越**挂单价才成交（悲观，仅仅触及不算），taker 与盘口**价格重合
  * 即全量成交**（乐观，不看挂单量）。净效果是成交机会偏多、taker 成本偏低，所以影子盘的
  * 盈亏系统性偏高，拿它做晋升判据时门槛要留余量。晋升后实盘与影子并行，两边成交率之差
  * 正是校准这个偏差的数据。
  */
final class PaperCounter(
    /** 本柜台服务的影子账户。类型就是 [[AccountId.Paper]] —— 虚拟柜台占用实盘账户是**写不出来**的，
      * 不必再拿运行时 require 去挡（Scala 3 里带参数的 enum case 本身就是一个类型）。 */
    val paperAccount: AccountId.Paper,
    override val exchange: Exchange,
    config: SimConfig,
    /** 本所合约规格：影子盘也按交易所精度对齐, 否则它的成交量与实盘系统性地差一个取整，
      * 而它存在的全部理由就是预测实盘。 */
    metas: Map[Symbol, SymbolMeta],
    /** 净值刷新间隔：与实盘柜台对齐，让两边的净值同频 */
    equityRefreshMs: Long = 1000,
) extends TradingGateway:
  private val logger = LoggerFactory.getLogger(classOf[PaperCounter])
  /** 唯一写者是 actor 线程；净值刷新线程只读快照 —— 不可变状态 + @volatile 即可, 无需锁 */
  @volatile private var state: SimState =
    SimState.empty(paperAccount, config.initialBalanceUsdt, config.makerFeeRate, config.takerFeeRate)
  private var orderIdSeq: Long = 0L

  override def account: AccountId = paperAccount
  override def name: String = s"paper-counter@$paperAccount"
  override protected def accountRefreshMs: Long = equityRefreshMs

  /** 行情全量收：柜台是基础设施，哪个标的会被交易由策略决定，它不必也不该预先知道。
    * 下单意图与对齐指令由基类按 (账户, 交易所) 声明 —— 实盘的意图不会进虚拟柜台。
    */
  override protected def extraInterests: Set[Interest] = Set(
    Interest.Keyed(CounterCommands, Set(account)),
    Interest.All(Topics.Bbo),
    Interest.All(Topics.Trade),
    Interest.All(Topics.MarkPrice),
  )

  override protected def connect(): Unit =
    logger.info(s"paper counter started: $paperAccount balance=${config.initialBalanceUsdt}")

  override protected def onOther(event: AnyEvent, now: Timestamp): Vector[AnyEvent] =
    // 行情即时进撮合 (柜台用实时行情)，回报按 ex->strat 延迟回传给策略 ——
    // 如实建模"基于稍陈旧的价格挂单、订单在途期间行情已经动了"
    if isMarket(event) then matchNow(CounterInput.Market(event), now)
    event.as(CounterCommands).foreach(cmd => matchNow(cmd.input, now))
    Vector.empty

  override protected def metaOf(symbol: Symbol): SymbolMeta =
    metas.getOrElse(symbol, sys.error(s"影子柜台没有 $symbol 的合约规格, 无法撮合 (装配时未加载?)"))

  override protected def placeAligned(order: Order, now: Timestamp): Unit =
    orderIdSeq += 1
    logger.debug(s"[$paperAccount] order in flight: ${order.symbol} ${order.side} qty=${order.quantity}")
    enqueue(Counter.inbound(config, CounterInput.OrderArrived(order, s"paper-$orderIdSeq")))

  override protected def cancelOrder(symbol: Symbol, ref: OrderRef, now: Timestamp): Unit =
    enqueue(Counter.inbound(config, CounterInput.CancelArrived(ref)))

  /** 影子账户从零开始, 引擎不会给它发对齐指令 (见 Engine.addStrategies)。
    * 真收到指令时如实报告当下的账本 —— 撤下策略再装回来时它确实可能已经有仓位了。 */
  /** 影子账户的世界全在进程内, 一次读取本就是原子的 —— 挂单登记在别处, 这里不返回 */
  override protected def syncSnapshot(symbols: Set[Symbol]): TradingGateway.AccountSnapshot =
    TradingGateway.AccountSnapshot(state.ledger.openPositions(exchange), Vector.empty)

  /** 本账户当前净值 —— 策略的杠杆闸门读它。基类按 [[accountRefreshMs]] 周期发布 */
  override protected def currentAccountInfo(): AccountInfo = state.accountInfo(exchange)

  /** 撮合一条命令并落地：回报按各自延迟发回策略。
    *
    * 延迟由 [[Counter]] 决定（与回测、实盘替身同一份），这里只负责用 actor 定时器等到点。
    * 本柜台**不转发行情** —— 策略直接从总线读真实行情，柜台再转一份就是重复投递。
    * 撮合本身也不回显行情（见 [[SimState.onMarket]]），所以这里无需再过滤。
    */
  private def matchNow(input: CounterInput, now: Timestamp): Unit =
    val (next, replies) = Counter.step(state, exchange, input, now, config)
    state = next
    replies.foreach(out => schedule(out.delayMs, out.value))

  /** 兑现一条延迟输入：在途结束后作为命令回到本柜台的邮箱 */
  private def enqueue(cmd: Delayed[CounterInput]): Unit =
    schedule(cmd.delayMs, Event.local(CounterCommands, CounterCommand(account, cmd.value)))

  private def isMarket(ev: AnyEvent): Boolean =
    ev.is(Topics.Bbo) || ev.is(Topics.Trade) || ev.is(Topics.MarkPrice)

  /** 本账户当前的账本快照 (供绩效统计与测试) */
  def ledger: Ledger = state.ledger
