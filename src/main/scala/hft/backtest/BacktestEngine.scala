package hft.backtest

import hft.domain.*
import hft.engine.StrategyRunner
import hft.event.Commands.{OrderIntent, OutcomeEvent}
import hft.event.{AnyEvent, Event, Topics}
import hft.exchange.TradingGateway
import hft.sim.{Counter, CounterInput, Delayed, SimConfig, SimState}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.control.NonFatal

/** 回测结果汇总。realizedPnl 为已实现盈亏 (= 账本现金增量)，finalEquity 含未实现。 */
final case class BacktestResult(
    initialBalance: Double,
    finalEquity: Double,
    realizedPnl: Double,
    fills: Int,
    marketEvents: Long,
    /** 数据源违反"时间戳升序"契约的事件数 (已被钳制到当前虚拟时间)。>0 说明数据源有乱序，
      * 回测仍确定但值得查源头。 */
    outOfOrderEvents: Long,
    positions: Vector[Position],
    firstTs: Timestamp,
    lastTs: Timestamp,
)

/** 虚拟时间回测引擎 —— 单线程、确定性。
  *
  * 与实盘/模拟盘共享**全部领域逻辑**：撮合用纯状态机 [[SimState]] (沿用 BBO 越价撮合)、
  * 策略步用 [[StrategyRunner]] (与 Executor 同一份)、账本用 [[hft.sim.Ledger]]。差异只在驱动层——
  * 实盘是并发虚拟线程 + 墙钟延迟，回测是单线程 + 虚拟时间优先队列：把"延迟 D 后做某事"
  * 实现为"入队到 now+D"。同一输入必得同一结果。
  *
  * 时间推进：行情事件来自 [[MarketDataSource]] (已全局时间有序)，按其 exchangeTs 注入；
  * 注入/撮合产生的回流按 `exchangeToStrategyDelayMs` 入队投递给策略，策略下单按
  * `orderToExchangeDelayMs` 入队到达撮合。主循环始终处理"源 peek 与队列 peek 中较早者"，
  * 同刻先排空队列 (先消化既有效果再注入新行情)。
  *
  * 账户净值：周期性 (clockIntervalMs) 由当前账本计算账户信息事件投递给策略 (等价实盘
  * Engine 的 accountRefresh)，并在首个事件时先投递一次初始净值，否则依赖净值的策略不会动作。
  *
  * 时间一致性：撮合回报与 pending order 的 createdAt 一律取虚拟时间 `now` (不读墙钟)，故
  * 逐笔回报时间戳、订单超时检测 (failOnTimedOutOrders) 在回测中与虚拟时间一致——同一输入
  * 必得同一结果。
  */
final class BacktestEngine(
    exchange: Exchange,
    source: MarketDataSource,
    runners: Seq[StrategyRunner],
    config: SimConfig = SimConfig(),
    /** 合约规格：回测里引擎兼任柜台，据此把策略的币本位意图对齐到交易所精度 ——
      * 与实盘的柜台同一份判据 (见 [[hft.exchange.TradingGateway]])，否则回测的成交量会
      * 系统性地比实盘多出一个取整。
      * 无默认值 —— 缺了它任何订单都对齐不了，与其在首笔下单时炸，不如装配期就写清楚。 */
    symbolMetas: Map[(Exchange, Symbol), SymbolMeta],
    observers: Seq[AnyEvent => Unit] = Nil,
    clockIntervalMs: Long = 1000,
    /** 本次回测的账户。撮合发出的回报标它，策略的私有回报订阅也按它路由 —— 两边必须一致，
      * 否则策略收不到自己的成交且毫无症状，故在此校验而非各写一个常量。 */
    account: AccountId = AccountId.Live,
):
  // 装配期校验, 不留给运行时静默失效
  private val mismatched = runners.filterNot(_.account == account)
  require(
    mismatched.isEmpty,
    s"backtest account is $account but ${mismatched.size} runner(s) bound to ${mismatched.map(_.account).distinct.mkString(",")}; " +
      "private reports are routed by (account, instrument) — a mismatch silently starves the strategy of its own fills",
  )

  private val logger = LoggerFactory.getLogger(classOf[BacktestEngine])

  /** 队列内的延迟动作。撮合命令统一用 [[CounterInput]]（与实盘替身、影子盘同一份），
    * 这里只多两个回测独有的：把事件投给策略、时钟节拍。 */
  private enum Action:
    case Deliver(ev: AnyEvent)      // 交易所侧事件到达策略/观察者
    case Match(input: CounterInput) // 下单/撤单在途结束, 抵达撮合
    case Clock

  private final case class Scheduled(time: Timestamp, seq: Long, action: Action)

  // 最小堆：先按时间、同刻按 seq (入队序) -> 完全确定。
  // 自定义比较 (不用 Ordering.by(tuple)) 避免每次堆比较都分配 Tuple2 + 装箱两个 Long ——
  // 月级回测堆操作上千万次, 这是热路径 GC 大头。语义等价原 `.reverse` (PriorityQueue 弹最大,
  // 故 compare 取反使最小 (time,seq) 先出)。
  private given Ordering[Scheduled] with
    def compare(a: Scheduled, b: Scheduled): Int =
      val byTime = java.lang.Long.compare(b.time, a.time)
      if byTime != 0 then byTime else java.lang.Long.compare(b.seq, a.seq)
  private val pq = mutable.PriorityQueue.empty[Scheduled]

  private var state: SimState = SimState.empty(account, config.initialBalanceUsdt, config.makerFeeRate, config.takerFeeRate)
  private var now: Timestamp = 0L
  private var seqGen: Long = 0L
  private var orderIdGen: Long = 0L
  private var fillCount: Int = 0
  private var marketEvents: Long = 0L
  private var firstTs: Timestamp = 0L
  private var moreData: Boolean = true // 源是否仍有行情待注入 (决定时钟是否续期, 保证终止)
  private var outOfOrderEvents: Long = 0L // 源违反升序契约的条数 (被钳制, 见 ingest)

  private def schedule(time: Timestamp, action: Action): Unit =
    seqGen += 1
    pq.enqueue(Scheduled(time, seqGen, action))

  /** 跑完整个数据源，返回汇总结果。 */
  def run(): BacktestResult =
    val src = source.events().buffered
    if !src.hasNext then
      logger.warn("no market data; empty backtest")
      return BacktestResult(config.initialBalanceUsdt, config.initialBalanceUsdt, 0.0, 0, 0, 0, Vector.empty, 0, 0)

    firstTs = src.head.exchangeTs
    now = firstTs
    deliver(accountInfoEvent(now)) // 初始净值, 让依赖 equity 的策略可启动
    schedule(now + clockIntervalMs, Action.Clock)

    while src.hasNext || pq.nonEmpty do
      moreData = src.hasNext
      val srcTs = if src.hasNext then Some(src.head.exchangeTs) else None
      val pqTs = pq.headOption.map(_.time)
      (srcTs, pqTs) match
        case (Some(st), Some(pt)) => if pt <= st then runQueued() else ingest(src.next())
        case (Some(_), None)      => ingest(src.next())
        case (None, Some(_))      => runQueued()
        case (None, None)         => () // 循环条件已排除

    val result = BacktestResult(
      initialBalance = config.initialBalanceUsdt,
      finalEquity = state.ledger.equity(state.markOf),
      realizedPnl = state.ledger.cash - config.initialBalanceUsdt,
      fills = fillCount,
      marketEvents = marketEvents,
      outOfOrderEvents = outOfOrderEvents,
      positions = state.ledger.openPositions(state.markOf),
      firstTs = firstTs,
      lastTs = now,
    )
    logger.info(
      s"backtest done: events=$marketEvents fills=$fillCount realizedPnl=${result.realizedPnl} finalEquity=${result.finalEquity}"
    )
    if outOfOrderEvents > 0 then
      logger.warn(
        s"market data source violated ascending-timestamp contract on $outOfOrderEvents/$marketEvents events; " +
          "they were clamped to the current virtual time (results stay deterministic, but check the source)"
      )
    result
  end run

  /** 落地一次撮合转移：新状态 + 按各自延迟入队的回报。
    * 延迟由 [[Counter]] 决定（与实盘替身、影子盘同一份），这里只负责"在虚拟时间里怎么等"。 */
  private def applyMatching(transfer: (SimState, Vector[Delayed[AnyEvent]])): Unit =
    val (next, replies) = transfer
    state = next
    replies.foreach(emit)

  /** 兑现一条延迟输出：入队到 now + delay */
  private def emit(out: Delayed[AnyEvent]): Unit =
    schedule(now + out.delayMs, Action.Deliver(out.value))

  /** 兑现一条延迟输入：下单/撤单在途结束后抵达撮合 */
  private def enqueue(cmd: Delayed[CounterInput]): Unit =
    schedule(now + cmd.delayMs, Action.Match(cmd.value))

  private def matchNow(input: CounterInput): Unit =
    applyMatching(Counter.step(state, exchange, input, now, config))

  /** 注入一条历史行情：推进时间、撮合、回流按延迟入队投递。
    *
    * 时间钳制：[[MarketDataSource]] 的契约是升序，但真实数据会破例 (如币安 bookTicker 文件内
    * 偶有毫秒级乱序)。虚拟时间一旦倒流，优先队列里"未来"的动作就会晚于它本该早于的事件执行，
    * 整个因果顺序作废。故取 max —— 乱序事件按当前时刻处理 (等价视作同刻)，并计数，跑完告警。
    *
    * 钳制的只是**虚拟时间**：该事件仍会以自己的 payload 更新行情快照，即一条迟到的旧盘口会
    * 覆盖较新的那条。毫秒级乱序下可忽略，但别指望钳制顺带修好了行情快照的新旧顺序。
    */
  private def ingest(ev: AnyEvent): Unit =
    if ev.exchangeTs < now then outOfOrderEvents += 1
    else now = ev.exchangeTs
    marketEvents += 1
    // 回测里引擎就是网关, 行情要自己转发给策略 (撮合不回显, 见 SimState.onMarket)。
    // 先发行情后发成交: 同刻入队, 递增的 seq 保证策略看到的"行情先于它引发的成交"。
    emit(Counter.toStrategy(config, ev))
    matchNow(CounterInput.Market(ev))

  private def runQueued(): Unit =
    val s = pq.dequeue()
    now = s.time
    s.action match
      case Action.Deliver(ev)   => deliver(ev)
      case Action.Match(input)  => matchNow(input)
      case Action.Clock =>
        deliver(Topics.clockAt(now))
        deliver(accountInfoEvent(now)) // 周期刷新净值, 等价实盘 Engine 的 accountRefresh
        // 仅在仍有行情待注入时续期, 源耗尽则停摆让队列自然排空 -> 保证终止
        if moreData then schedule(now + clockIntervalMs, Action.Clock)

  /** 把事件投递给观察者与各策略；策略产出的信号按下单延迟入队到达撮合。 */
  private def deliver(ev: AnyEvent): Unit =
    if ev.is(Topics.Fill) then fillCount += 1
    // 旁路观察者隔离: 观察者 (出图/记录等) 自身异常绝不拖垮回测核心, 只 warn 后继续
    observers.foreach { obs =>
      try obs(ev)
      catch case NonFatal(e) => logger.warn(s"backtest observer failed on event (ignored): ${e.getMessage}", e)
    }
    runners.foreach { r =>
      if r.accepts(ev) then
        r.onEvent(ev, now).foreach { produced =>
          // 策略产出的可以是下单意图, 也可以是它自己的指标事件。前者进撮合, 后者只投给观察者
          produced.as(OrderIntent) match
            case Some(intent) =>
              intent.outcome match
                case OutcomeEvent.PlaceOrders(orders, _) =>
                  // 回测里引擎兼任柜台：精度对齐与"收不下就拒单回流"都走实盘那一份判据，
                  // 否则策略在回测里发得出、在实盘发不出的单会悄悄改变结论。
                  orders.foreach { o =>
                    OrderConversion.alignToExchange(o, metaOf(o)) match
                      case Right(aligned) =>
                        orderIdGen += 1
                        enqueue(Counter.inbound(config, CounterInput.OrderArrived(aligned, orderIdGen.toString)))
                      case Left(reason) =>
                        logger.warn(s"下单被交易所精度拒绝: $reason")
                        schedule(now, Action.Deliver(TradingGateway.rejection(account, exchange, o, reason)))
                  }
                case OutcomeEvent.CancelOrder(_, _, ref) =>
                  enqueue(Counter.inbound(config, CounterInput.CancelArrived(ref)))
            case None =>
              // 自定义事件 (如策略指标): 回测里同样按虚拟时间投递, 观察者与其他 runner 都能收到
              schedule(now, Action.Deliver(produced))
        }
    }

  /** 缺规格即策略引用了未装配的标的, 是装配错误, 立即终止 */
  private def metaOf(order: Order): SymbolMeta =
    symbolMetas.getOrElse(
      (order.exchange, order.symbol),
      sys.error(s"回测缺少 ${order.exchange} ${order.symbol} 的合约规格, 无法对齐订单"),
    )

  private def accountInfoEvent(ts: Timestamp): AnyEvent =
    Event.stamped(Topics.AccountInfo, state.accountInfo(exchange), ts, ts)
