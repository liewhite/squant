package hft.backtest

import hft.domain.*
import hft.engine.StrategyRunner
import hft.event.{AnyEvent, Event, Topics}
import hft.sim.{SimConfig, SimState}
import hft.strategy.{OrderIntent, OutcomeEvent}
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
    /** 合约规格：撮合入口据此把订单从交易所格式还原成币本位 (见 [[hft.sim.SimState.onOrderArrived]])。
      * 无默认值 —— 缺了它任何订单都撮合不了，与其在首笔成交时炸，不如装配期就写清楚。 */
    symbolMetas: Map[(Exchange, Symbol), SymbolMeta],
    observers: Seq[AnyEvent => Unit] = Nil,
    clockIntervalMs: Long = 1000,
):
  private val logger = LoggerFactory.getLogger(classOf[BacktestEngine])

  /** 队列内的延迟动作 */
  private enum Action:
    case Deliver(ev: AnyEvent) // 交易所侧事件到达策略/观察者
    case OrderArrive(order: Order, orderId: OrderId)
    case CancelArrive(ref: OrderRef)
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

  private var state: SimState = SimState.empty(AccountId.Live, symbolMetas, config.initialBalanceUsdt, config.makerFeeRate, config.takerFeeRate)
  private var now: Timestamp = 0L
  private var seqGen: Long = 0L
  private var orderIdGen: Long = 0L
  private var fillCount: Int = 0
  private var marketEvents: Long = 0L
  private var firstTs: Timestamp = 0L
  private var moreData: Boolean = true // 源是否仍有行情待注入 (决定时钟是否续期, 保证终止)

  private def schedule(time: Timestamp, action: Action): Unit =
    seqGen += 1
    pq.enqueue(Scheduled(time, seqGen, action))

  /** 跑完整个数据源，返回汇总结果。 */
  def run(): BacktestResult =
    val src = source.events().buffered
    if !src.hasNext then
      logger.warn("no market data; empty backtest")
      return BacktestResult(config.initialBalanceUsdt, config.initialBalanceUsdt, 0.0, 0, 0, Vector.empty, 0, 0)

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
      positions = state.ledger.openPositions(state.markOf),
      firstTs = firstTs,
      lastTs = now,
    )
    logger.info(
      s"backtest done: events=$marketEvents fills=$fillCount realizedPnl=${result.realizedPnl} finalEquity=${result.finalEquity}"
    )
    result
  end run

  /** 应用一次撮合转移：落地新状态，并把回流事件按 ex->strat 延迟入队投递给策略。
    * 三个撮合入口 (行情注入 / 下单到达 / 撤单到达) 的唯一公共形态——与实盘
    * [[hft.sim.SimulatedExchange.process]] 同构 (那里是 actor 串行, 这里是虚拟时间入队)。
    */
  private def applyMatching(transfer: (SimState, Vector[AnyEvent])): Unit =
    val (next, replies) = transfer
    state = next
    replies.foreach(r => schedule(now + config.exchangeToStrategyDelayMs, Action.Deliver(r)))

  /** 注入一条历史行情：推进时间、撮合、回流按延迟入队投递。 */
  private def ingest(ev: AnyEvent): Unit =
    now = ev.exchangeTs
    marketEvents += 1
    applyMatching(state.onMarket(exchange, ev, now))

  private def runQueued(): Unit =
    val s = pq.dequeue()
    now = s.time
    s.action match
      case Action.Deliver(ev)            => deliver(ev)
      case Action.OrderArrive(order, id) => applyMatching(state.onOrderArrived(exchange, order, id, now))
      case Action.CancelArrive(ref)      => applyMatching(state.onCancelArrived(exchange, ref, now))
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
                  orders.foreach { o =>
                    orderIdGen += 1
                    schedule(now + config.orderToExchangeDelayMs, Action.OrderArrive(o, orderIdGen.toString))
                  }
                case OutcomeEvent.CancelOrder(_, _, ref) =>
                  schedule(now + config.orderToExchangeDelayMs, Action.CancelArrive(ref))
            case None =>
              // 自定义事件 (如策略指标): 回测里同样按虚拟时间投递, 观察者与其他 runner 都能收到
              schedule(now, Action.Deliver(produced))
        }
    }

  private def accountInfoEvent(ts: Timestamp): AnyEvent =
    val info = AccountInfo(AccountId.Live, exchange, equity = state.ledger.equity(state.markOf), notional = state.ledger.notional(state.markOf))
    Event.stamped(Topics.AccountInfo, info, ts, ts)
