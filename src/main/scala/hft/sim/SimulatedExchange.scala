package hft.sim

import hft.domain.*
import hft.exchange.{AccountStream, ExchangeClient, MarketDataStream, SubscriptionKind}
import hft.messaging.{EventBus, EventData, IncomeEvent}
import org.slf4j.LoggerFactory
import ox.{Ox, fork}
import ox.channels.Channel

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

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
)

/** 虚拟柜台 / 模拟撮合引擎 —— 单 actor 实现。
  *
  * 完整扮演一个交易所的三个交互面 (REST 下单 / 公共行情流 / 私有账户流)，使策略
  * 对"实盘还是模拟盘"完全无感知：
  *   - [[MarketDataStream]]：消费上游真实公共行情 (实时, 用于撮合)，并把同样的行情
  *     按 exchangeToStrategyDelay 延迟后转发给策略
  *   - [[ExchangeClient]]：接收下单/撤单 (按 orderToExchangeDelay 延迟后到达撮合)，
  *     提供账户/仓位/挂单查询
  *   - [[AccountStream]]：撮合产生的订单回报/成交按 exchangeToStrategyDelay 延迟后回流策略
  *
  * 撮合规则：挂单成交判定为 **BBO 越过挂单价**，PostOnly 到达即可成交则拒单。撮合用上游
  * 实时行情、策略看延迟行情，如实建模"基于陈旧价格挂单、订单在途行情移动"。
  *
  * 线程模型 (actor)：所有命令 (行情到达 / 下单到达 / 撤单到达) 串行进入单一 mailbox，由
  * 唯一的处理线程消费——它是 [[SimState]] 的唯一写者，也是回流事件的唯一发布者。因此
  * "状态变更顺序 == 回流顺序"天然成立 (一张订单的 Pending 必早于其 Filled)，无需锁。
  * 延迟仅由 scheduler 负责"把命令/发布推迟到点"，不触碰状态。REST 查询读取 @volatile 的
  * 不可变状态快照 (单写多读, 无锁)。
  */
final class SimulatedExchange(
    market: MarketDataStream,
    publicClient: ExchangeClient,
    config: SimConfig = SimConfig(),
) extends ExchangeClient,
      MarketDataStream,
      AccountStream:
  require(market.exchange == publicClient.exchange, "market and publicClient must be the same exchange")
  private val logger = LoggerFactory.getLogger(classOf[SimulatedExchange])

  override def exchange: Exchange = publicClient.exchange

  /** 柜台内部命令：全部串行进入 mailbox */
  private enum Command:
    case Market(ev: IncomeEvent)          // 上游行情到达 (实时)
    case OrderArrived(order: Order, orderId: OrderId)
    case CancelArrived(orderId: OrderId)

  // ---- actor 基础设施 ----
  private val mailbox = Channel.unlimited[Command]
  /** 唯一写者 = actor 线程；读者 = REST 查询线程。不可变快照 + @volatile 保证可见性 */
  @volatile private var state: SimState = SimState.empty(config.initialBalanceUsdt)
  @volatile private var strategyBus: EventBus[IncomeEvent] = scala.compiletime.uninitialized

  private val orderIdSeq = AtomicLong(1)
  private val started = AtomicBoolean(false)
  private val rawBus = EventBus[IncomeEvent]()

  /** 延迟调度器：仅负责把命令/发布推迟到点 (不触碰状态)。daemon 单线程 */
  private val scheduler: ScheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor { r =>
      val t = Thread(r, "sim-exchange-scheduler"); t.setDaemon(true); t
    }

  /** 延迟 delayMs 后执行 action；delayMs <= 0 时同步执行 */
  private def after(delayMs: Long)(action: => Unit): Unit =
    if delayMs <= 0 then action
    else scheduler.schedule((() => action): Runnable, delayMs, TimeUnit.MILLISECONDS)

  /** 关闭调度线程 (测试清理用) */
  def shutdown(): Unit = scheduler.shutdownNow()

  // ==================== 生命周期 (同时实现 MarketDataStream / AccountStream.start) ====================

  /** 启动柜台。幂等：Engine 经 accountStream 与 marketData 两个角色各调用一次，只生效一次 */
  override def start(incomeBus: EventBus[IncomeEvent])(using Ox): Unit =
    require(strategyBus == null || (strategyBus eq incomeBus), "SimulatedExchange started with two different buses")
    strategyBus = incomeBus
    if started.compareAndSet(false, true) then
      // 上游真实行情发布到内部 rawBus；转发线程把行情即时投入 mailbox (撮合用实时行情)
      market.start(rawBus)
      val upstream = rawBus.subscribe()
      fork { while true do mailbox.send(Command.Market(upstream.receive())) }
      // actor 线程：串行消费命令, 是状态唯一写者与事件唯一发布者
      fork { while true do process(mailbox.receive()) }
      logger.info(
        s"SimulatedExchange started (exchange=$exchange, order->ex=${config.orderToExchangeDelayMs}ms, " +
          s"ex->strat=${config.exchangeToStrategyDelayMs}ms, initialBalance=${config.initialBalanceUsdt})"
      )

  override def subscribe(kinds: Set[SubscriptionKind]): Unit = market.subscribe(kinds)

  /** actor 主循环：纯转移 + 顺序发布 (在唯一线程上, 故全局有序) */
  private def process(cmd: Command): Unit =
    val (next, events) = cmd match
      case Command.Market(ev)             => state.onMarket(exchange, ev)
      case Command.OrderArrived(order, id) => state.onOrderArrived(exchange, order, id)
      case Command.CancelArrived(id)       => state.onCancelArrived(exchange, id)
    state = next
    events.foreach { ev =>
      ev.data match
        case EventData.FillUpdate(f) => logger.info(s"[SIM] fill ${f.side} ${f.symbol} qty=${f.size} @ ${f.price}")
        case _                       => ()
      deliver(ev)
    }

  /** 把交易所侧事件按 ex->strat 延迟投递给策略 */
  private def deliver(ev: IncomeEvent): Unit =
    after(config.exchangeToStrategyDelayMs) { strategyBus.publish(ev) }

  // ==================== ExchangeClient: 公共 REST (委托真实客户端) ====================

  override def fetchAllSymbolMetas(): Either[ExchangeError, Vector[SymbolMeta]] =
    publicClient.fetchAllSymbolMetas()

  // ==================== ExchangeClient: 私有 REST (模拟) ====================

  override def placeOrder(order: Order): Either[ExchangeError, OrderId] =
    val orderId = orderIdSeq.getAndIncrement().toString
    // 下单在途延迟后作为命令进入 mailbox
    after(config.orderToExchangeDelayMs) { mailbox.send(Command.OrderArrived(order, orderId)) }
    Right(orderId)

  override def cancelOrder(symbol: Symbol, orderId: OrderId): Either[ExchangeError, Unit] =
    // 撤单请求时订单已不在挂单簿 -> 模拟 Binance -2011 (已成交/已撤)，由 OutcomeProcessor 容忍。
    // 读快照判定；在途期间真撤由 CancelArrived 在 actor 线程内裁决 (届时成交则 remove 落空, 不再发 Cancelled)
    if !state.resting.contains(orderId) then Left(ExchangeError.Http(400, """{"code":-2011,"msg":"Unknown order sent."}"""))
    else
      after(config.orderToExchangeDelayMs) { mailbox.send(Command.CancelArrived(orderId)) }
      Right(())

  override def fetchPendingOrders(symbol: Symbol): Either[ExchangeError, Vector[OrderUpdate]] =
    val s = state
    Right(s.resting.values.filter(_.symbol == symbol).map { o =>
      OrderUpdate(o.orderId, Some(o.clientOrderId), exchange, o.symbol, o.side, OrderStatus.Pending, o.limitPrice, o.quantity, 0.0, 0.0, nowMs)
    }.toVector)

  override def setLeverage(symbol: Symbol, leverage: Int): Either[ExchangeError, Unit] = Right(())

  override def fetchAccountInfo(): Either[ExchangeError, AccountInfo] =
    val s = state
    Right(AccountInfo(equity = s.ledger.equity(s.markOf), notional = s.ledger.notional(s.markOf)))

  override def fetchPositions(): Either[ExchangeError, Vector[Position]] =
    val s = state
    Right(s.ledger.openPositions(s.markOf))
