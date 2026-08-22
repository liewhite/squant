package hft.sim

import hft.domain.*
import hft.exchange.{AccountStream, ExchangeClient, MarketDataStream, SubscriptionKind}
import hft.event.{AnyEvent, EventBus, Interest, Topics}
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
    /** maker 手续费率 (resting 单被越价成交)，0.0002 = 0.02%。默认 0 = 不计费 */
    makerFeeRate: Double = 0.0,
    /** taker 手续费率 (到达即吃单成交)，0.0005 = 0.05%。默认 0 = 不计费 */
    takerFeeRate: Double = 0.0,
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
    /** 本柜台服务的账户。作为实盘替身时是 [[AccountId.Live]] (策略对真假无感知)；
      * 与实盘并行跑影子盘时是 `Paper(n)`，两边的回报靠这个维度分开。
      * 无默认值，理由同 [[hft.engine.Executor]] */
    account: AccountId,
) extends ExchangeClient,
      MarketDataStream,
      AccountStream:
  require(market.exchange == publicClient.exchange, "market and publicClient must be the same exchange")
  private val logger = LoggerFactory.getLogger(classOf[SimulatedExchange])

  override def exchange: Exchange = publicClient.exchange

  /** 柜台内部命令：全部串行进入 mailbox */
  private enum Command:
    case Market(ev: AnyEvent)          // 上游行情到达 (实时)
    case OrderArrived(order: Order, orderId: OrderId)
    case CancelArrived(ref: OrderRef)

  // ---- actor 基础设施 ----
  private val mailbox = Channel.unlimited[Command]
  /** 唯一写者 = actor 线程；读者 = REST 查询线程。不可变快照 + @volatile 保证可见性 */
  @volatile private var state: SimState = SimState.empty(account, config.initialBalanceUsdt, config.makerFeeRate, config.takerFeeRate)
  @volatile private var strategyBus: EventBus = scala.compiletime.uninitialized

  private val orderIdSeq = AtomicLong(1)
  private val started = AtomicBoolean(false)
  private val rawBus = EventBus()

  /** 延迟调度器：仅负责把命令/发布推迟到点 (不触碰状态)。
    * **单线程是顺序保证的承重墙**——等延迟事件按提交序 FIFO 投递, 把 actor 的输出序原样透过延迟传出;
    * 勿改为多线程 (会破坏等延迟事件的投递序)。daemon, 进程退出即回收 */
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
  override def start(eventBus: EventBus)(using Ox): Unit =
    require((strategyBus eq null) || (strategyBus eq eventBus), "SimulatedExchange started with two different buses")
    strategyBus = eventBus
    if started.compareAndSet(false, true) then
      // 上游真实行情发布到内部 rawBus；转发线程把行情即时投入 mailbox (撮合用实时行情)
      market.start(rawBus)
      // 只订公共行情：柜台撮合的输入就是行情，别的 topic 与它无关
      val upstream = rawBus.subscribe(Topics.market.map(Interest.All.apply))
      fork { while true do mailbox.send(Command.Market(upstream.events.receive())) }
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
      case Command.Market(ev)             => state.onMarket(exchange, ev, nowMs)
      case Command.OrderArrived(order, id) => state.onOrderArrived(exchange, order, id, nowMs)
      case Command.CancelArrived(ref)      => state.onCancelArrived(exchange, ref, nowMs)
    state = next
    events.foreach { ev =>
      ev.as(Topics.Fill).foreach(f => logger.info(s"[SIM] fill ${f.side} ${f.symbol} qty=${f.size} @ ${f.price}"))
      deliver(ev)
    }

  /** 把交易所侧事件按 ex->strat 延迟投递给策略 */
  private def deliver(ev: AnyEvent): Unit =
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

  override def cancelOrder(symbol: Symbol, ref: OrderRef): Either[ExchangeError, Unit] =
    // 撤单请求时订单已不在挂单簿 -> OrderNotFound (已成交/已撤)，由 OutcomeProcessor 容忍。
    // 读快照判定；在途期间真撤由 CancelArrived 在 actor 线程内裁决 (届时成交则 remove 落空, 不再发 Cancelled)
    if state.findResting(ref).isEmpty then Left(ExchangeError.OrderNotFound(s"order ${ref.raw} not in book"))
    else
      after(config.orderToExchangeDelayMs) { mailbox.send(Command.CancelArrived(ref)) }
      Right(())

  override def fetchPendingOrders(symbol: Symbol): Either[ExchangeError, Vector[OrderUpdate]] =
    val s = state
    Right(s.resting.values.filter(_.symbol == symbol).map { o =>
      OrderUpdate(account, o.orderId, Some(o.clientOrderId), exchange, o.symbol, o.side, OrderStatus.Pending, o.limitPrice, o.quantity, 0.0, 0.0, nowMs)
    }.toVector)

  override def setLeverage(symbol: Symbol, leverage: Int): Either[ExchangeError, Unit] = Right(())

  override def fetchAccountInfo(): Either[ExchangeError, AccountInfo] =
    val s = state
    Right(AccountInfo(account, exchange, equity = s.ledger.equity(s.markOf), notional = s.ledger.notional(s.markOf)))

  override def fetchPositions(): Either[ExchangeError, Vector[Position]] =
    val s = state
    Right(s.ledger.openPositions(s.markOf))
