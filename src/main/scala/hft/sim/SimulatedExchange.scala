package hft.sim

import hft.domain.*
import hft.exchange.{AccountStream, ExchangeClient, MarketDataStream, SubscriptionKind}
import hft.messaging.{EventBus, EventData, IncomeEvent}
import org.slf4j.LoggerFactory
import ox.{Ox, fork}

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import scala.collection.mutable

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

/** 虚拟柜台 / 模拟撮合引擎。
  *
  * 完整扮演一个交易所的三个交互面 (REST 下单 / 公共行情流 / 私有账户流)，使策略
  * 对"实盘还是模拟盘"完全无感知：
  *   - [[MarketDataStream]]：消费上游真实公共行情 (实时, 用于撮合)，并把同样的行情
  *     按 exchangeToStrategyDelay 延迟后转发给策略
  *   - [[ExchangeClient]]：接收下单/撤单 (按 orderToExchangeDelay 延迟后到达撮合)，
  *     提供账户/仓位/挂单查询
  *   - [[AccountStream]]：撮合产生的订单回报/成交按 exchangeToStrategyDelay 延迟后回流策略
  *
  * 撮合规则：挂单 (resting) 的成交判定为 **BBO 越过挂单价**——买单在最优卖价跌破其价位时
  * 成交、卖单在最优买价升破其价位时成交，成交价取挂单价 (maker 价)。
  * PostOnly 到达时已可成交则拒单 (不吃单)。撮合用的是上游实时行情，而策略看到的是延迟行情，
  * 因此"基于陈旧价格挂单、订单在途期间行情移动"的真实交互被如实建模。
  *
  * 线程模型：所有账户状态由 `lock` 串行化保护，可被上游行情线程 (撮合)、下单线程 (REST)、
  * 调度线程 (延迟到达) 并发访问。事件的延迟投递由单线程 scheduler 完成，保证同延迟事件 FIFO。
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

  // ==================== 延迟调度 ====================

  /** 单线程调度器：同延迟的事件按提交顺序投递 (保证行情/回报有序) */
  private val scheduler: ScheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor { r =>
      val t = Thread(r, "sim-exchange-scheduler"); t.setDaemon(true); t
    }

  /** 延迟 delayMs 后执行 action；delayMs <= 0 时在当前线程同步执行 (测试可确定性运行) */
  private def after(delayMs: Long)(action: => Unit): Unit =
    if delayMs <= 0 then action
    else scheduler.schedule((() => action): Runnable, delayMs, TimeUnit.MILLISECONDS)

  /** 关闭调度线程 (测试清理用；常驻运行无需调用) */
  def shutdown(): Unit = scheduler.shutdownNow()

  // ==================== 账户状态 (lock 保护) ====================

  private object lock
  /** 挂单簿: orderId -> 挂单 */
  private val resting = mutable.Map.empty[OrderId, RestingOrder]
  /** 账本 (仓位 + 现金)：纯不可变, lock 内整体替换 */
  private var ledger = Ledger.empty(config.initialBalanceUsdt)
  /** 最新实时行情 (撮合 + 估值用) */
  private val lastBbo = mutable.Map.empty[Symbol, BBO]
  private val lastMark = mutable.Map.empty[Symbol, Double]

  private val orderIdSeq = AtomicLong(1)
  private val started = AtomicBoolean(false)
  private val rawBus = EventBus[IncomeEvent]()
  @volatile private var strategyBus: EventBus[IncomeEvent] = scala.compiletime.uninitialized

  private final case class RestingOrder(
      orderId: OrderId,
      clientOrderId: String,
      symbol: Symbol,
      side: Side,
      limitPrice: Price,
      quantity: Quantity,
  )

  // ==================== 生命周期 (同时实现 MarketDataStream / AccountStream.start) ====================

  /** 启动柜台。幂等：Engine 经 accountStream 与 marketData 两个角色各调用一次，只生效一次 */
  override def start(incomeBus: EventBus[IncomeEvent])(using Ox): Unit =
    strategyBus = incomeBus
    if started.compareAndSet(false, true) then
      // 上游真实行情发布到内部 rawBus，柜台据此撮合并延迟转发给策略
      market.start(rawBus)
      val upstream = rawBus.subscribe()
      fork {
        while true do onUpstream(upstream.receive())
      }
      logger.info(
        s"SimulatedExchange started (exchange=$exchange, order->ex=${config.orderToExchangeDelayMs}ms, " +
          s"ex->strat=${config.exchangeToStrategyDelayMs}ms, initialBalance=${config.initialBalanceUsdt})"
      )

  override def subscribe(kinds: Set[SubscriptionKind]): Unit = market.subscribe(kinds)

  // ==================== 上游行情处理 ====================

  private def onUpstream(ev: IncomeEvent): Unit =
    ev.data match
      case EventData.BboUpdate(bbo) =>
        // 撮合用实时行情, 在行情转发给策略 **之前** 完成 (柜台先于策略看到价格)。
        // 投递排队在锁内进行: 让「锁获取顺序 == 投递顺序」, 使 Pending 永远先于其 Filled
        // 到达策略 (delay=0 内联发布与 delay>0 调度两条路径都成立)
        lock.synchronized {
          lastBbo(bbo.symbol) = bbo
          val fills = matchCrossing(bbo)
          deliver(ev)            // 行情先排队
          fills.foreach(deliver) // 成交回报随后
        }
      case EventData.MarkPriceUpdate(mp) =>
        lock.synchronized {
          lastMark(mp.symbol) = mp.price
          deliver(ev)
        }
      case _ => deliver(ev)

  /** 把交易所侧事件按 ex->strat 延迟投递给策略 */
  private def deliver(ev: IncomeEvent): Unit =
    after(config.exchangeToStrategyDelayMs) { strategyBus.publish(ev) }

  // ==================== 撮合 (均在 lock 内调用) ====================

  /** BBO 越过挂单价的全部挂单成交 (maker 成交价取挂单价)，返回回流事件 */
  private def matchCrossing(bbo: BBO): Vector[IncomeEvent] =
    val crossed = resting.values.filter(o => o.symbol == bbo.symbol && Matcher.crosses(o.side, o.limitPrice, bbo)).toVector
    crossed.flatMap { o =>
      resting.remove(o.orderId)
      fillEvents(o.orderId, o.clientOrderId, o.symbol, o.side, fillPrice = o.limitPrice, qty = o.quantity, ts = bbo.timestamp)
    }

  /** 成交：更新账本，构造 OrderUpdated(Filled) + FillUpdate */
  private def fillEvents(
      orderId: OrderId,
      clientOrderId: String,
      symbol: Symbol,
      side: Side,
      fillPrice: Price,
      qty: Quantity,
      ts: Timestamp,
  ): Vector[IncomeEvent] =
    ledger = ledger.applyFill(exchange, symbol, side, fillPrice, qty)
    logger.info(s"[SIM] fill $side $symbol qty=$qty @ $fillPrice (orderId=$orderId)")
    val update = OrderUpdate(
      orderId = orderId,
      clientOrderId = Some(clientOrderId),
      exchange = exchange,
      symbol = symbol,
      side = side,
      status = OrderStatus.Filled,
      price = fillPrice,
      quantity = qty,
      filledQuantity = qty,
      fillSize = qty,
      timestamp = ts,
    )
    val fill = Fill(exchange, symbol, side, fillPrice, qty, ts)
    Vector(
      IncomeEvent.at(ts, EventData.OrderUpdated(update)),
      IncomeEvent.at(ts, EventData.FillUpdate(fill)),
    )

  // ==================== 估值 (均在 lock 内调用) ====================

  /** 估值价格：优先标记价格，退化为 BBO 中间价 */
  private def markOf(symbol: Symbol): Double =
    lastMark.getOrElse(symbol, lastBbo.get(symbol).map(_.midPrice).getOrElse(0.0))

  // ==================== ExchangeClient: 公共 REST (委托真实客户端) ====================

  override def fetchAllSymbolMetas(): Either[ExchangeError, Vector[SymbolMeta]] =
    publicClient.fetchAllSymbolMetas()

  // ==================== ExchangeClient: 私有 REST (模拟) ====================

  override def placeOrder(order: Order): Either[ExchangeError, OrderId] =
    val orderId = orderIdSeq.getAndIncrement().toString
    // 下单在途延迟后到达撮合
    after(config.orderToExchangeDelayMs) { onOrderArrived(order, orderId) }
    Right(orderId)

  /** 订单到达撮合：按类型/TIF 决定 resting / 成交 / 拒单，事件延迟回流。
    * 投递排队在锁内 (与 onUpstream 一致), 保证回报相对行情/成交全局有序
    */
  private def onOrderArrived(order: Order, orderId: OrderId): Unit =
    lock.synchronized {
      val bboOpt = lastBbo.get(order.symbol)
      val ts = bboOpt.map(_.timestamp).getOrElse(nowMs)
      val events = order.orderType match
        case OrderType.Market =>
          bboOpt match
            case Some(bbo) =>
              val price = order.side match
                case Side.Long  => bbo.askPrice
                case Side.Short => bbo.bidPrice
              fillEvents(orderId, order.clientOrderId, order.symbol, order.side, price, order.quantity, ts)
            case None =>
              Vector(statusEvent(order, orderId, OrderStatus.Rejected("no market data for market order"), 0.0, ts))
        case OrderType.Limit(limit, tif) =>
          // 到达即可成交 (marketable) 与 resting 越价用同一判定, 二者自洽
          val marketable = bboOpt.exists(Matcher.crosses(order.side, limit, _))
          def takerFill = fillEvents(orderId, order.clientOrderId, order.symbol, order.side, Matcher.touchPrice(order.side, bboOpt.get), order.quantity, ts)
          tif match
            case TimeInForce.PostOnly =>
              if marketable then Vector(statusEvent(order, orderId, OrderStatus.Rejected("post-only would take liquidity"), limit, ts))
              else restOrder(order, orderId, limit, ts)
            case TimeInForce.GTC =>
              if marketable then takerFill else restOrder(order, orderId, limit, ts)
            case TimeInForce.IOC | TimeInForce.FOK =>
              // 无深度模型, 可成交即全量成交, 否则整单取消 (不 resting)
              if marketable then takerFill else Vector(statusEvent(order, orderId, OrderStatus.Cancelled, limit, ts))
      events.foreach(deliver)
    }

  private def restOrder(order: Order, orderId: OrderId, limit: Price, ts: Timestamp): Vector[IncomeEvent] =
    resting(orderId) = RestingOrder(orderId, order.clientOrderId, order.symbol, order.side, limit, order.quantity)
    Vector(statusEvent(order, orderId, OrderStatus.Pending, limit, ts))

  private def statusEvent(order: Order, orderId: OrderId, status: OrderStatus, price: Price, ts: Timestamp): IncomeEvent =
    IncomeEvent.at(
      ts,
      EventData.OrderUpdated(
        OrderUpdate(
          orderId = orderId,
          clientOrderId = Some(order.clientOrderId),
          exchange = exchange,
          symbol = order.symbol,
          side = order.side,
          status = status,
          price = price,
          quantity = order.quantity,
          filledQuantity = 0.0,
          fillSize = 0.0,
          timestamp = ts,
        )
      ),
    )

  override def cancelOrder(symbol: Symbol, orderId: OrderId): Either[ExchangeError, Unit] =
    // 撤单请求时订单已不在挂单簿 -> 模拟 Binance -2011 (已成交/已撤)，由 OutcomeProcessor 容忍
    val exists = lock.synchronized { resting.contains(orderId) }
    if !exists then Left(ExchangeError.Http(400, """{"code":-2011,"msg":"Unknown order sent."}"""))
    else
      after(config.orderToExchangeDelayMs) {
        // 在途期间可能已成交, 届时 remove 落空 -> 不再发 Cancelled (终态已由成交回报给出)
        lock.synchronized {
          resting.remove(orderId).foreach { o =>
            deliver(
              IncomeEvent.at(
                nowMs,
                EventData.OrderUpdated(
                  OrderUpdate(orderId, Some(o.clientOrderId), exchange, o.symbol, o.side, OrderStatus.Cancelled, o.limitPrice, o.quantity, 0.0, 0.0, nowMs)
                ),
              )
            )
          }
        }
      }
      Right(())

  override def fetchPendingOrders(symbol: Symbol): Either[ExchangeError, Vector[OrderUpdate]] =
    Right(lock.synchronized {
      resting.values.filter(_.symbol == symbol).map { o =>
        OrderUpdate(o.orderId, Some(o.clientOrderId), exchange, o.symbol, o.side, OrderStatus.Pending, o.limitPrice, o.quantity, 0.0, 0.0, nowMs)
      }.toVector
    })

  override def setLeverage(symbol: Symbol, leverage: Int): Either[ExchangeError, Unit] = Right(())

  override def fetchAccountInfo(): Either[ExchangeError, AccountInfo] =
    Right(lock.synchronized {
      AccountInfo(equity = ledger.equity(markOf), notional = ledger.notional(markOf))
    })

  override def fetchPositions(): Either[ExchangeError, Vector[Position]] =
    Right(lock.synchronized(ledger.openPositions(markOf)))
