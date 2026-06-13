package hft.engine

import hft.domain.*
import hft.exchange.{AccountStream, ExchangeClient, MarketDataStream, SubscriptionKind}
import hft.messaging.{EventBus, EventData, IncomeEvent}
import hft.strategy.{OutcomeEvent, Strategy}
import org.slf4j.LoggerFactory
import ox.{Ox, fork}
import ox.channels.Source

/** 一个交易所的接入单元: REST 客户端 + 公共行情流 + 可选私有账户流。
  *
  * 私有账户流可缺省 (无凭证的研究模式)，也可由虚拟柜台 (模拟盘) 提供——
  * 此时 client / marketData / accountStream 可指向同一个 SimulatedExchange，
  * 策略对真实还是模拟无感知。
  */
final case class ExchangeGateway(
    client: ExchangeClient,
    marketData: MarketDataStream,
    accountStream: Option[AccountStream] = None,
)

/** 引擎：装配并管理所有组件的生命周期。
  *
  * 事件流：
  * {{{
  * Connector (WS) ──┐
  * Clock ───────────┼─> incomeBus ─> Executor (Strategy + StateManager) ─> outcomeBus ─> OutcomeProcessor ─> REST
  * 执行回流 <────────┘                                                                          │
  *    └──────────────────────────────── OrderUpdate (撤单确认/下单失败) <───────────────────────┘
  * }}}
  *
  * 所有组件都是引擎所在 Ox 作用域内的虚拟线程 fork，任一组件崩溃将级联终止整个作用域，
  * 对应参考实现中 spawn_link + 级联退出的监督语义。
  */
final class Engine private (
    clients: Map[Exchange, ExchangeClient],
    marketStreams: Map[Exchange, MarketDataStream],
    symbolMetas: Map[(Exchange, Symbol), SymbolMeta],
    incomeBus: EventBus[IncomeEvent],
    outcomeBus: EventBus[OutcomeEvent],
)(using Ox):
  private val logger = LoggerFactory.getLogger(classOf[Engine])

  /** 订阅 income 总线 (只读观察)，用于在策略**之外**消费事件 (如成交记录、监控)，
    * 策略因此无需承担写文件等副作用。返回独立 Source；调用方负责在自己的作用域内 fork 消费。
    * 须在关心的事件产生前订阅 (通常紧随 Engine.start、addStrategy 之前)。
    */
  def subscribeIncome(): Source[IncomeEvent] = incomeBus.subscribe()

  def addStrategy(strategy: Strategy): Unit = addStrategies(Vector(strategy))

  /** 批量添加策略。
    *
    * 启动顺序保证 (与参考实现一致)：
    *   1. 创建 Executor 并订阅 income 总线 —— 之后发布的事件不会丢
    *   2. REST 查询初始持仓并发布 —— 避免策略基于缺失仓位决策；
    *      交易所未返回的 symbol 显式推 size=0，保证 SymbolState 一定收到初始值
    *   3. REST 查询账户信息 (净值/名义价值) 并发布 —— 风控类决策 (杠杆率) 依赖
    *   4. REST 查询现有挂单并发布 —— 策略接管启动前的遗留订单
    *   5. 向交易所订阅行情 —— 市场数据从此处开始流动
    */
  def addStrategies(strategies: Seq[Strategy]): Unit =
    if strategies.isEmpty then return

    // 1. 创建 Executor，订阅 income 总线
    strategies.foreach { strategy =>
      Executor(strategy, symbolMetas, outcomeBus).run(incomeBus.subscribe())
    }

    // 收集所有策略涉及的订阅
    val allSubscriptions: Set[(Exchange, SubscriptionKind)] =
      strategies.toSet.flatMap { (s: Strategy) =>
        s.publicStreams.toSet.flatMap { (exchange, kinds) => kinds.map((exchange, _)) }
      }
    val exchangeSymbols: Set[(Exchange, Symbol)] =
      allSubscriptions.map((exchange, kind) => (exchange, kind.subscribedSymbol))

    // 2. 初始持仓
    publishInitialPositions(exchangeSymbols)

    // 3. 初始账户信息
    exchangeSymbols.map(_._1).foreach(exchange => publishAccountInfoFrom(requireClient(exchange)))

    // 4. 现有挂单
    publishExistingPendingOrders(exchangeSymbols)

    // 5. 订阅行情
    allSubscriptions.groupMap(_._1)(_._2).foreach { (exchange, kinds) =>
      marketStreams
        .getOrElse(exchange, throw IllegalStateException(s"No market data stream configured for exchange $exchange"))
        .subscribe(kinds)
    }

    logger.info(s"${strategies.size} strategies added")

  private def requireClient(exchange: Exchange): ExchangeClient =
    clients.getOrElse(exchange, throw IllegalStateException(s"No client configured for exchange $exchange"))

  /** REST 查询账户信息并发布 AccountInfoUpdate。
    * 返回 false 表示该交易所未配置凭证 (无账户即无需刷新，确定安全)；其余失败致命
    */
  private def publishAccountInfoFrom(client: ExchangeClient): Boolean =
    client.fetchAccountInfo() match
      case Right(info) =>
        incomeBus.publish(IncomeEvent.local(EventData.AccountInfoUpdate(client.exchange, info)))
        true
      case Left(ExchangeError.Auth(_)) =>
        logger.info(s"No credentials for ${client.exchange}, skipping account info")
        false
      case Left(e) =>
        throw IllegalStateException(s"Failed to fetch account info from ${client.exchange}: ${e.message}")

  /** 启动对齐必须成功 (fail-fast)，唯一的例外是未配置凭证：
    * 无账户即无仓位/挂单需要对齐，跳过是确定安全的 (公开数据演示/研究模式)
    */
  private def publishInitialPositions(exchangeSymbols: Set[(Exchange, Symbol)]): Unit =
    exchangeSymbols.groupMap(_._1)(_._2).foreach { (exchange, symbols) =>
      requireClient(exchange).fetchPositions() match
        case Left(ExchangeError.Auth(_)) =>
          logger.info(s"No credentials for $exchange, skipping position alignment")
        case Left(e) =>
          throw IllegalStateException(s"Failed to fetch initial positions from $exchange: ${e.message}")
        case Right(positions) =>
          val bySymbol = positions.map(p => p.symbol -> p).toMap
          symbols.foreach { symbol =>
            val pos = bySymbol.getOrElse(symbol, Position.empty(exchange, symbol))
            logger.info(s"Initial position loaded: $exchange $symbol size=${pos.size}")
            incomeBus.publish(IncomeEvent.local(EventData.PositionUpdate(pos)))
          }
    }

  private def publishExistingPendingOrders(exchangeSymbols: Set[(Exchange, Symbol)]): Unit =
    exchangeSymbols.foreach { (exchange, symbol) =>
      requireClient(exchange).fetchPendingOrders(symbol) match
        case Left(ExchangeError.Auth(_)) =>
          logger.info(s"No credentials for $exchange, skipping pending order alignment")
        case Left(e) =>
          throw IllegalStateException(s"Failed to fetch pending orders from $exchange $symbol: ${e.message}")
        case Right(updates) =>
          if updates.nonEmpty then
            logger.info(s"Fetched ${updates.size} existing pending orders: $exchange $symbol")
          updates.foreach { update =>
            // REST 返回的数量是合约张数，转换为币本位
            val meta = symbolMetas.getOrElse(
              (exchange, symbol),
              throw IllegalStateException(s"SymbolMeta not found for $exchange $symbol"),
            )
            val converted = update.copy(
              quantity = meta.qtyToCoin(update.quantity),
              filledQuantity = meta.qtyToCoin(update.filledQuantity),
            )
            incomeBus.publish(IncomeEvent.local(EventData.OrderUpdated(converted)))
          }
    }

object Engine:
  private val logger = LoggerFactory.getLogger(classOf[Engine])

  /** 启动引擎：预加载交易对元数据 (失败即终止启动)、装配事件总线、
    * 启动信号处理器 / 时钟 / 账户信息刷新 / 各交易所连接器。
    *
    * @param accountRefreshMs 账户信息 (净值/名义价值) REST 刷新间隔；
    *                         净值随行情持续变动，无对应 WS 推送，周期拉取保证风控数据新鲜
    */
  def start(
      gateways: Seq[ExchangeGateway],
      dryRun: Boolean = false,
      clockIntervalMs: Long = 1000,
      accountRefreshMs: Long = 10_000,
  )(using Ox): Engine =
    val clients = gateways.map(g => g.client.exchange -> g.client).toMap
    val marketStreams = gateways.map(g => g.marketData.exchange -> g.marketData).toMap
    val accountStreams = gateways.flatMap(g => g.accountStream.map(a => a.exchange -> a)).toMap

    // 预加载所有交易所的 symbol metas，任一失败 → 启动失败快速退出
    val symbolMetas: Map[(Exchange, Symbol), SymbolMeta] =
      clients.values.flatMap { client =>
        client.fetchAllSymbolMetas() match
          case Right(metas) =>
            logger.info(s"Preloaded ${metas.size} symbol metas from ${client.exchange}")
            metas.map(m => (m.exchange, m.symbol) -> m)
          case Left(e) =>
            throw IllegalStateException(s"Failed to preload symbol metas from ${client.exchange}: ${e.message}")
      }.toMap

    val incomeBus = EventBus[IncomeEvent]()
    val outcomeBus = EventBus[OutcomeEvent]()

    OutcomeProcessor(clients, incomeBus, dryRun).run(outcomeBus.subscribe())

    // 时钟: 周期性发布 Clock 事件 (驱动订单超时清理等定时任务)
    fork {
      while true do
        Thread.sleep(clockIntervalMs)
        incomeBus.publish(IncomeEvent.local(EventData.Clock))
    }

    // 先启动账户流 (订阅 income 总线、建立私有连接)，再启动公共行情流——
    // 虚拟柜台同时扮演两者时，start 幂等，两次调用只生效一次
    accountStreams.values.foreach(_.start(incomeBus))
    marketStreams.values.foreach(_.start(incomeBus))

    val engine = Engine(clients, marketStreams, symbolMetas, incomeBus, outcomeBus)

    // 账户信息周期刷新: 未配置凭证的交易所在首次拉取后退出轮询 (确定安全的例外)
    fork {
      var polled = clients.values.toVector
      while polled.nonEmpty do
        Thread.sleep(accountRefreshMs)
        polled = polled.filter(engine.publishAccountInfoFrom)
    }

    logger.info("Engine started")
    engine
