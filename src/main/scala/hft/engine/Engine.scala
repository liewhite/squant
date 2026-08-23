package hft.engine

import hft.domain.*
import hft.state.{StateManager, SymbolState}
import hft.exchange.{AccountStream, ExchangeClient, MarketDataStream, SubscriptionKind}
import hft.actor.{ActorHandle, ActorSystem}
import hft.event.{Event, EventBus, Interest, Subscription, Topics}
import hft.strategy.Strategy
import org.slf4j.LoggerFactory
import ox.{Ox, fork}

import scala.collection.mutable
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
  * Connector (WS) ──┐                    ┌─> Executor (Strategy + StateManager) ─┐
  * Clock ───────────┼─> bus (topic 路由) ┤                                        │ OrderIntent
  * 执行回流 <────────┘                    └─> OutcomeProcessor ─> REST ─────────────┘
  * }}}
  *
  * 只有**一条**总线：行情、账户回报、时钟、策略下单意图都是它上面的事件，只是 topic 不同。
  * 投递按 (topic, key) 建索引，订阅者只收自己声明的那些 —— 下单意图不会回流给策略，
  * 因为策略压根不订阅 [[hft.strategy.OrderIntent]]。
  *
  * 所有组件都是引擎所在 Ox 作用域内的虚拟线程 fork，任一组件崩溃将级联终止整个作用域，
  * 对应参考实现中 spawn_link + 级联退出的监督语义。
  */
final class Engine private (
    clients: Map[Exchange, ExchangeClient],
    marketStreams: Map[Exchange, MarketDataStream],
    symbolMetas: Map[(Exchange, Symbol), SymbolMeta],
    bus: EventBus,
    system: ActorSystem,
)(using Ox):
  private val logger = LoggerFactory.getLogger(classOf[Engine])

  /** (账户, 标的) 的独占登记，见 [[InstrumentClaims]] */
  private val claims = InstrumentClaims[ActorHandle]()

  /** 在策略**之外**订阅事件 (成交记录、监控、指标导出)，策略因此无需承担写文件等副作用。
    *
    * 返回一个邮箱：调用方负责在自己的作用域内 fork 消费，**并在不再需要时 `close()` 退订**
    * (邮箱无界，不退订就会一直攒事件)。需要随引擎一起管理生命周期的观察者，更好的做法是
    * 实现 [[hft.actor.Actor]] 交给 [[hft.actor.ActorSystem]]，退订由它负责。
    *
    * 须在关心的事件产生前订阅 (通常紧随 [[Engine.start]]、在 [[addStrategies]] 之前)。
    */
  def subscribe(interests: Set[Interest]): EventBus.Mailbox = bus.subscribe(interests)

  def addStrategy(strategy: Strategy, account: AccountId): ActorHandle = addStrategies(Vector(strategy), account).head

  /** 撤下一个策略实例：先撤掉它挂在交易所的单 (见 [[Executor.onStop]])，再退订、摘除，
    * 最后释放它占用的 (账户, 标的)。
    *
    * 返回时收尾已经跑完。**不平仓** —— 仓位归谁管是策略之外的决定。
    */
  def removeStrategy(handle: ActorHandle): Unit = synchronized {
    system.stop(handle)
    claims.release(handle)
  }

  /** 批量添加策略。
    *
    * 启动顺序保证 (与参考实现一致)：
    *   1. 创建 Executor 并订阅 事件总线 —— 之后发布的事件不会丢
    *   2. REST 查询初始持仓并发布 —— 避免策略基于缺失仓位决策；
    *      交易所未返回的 symbol 显式推 size=0，保证 SymbolState 一定收到初始值
    *   3. REST 查询账户信息 (净值/名义价值) 并发布 —— 风控类决策 (杠杆率) 依赖
    *   4. REST 查询现有挂单并发布 —— 策略接管启动前的遗留订单
    *   5. 向交易所订阅行情 —— 市场数据从此处开始流动
    */
  def addStrategies(strategies: Seq[Strategy], account: AccountId): Seq[ActorHandle] =
    synchronized {
    if strategies.isEmpty then return Vector.empty

    // 1. 创建 Executor，先查唯一性再 spawn (查完再落地，避免半启动状态)
    val executors = strategies.map(Executor(_, symbolMetas, account))
    def keysOf(ex: Executor) = ex.subscription.instruments.map(AccountInstrument(ex.account, _))
    // 先查后起：(账户, 标的) 冲突要在策略启动之前拒绝
    claims.checkAll(executors.map(ex => (ex.name, keysOf(ex))))
    val ids = executors.map(system.spawn)
    claims.claimAll(executors.zip(ids).map((ex, handle) => (handle, ex.name, keysOf(ex))))

    // 策略订阅范围的并集 —— 行情订阅与启动对齐都从这一处派生
    val combined = Subscription(executors.flatMap(_.subscription.interests).toSet)
    val instruments = combined.instruments

    // 2~4. 启动对齐：只有实盘需要。
    // 影子账户从零开始 —— 没有历史仓位与挂单要恢复，唯一的初值 (净值) 由它自己的
    // 虚拟柜台周期发布 (见 hft.sim.PaperCounter)。拿真实交易所的持仓去对齐一个模拟
    // 账户是错的：那是别人的仓位。
    if account == AccountId.Live then
      publishInitialPositions(instruments)
      combined.exchanges.foreach(exchange =>
        AccountRefresher.publishAccountInfo(requireClient(exchange), bus.publish, logger)
      )
      publishExistingPendingOrders(instruments)

    // 5. 订阅行情
    SubscriptionKind.from(combined).groupMap(_._1)(_._2).foreach { (exchange, kinds) =>
      marketStreams
        .getOrElse(exchange, throw IllegalStateException(s"No market data stream configured for exchange $exchange"))
        .subscribe(kinds)
    }

    logger.info(s"${strategies.size} strategies added on $account")
    ids
  }

  private def requireClient(exchange: Exchange): ExchangeClient =
    clients.getOrElse(exchange, throw IllegalStateException(s"No client configured for exchange $exchange"))

  /** 启动对齐必须成功 (fail-fast)，唯一的例外是未配置凭证：
    * 无账户即无仓位/挂单需要对齐，跳过是确定安全的 (公开数据演示/研究模式)
    */
  private def publishInitialPositions(instruments: Set[Instrument]): Unit =
    instruments.groupMap(_.exchange)(_.symbol).foreach { (exchange, symbols) =>
      requireClient(exchange).fetchPositions() match
        case Left(ExchangeError.Auth(_)) =>
          logger.info(s"No credentials for $exchange, skipping position alignment")
        case Left(e) =>
          throw IllegalStateException(s"Failed to fetch initial positions from $exchange: ${e.message}")
        case Right(positions) =>
          val bySymbol = positions.map(p => p.symbol -> p).toMap
          symbols.foreach { symbol =>
            val pos = bySymbol.getOrElse(symbol, Position.empty(AccountId.Live, exchange, symbol))
            logger.info(s"Initial position loaded: $exchange $symbol size=${pos.size}")
            bus.publish(Event.local(Topics.Position, pos))
          }
    }

  private def publishExistingPendingOrders(instruments: Set[Instrument]): Unit =
    instruments.foreach { case Instrument(exchange, symbol) =>
      requireClient(exchange).fetchPendingOrders(symbol) match
        case Left(ExchangeError.Auth(_)) =>
          logger.info(s"No credentials for $exchange, skipping pending order alignment")
        case Left(e) =>
          throw IllegalStateException(s"Failed to fetch pending orders from $exchange $symbol: ${e.message}")
        case Right(updates) =>
          if updates.nonEmpty then
            logger.info(s"Fetched ${updates.size} existing pending orders: $exchange $symbol")
          // 数量已由适配层在解析时换成币本位 (类型保证)，这里不再转第二次
          updates.foreach(update => bus.publish(Event.local(Topics.OrderUpdate, update)))
    }

object Engine:
  private val logger = LoggerFactory.getLogger(classOf[Engine])

  /** 启动引擎：预加载交易对元数据 (失败即终止启动)、装配事件总线 (只有一条)、
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

    val bus = EventBus()
    val system = ActorSystem(bus)

    // 消费者先起、生产者后起: 事件开始流动时下游必须已经在总线上, 否则最早的那批事件没人接。
    // 这也是停机顺序的反面 —— 生产者先停, 它们收尾时补发的最后一批事件仍有人消费。
    system.spawn(OutcomeProcessor(clients, symbolMetas, dryRun, AccountId.Live))
    system.spawn(Clock(clockIntervalMs))
    system.spawn(AccountRefresher(clients.values, accountRefreshMs))

    // 先启动账户流 (订阅总线、建立私有连接)，再启动公共行情流——
    // 虚拟柜台同时扮演两者时，start 幂等，两次调用只生效一次
    accountStreams.values.foreach(_.start(bus))
    marketStreams.values.foreach(_.start(bus))

    logger.info("Engine started")
    Engine(clients, marketStreams, symbolMetas, bus, system)
