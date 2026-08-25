package hft.engine

import hft.domain.*
import hft.state.{StateManager, SymbolState}
import hft.exchange.{AccountStream, ExchangeClient, MarketDataStream, TradingClient}
import hft.actor.{ActorHandle, ActorSystem}
import hft.event.{Event, EventBus, Interest, MarketTopic, Subscription, Topics}
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
final case class ExchangeGateway private (
    client: ExchangeClient,
    marketData: MarketDataStream,
    /** 私有面 —— 只有带凭证的网关才有。`None` 就是"这个交易所我们只读"，
      * 不再靠运行时问 `hasCredentials`、也不再靠 `Left(Auth)` 在调用链上传递这个事实。 */
    trading: Option[TradingClient],
    accountStream: Option[AccountStream],
)

object ExchangeGateway:
  /** 只读网关：无凭证的研究 / 全市场扫描模式。私有端点在类型上就够不着。 */
  def readOnly(client: ExchangeClient, marketData: MarketDataStream): ExchangeGateway =
    ExchangeGateway(client, marketData, trading = None, accountStream = None)

  /** 交易网关：拿得出 [[TradingClient]] 即证明凭证已具备。 */
  def trading(
      client: TradingClient,
      marketData: MarketDataStream,
      accountStream: Option[AccountStream] = None,
  ): ExchangeGateway =
    ExchangeGateway(client, marketData, trading = Some(client), accountStream)

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
    tradingClients: Map[Exchange, TradingClient],
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

  /** 把一个 [[hft.actor.Actor]] 挂进引擎的生命周期树 —— 全市场扫描器、绩效跟踪、监督者
    * 这类常驻组件。
    *
    * 与 [[addStrategy]] 的区别：不绑账户、不占标的、不做启动对齐。框架说"引擎里所有有生命
    * 周期的东西都是 Actor"，此前却只有策略进得来，别的组件只能绕开引擎自建 ActorSystem ——
    * 那样它们就不在引擎的停机链上了。
    */
  def spawn(actor: hft.actor.Actor): ActorHandle = system.spawn(actor)

  /** 停一个组件（连同它的整棵子树），并释放它可能占用的 (账户, 标的)。返回时收尾已跑完。 */
  def stop(handle: ActorHandle): Unit = synchronized {
    system.stop(handle)
    claims.release(handle)
  }

  def addStrategy(strategy: Strategy, account: AccountId): ActorHandle = addStrategies(Vector(strategy), account).head

  /** 撤下一个策略实例：先撤掉它挂在交易所的单 (见 [[Executor.onStop]])，再退订、摘除，
    * 最后释放它占用的 (账户, 标的)。
    *
    * 返回时收尾已经跑完。**不平仓** —— 仓位归谁管是策略之外的决定。
    */
  def removeStrategy(handle: ActorHandle): Unit = stop(handle)

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
    // 用穷举 match 而不是 `== Live`：将来多一种账户类型时这里会编译报错逼人来决定它要不要对齐，
    // 相等判断则会把它默默归进"影子盘"那一侧。
    account match
      case AccountId.Live =>
        publishInitialPositions(instruments)
        combined.exchanges.foreach(exchange =>
          AccountRefresher.publishAccountInfo(requireTrading(exchange), bus.publish)
        )
        publishExistingPendingOrders(instruments)
      case _: AccountId.Paper => () // 影子账户无历史可对齐

    // 5. 订阅行情
    subscribeStreams(combined)

    logger.info(s"${strategies.size} strategies added on $account")
    ids
  }

  /** 订阅公共行情但**不交易**这些标的 —— 全市场扫描器一类的观察者用。
    *
    * 与 [[addStrategies]] 的区别：不占 [[InstrumentClaims]]、不做启动对齐、不补私有回报。
    * "声明了某标的的行情 = 交易该标的"这条等价是对**策略**成立的（见
    * [[hft.event.Subscription.instruments]]），框架据此补私有回报、做仓位对齐。但扫描器要看
    * 全市场几百个标的、一个都不交易：走 addStrategies 会把它们全部独占登记，
    * 之后任何针对这些标的的交易策略都起不来 —— 那正是扫描器存在的目的。
    *
    * 观察者自己以 `Interest.All(topic)` 订总线收事件即可（[[hft.event.Interest.All]] 不产生
    * 标的归属）；本方法只补上"让数据真的从交易所流过来"这一步 —— 否则它订了个空。
    *
    * 可重复调用：交易所侧订阅是幂等的增量操作。
    */
  def watchMarket(instruments: Set[Instrument], topics: Set[MarketTopic[?]]): Unit = synchronized {
    require(instruments.nonEmpty, "watchMarket 需要至少一个标的")
    require(topics.nonEmpty, "watchMarket 需要至少一个行情 topic")
    subscribeStreams(Subscription(topics.map(t => Interest.Keyed(t, instruments))))
    logger.info(s"watching ${instruments.size} instruments for ${topics.map(_.name).mkString(",")} (不交易, 不占标的)")
  }

  /** 把订阅范围派生成交易所行情流并订上 —— addStrategies 与 watchMarket 的唯一公共出口 */
  private def subscribeStreams(subscription: Subscription): Unit =
    subscription.marketStreams.groupMap(_._1)(_._2).foreach { (exchange, kinds) =>
      marketStreams
        .getOrElse(exchange, throw IllegalStateException(s"No market data stream configured for exchange $exchange"))
        .subscribe(kinds)
    }

  /** 该交易所的私有面。缺失即"这个交易所没配凭证"，是装配错误，立即终止。 */
  private def requireTrading(exchange: Exchange): TradingClient =
    tradingClients.getOrElse(
      exchange,
      throw IllegalStateException(s"No trading client configured for exchange $exchange (只读网关不能下单/对齐)"),
    )

  /** 启动对齐必须成功 (fail-fast)。
    *
    * 从前这里有一条 `case Left(Auth) => 跳过` 的例外，用来放过"没配凭证"的研究模式 ——
    * 那是把一个**装配期就已确定的事实**伪装成运行时错误。现在只读网关根本没有私有面，
    * 压根不会走到这里，于是任何失败都是真失败，不必再分辨。
    */
  private def publishInitialPositions(instruments: Set[Instrument]): Unit =
    instruments.groupMap(_.exchange)(_.symbol).foreach { (exchange, symbols) =>
      requireTrading(exchange).fetchPositions() match
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
      requireTrading(exchange).fetchPendingOrders(symbol) match
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
      clockIntervalMs: Long = 1000,
      accountRefreshMs: Long = 10_000,
  )(using Ox): Engine =
    val clients = gateways.map(g => g.client.exchange -> g.client).toMap
    val tradingClients = gateways.flatMap(g => g.trading.map(t => t.exchange -> t)).toMap
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
    system.spawn(OutcomeProcessor(tradingClients, symbolMetas, AccountId.Live))
    system.spawn(Clock(clockIntervalMs))
    system.spawn(AccountRefresher(tradingClients.values, accountRefreshMs))

    // 先启动账户流 (订阅总线、建立私有连接)，再启动公共行情流——
    // 虚拟柜台同时扮演两者时，start 幂等，两次调用只生效一次
    accountStreams.values.foreach(_.start(bus))
    marketStreams.values.foreach(_.start(bus))

    logger.info("Engine started")
    Engine(clients, tradingClients, marketStreams, symbolMetas, bus, system)
