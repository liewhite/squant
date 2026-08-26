package hft.engine

import hft.actor.{Actor, ActorHandle, ActorSystem}
import hft.domain.*
import hft.event.Commands.*
import hft.event.{Event, EventBus, Interest, MarketTopic, Subscription, Topics}
import hft.strategy.Strategy
import org.slf4j.LoggerFactory
import ox.{Ox, forkDiscard}

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{CountDownLatch, TimeUnit}
import scala.collection.mutable

/** 引擎：装配插件、校验契约、管理生命周期。**不持有任何交易所侧的实现。**
  *
  * {{{
  *   行情插件 ──┐                    ┌── 策略插件 ──┐
  *   柜台插件 ──┼──> 总线 (topic 路由) ┤              │ 下单指令
  *   时钟插件 ──┘                    └── 观察者     │
  *        ▲                                        │
  *        └────────────────────────────────────────┘
  * }}}
  *
  * 只有**一条**总线：行情、账户回报、时钟、下单指令、控制指令都是它上面的事件，
  * 只是 topic 不同。投递按 (topic, key) 建索引，订阅者只收自己声明的那些 ——
  * 下单指令不会回流给策略，因为策略压根不订阅它。
  *
  * ## 引擎不再知道交易所
  *
  * 从前它攥着三张按交易所索引的表 (公共客户端 / 行情流 / 交易客户端)，于是"接入任意
  * 交易所"止步于这三张表装得下的东西。现在它只发指令：谁接、有几个接、接的是真交易所
  * 还是虚拟柜台，一概不需要知道 (见 [[hft.event.Commands]])。
  *
  * ## 所有组件都是同一种形态
  *
  * 策略执行器、柜台、行情源、时钟、监控与指标导出，都是 [[Actor]]，都在引擎的生命周期
  * 树上。任一组件崩溃将级联终止整个作用域 —— 不做局部重启：一个崩掉的策略留下的挂单与
  * 仓位归谁管是个没有好答案的问题，而重启后的对齐有答案。
  */
final class Engine private (bus: EventBus, system: ActorSystem)(using Ox):
  private val logger = LoggerFactory.getLogger(classOf[Engine])

  /** (账户, 标的) 的独占登记，见 [[InstrumentClaims]] */
  private val claims = InstrumentClaims[ActorHandle]()

  /** 对齐请求编号：逐次自增，使等待方只认领自己那一条应答，
    * 不会把上一轮的残留应答当成本轮完成 */
  private val syncSeq = AtomicLong(0)

  /** 在策略**之外**订阅事件 (成交记录、监控、指标导出)，策略因此无需承担写文件等副作用。
    *
    * 返回一个邮箱：调用方负责在自己的作用域内 fork 消费，**并在不再需要时 `close()` 退订**
    * (邮箱无界，不退订就会一直攒事件)。需要随引擎一起管理生命周期的观察者，更好的做法是
    * 实现 [[Actor]] 交给 [[install]]，退订由框架负责。
    */
  def subscribe(interests: Set[Interest]): EventBus.Mailbox = bus.subscribe(interests)

  /** 把一个插件装到总线上 —— 行情源、柜台、扫描器、绩效跟踪、监督者都走这里。
    *
    * 与 [[addStrategy]] 的区别：不绑账户、不占标的、不做启动对齐、不校验指令契约。
    * 插件装上即开始工作：柜台开始接下单指令，行情源开始接订阅指令。
    */
  def install(plugin: Actor): ActorHandle = system.spawn(plugin)

  /** 停一个组件（连同它的整棵子树），并释放它可能占用的 (账户, 标的)。返回时收尾已跑完。
    *
    * 若它是某条指令的最后一个接单者，记录告警并列出还在发这条指令的组件 ——
    * 但**不级联停止**它们：数据依赖天然成环 (策略依赖柜台的回报、柜台依赖策略的下单)，
    * 拿它定停机顺序无解；"要不要把依赖它的一起停掉"是运维决定，框架替人决定会在不该停的
    * 时候停。生命周期依赖走的是另一条路 —— 谁装的谁负责停，那是棵树。
    */
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
    * 启动顺序是一条**因果链**，不再是一串调用顺序：
    * {{{
    *   1. 标的独占检查        —— 冲突在策略启动之前拒绝
    *   2. 指令契约校验        —— 它要发的每条指令都有人接吗
    *   3. 装配, 订阅总线      —— 此后发布的事件不会丢
    *   4. 发对齐指令, 等应答  —— 柜台推持仓/净值/挂单
    *   5. 发行情订阅指令      —— 数据从此刻开始流动
    * }}}
    *
    * 第 4 步在第 5 步之前，保住的是：**策略在拿到初始仓位之前不会看到第一条行情**。
    * 否则它基于"仓位为空"这个错误前提做第一次决策 —— 可能重复开仓，可能对着不存在的
    * 敞口对冲。
    *
    * 前两步都排在 spawn 之前：起来了再拒绝，就得再把它停回去。
    */
  def addStrategies(strategies: Seq[Strategy], account: AccountId): Seq[ActorHandle] = synchronized {
    if strategies.isEmpty then return Vector.empty

    val executors = strategies.map(Executor(_, account))
    def keysOf(ex: Executor) = ex.subscription.instruments.map(AccountInstrument(ex.account, _))
    // 策略订阅范围的并集 —— 契约校验、对齐、行情订阅都从这一处派生
    val combined = Subscription(executors.flatMap(_.subscription.interests).toSet)

    // 1~2. 启动之前的两道闸
    claims.checkAll(executors.map(ex => (ex.name, keysOf(ex))))
    verifyCommandsServed(combined, account)

    // 3. 装配
    val ids = executors.map(system.spawn)
    claims.claimAll(executors.zip(ids).map((ex, handle) => (handle, ex.name, keysOf(ex))))

    // 4. 启动对齐：只有实盘需要。
    // 影子账户从零开始 —— 没有历史仓位与挂单要恢复。拿真实交易所的持仓去对齐一个模拟
    // 账户是错的：那是别人的仓位。
    // 用穷举 match 而不是 `== Live`：将来多一种账户类型时这里会编译报错逼人来决定它要不要
    // 对齐，相等判断则会把它默默归进"影子盘"那一侧。
    account match
      case AccountId.Live   => syncAccounts(combined, account)
      case _: AccountId.Paper => ()

    // 5. 行情从此刻开始流动
    requestMarketData(combined)

    logger.info(s"${strategies.size} strategies added on $account")
    ids
  }

  /** 订阅公共行情但**不交易**这些标的 —— 全市场扫描器一类的观察者用。
    *
    * 与 [[addStrategies]] 的区别：不占标的、不做启动对齐、不补私有回报。
    * "声明了某标的的行情 = 交易该标的"这条等价是对**策略**成立的（见
    * [[hft.event.Subscription.instruments]]），框架据此补私有回报、做仓位对齐。但扫描器要看
    * 全市场几百个标的、一个都不交易：走 addStrategies 会把它们全部独占登记，
    * 之后任何针对这些标的的交易策略都起不来 —— 那正是扫描器存在的目的。
    *
    * 观察者自己以 `Interest.All(topic)` 订总线收事件即可；本方法只补上"让数据真的从
    * 交易所流过来"这一步 —— 否则它订了个空。
    *
    * 可重复调用：行情订阅指令是幂等的增量操作。
    */
  def watchMarket(instruments: Set[Instrument], topics: Set[MarketTopic[?]]): Unit = synchronized {
    require(instruments.nonEmpty, "watchMarket 需要至少一个标的")
    require(topics.nonEmpty, "watchMarket 需要至少一个行情 topic")
    val subscription = Subscription(topics.map(t => Interest.Keyed(t, instruments)))
    verifyMarketFeedsServed(subscription)
    requestMarketData(subscription)
    logger.info(s"watching ${instruments.size} instruments for ${topics.map(_.name).mkString(",")} (不交易, 不占标的)")
  }

  // ==================== 指令契约校验 ====================

  /** 这批策略将要发出的每一条指令，总线上都有人接吗？没有就拒绝启动。
    *
    * 一条没人接的指令是**零症状**的静默失效：行情永远不会到、订单永远不会发出、
    * 对齐永远不完成，而不会有任何异常、任何错误日志。把它变成启动即失败，
    * 是整套插件化最主要的正确性收益 (见 [[hft.event.Commands]])。
    *
    * 校验查的是**订阅事实本身**，不需要任何插件声明"我提供什么" —— 订阅是它为了工作
    * 本来就必须做的事，多一份声明就多一处会写错、会漏写的事实，而漏写的表现是启动被误拒。
    */
  private def verifyCommandsServed(subscription: Subscription, account: AccountId): Unit =
    val missing = mutable.ArrayBuffer.empty[String]
    missing ++= unservedMarketFeeds(subscription)
    subscription.exchanges.toVector.sortBy(_.toString).foreach { exchange =>
      val target = AccountExchange(account, exchange)
      if !bus.hasSubscriber(OrderIntent, target) then
        missing += s"下单指令 $target 无人接单: 没有装载该账户在 $exchange 的柜台, 订单永远发不出去"
      // 只有会发对齐指令的账户才需要有人接 —— 影子账户从零开始, 不对齐
      val needsSync = account match
        case AccountId.Live     => true
        case _: AccountId.Paper => false
      if needsSync && !bus.hasSubscriber(AccountSync, target) then
        missing += s"账户对齐指令 $target 无人接单: 启动对齐永远不会完成, 策略会一直等下去"
    }
    if missing.nonEmpty then
      throw IllegalStateException(
        s"指令契约校验未通过, 拒绝启动 (${missing.size} 项):\n  " + missing.mkString("\n  ")
      )

  private def verifyMarketFeedsServed(subscription: Subscription): Unit =
    val missing = unservedMarketFeeds(subscription)
    if missing.nonEmpty then
      throw IllegalStateException(
        s"指令契约校验未通过, 拒绝启动 (${missing.size} 项):\n  " + missing.mkString("\n  ")
      )

  private def unservedMarketFeeds(subscription: Subscription): Vector[String] =
    subscription.marketStreams
      .map(_._1)
      .toVector
      .distinct
      .sortBy(_.toString)
      .filterNot(bus.hasSubscriber(MarketSubscription, _))
      .map(exchange => s"行情订阅指令 $exchange 无人接单: 没有装载 $exchange 的行情插件, 策略订了个空")

  // ==================== 指令发出 ====================

  /** 把订阅范围派生成行情订阅指令。同一条流被请求多次由行情插件去重 (幂等增量) */
  private def requestMarketData(subscription: Subscription): Unit =
    subscription.marketStreams.groupMap(_._1)(_._2).foreach { (exchange, kinds) =>
      bus.publish(Event.local(MarketSubscription, MarketSubscriptionRequest(exchange, kinds.toSet)))
    }

  /** 发对齐指令并**等到柜台推完**。
    *
    * 请求—应答在发布订阅上通常很别扭，因为失败无从表达。这里不别扭：柜台对齐失败一律
    * 抛异常终止进程 (账户状态没对上就交易，比不启动危险得多)，"没人接"已被
    * [[verifyCommandsServed]] 挡在前面 —— 不存在"既没成功也没失败"的第三种结局。
    *
    * 超时仍然设：一个永久挂起的启动是最难诊断的失效，宁可以明确的错误退出。
    */
  private def syncAccounts(subscription: Subscription, account: AccountId): Unit =
    val symbolsByExchange = subscription.instruments.groupMap(_.exchange)(_.symbol)
    if symbolsByExchange.isEmpty then return

    val requestId = syncSeq.incrementAndGet()
    val targets = symbolsByExchange.keySet.map(AccountExchange(account, _))
    val done = CountDownLatch(targets.size)
    // 先订阅再发指令：应答可能在指令发出后立刻回来
    val mailbox = bus.subscribe(targets.map(t => Interest.Keyed(AccountSynced, Set(t))).toSet[Interest])
    forkDiscard {
      mailbox.events.foreach { event =>
        event.as(AccountSynced).foreach(report => if report.requestId == requestId then done.countDown())
      }
    }

    symbolsByExchange.foreach { (exchange, symbols) =>
      bus.publish(Event.local(AccountSync, AccountSyncRequest(account, exchange, requestId, symbols)))
    }

    val completed = done.await(Engine.SyncTimeoutMs, TimeUnit.MILLISECONDS)
    mailbox.close()
    mailbox.done()
    if !completed then
      throw IllegalStateException(
        s"启动对齐超时 (${Engine.SyncTimeoutMs}ms, req=$requestId): ${targets.mkString(",")} 中有柜台没有回应。" +
          "对齐未完成就放行会让策略基于空仓位决策"
      )
    logger.info(s"启动对齐完成: ${targets.mkString(",")} (req=$requestId)")

object Engine:
  private val logger = LoggerFactory.getLogger(classOf[Engine])

  /** 启动对齐的等待上限。柜台在自己的 actor 线程上同步跑完对齐，正常情况是几次 REST 往返 */
  val SyncTimeoutMs: Long = 60_000

  /** 启动引擎：装配总线与生命周期树，装上时钟与调用方给的插件。
    *
    * **消费者先起、生产者后起**：事件开始流动时下游必须已经在总线上，否则最早的那批事件
    * 没人接。插件按给定顺序装载，调用方因此可以把柜台排在行情源之前 —— 柜台既是消费者
    * (接下单指令) 也是生产者 (推回报)，而行情源是纯生产者。
    *
    * 这也是停机顺序的反面：生产者先停，它们收尾时补发的最后一批事件仍有人消费。
    *
    * @param plugins          行情源、柜台、观察者 —— 装配顺序即启动顺序
    * @param clockIntervalMs  时钟节拍间隔 (驱动订单超时检测等)
    */
  def start(plugins: Seq[Actor] = Vector.empty, clockIntervalMs: Long = 1000)(using Ox): Engine =
    val bus = EventBus()
    val system = ActorSystem(bus)
    system.spawn(Clock(clockIntervalMs))
    plugins.foreach(system.spawn)
    logger.info(s"Engine started with ${plugins.size} plugins")
    Engine(bus, system)
