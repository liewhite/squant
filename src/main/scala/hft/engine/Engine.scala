package hft.engine

import hft.actor.{Actor, ActorHandle, ActorState, ActorSystem}
import hft.domain.*
import hft.event.Commands.*
import hft.event.{Event, EventBus, Interest, MarketTopic, Subscription, Topics}
import hft.strategy.Strategy
import org.slf4j.LoggerFactory
import ox.{Ox, forkDiscard}

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, TimeUnit}
import scala.jdk.CollectionConverters.*

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
    * 与 [[addStrategy]] 的区别：不绑账户、不占标的、不做策略专属的启动对齐。
    * 插件装上即开始工作：柜台开始接下单指令，行情源开始接订阅指令。
    */
  def install(plugin: Actor): ActorHandle = synchronized {
    system.spawn(plugin)
  }

  /** 停一个组件（连同它的整棵子树），并释放它可能占用的 (账户, 标的)。返回时收尾已跑完。
    * 若仍存活组件依赖它提供的命令能力，ActorSystem 会在产生副作用前拒绝停止。 */
  def stop(handle: ActorHandle): Unit = synchronized {
    try system.stop(handle)
    finally releaseClaimIfStopped(handle)
  }

  /** 只有已从 ActorSystem 摘除的句柄才能释放独占权；拒绝停止与隔离都必须继续占用。 */
  private def releaseClaimIfStopped(handle: ActorHandle): Unit =
    if handle.state == ActorState.Stopped || handle.state == ActorState.Failed then claims.release(handle)

  /** 请求停机 —— 幂等。风控插件、运维接口都可以调它 */
  def requestShutdown(reason: String): Unit = system.requestShutdown(reason)

  /** **启动器的最后一行**：阻塞到有人请求停机，按依赖拓扑停完全部组件，核心最后退出。
    *
    * 期间接管中断信号；组件失败 (私有流断线一类) 同样走这条路 —— 先停完、每个 `onStop`
    * 都跑到 (策略因此撤得掉它挂在交易所的单)，再把原异常抛出使进程非零退出。
    *
    * 从前这里是 `Thread.sleep(Long.MaxValue)`：Ctrl+C 直接杀掉 JVM，`onStop` 一律不跑，
    * 挂单原样留在交易所无人跟踪 —— 那条撤单机制在实盘路径上从未执行过。
    */
  def awaitShutdown(): Unit = system.awaitShutdown()

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
    *
    * ## 一个实例只装一次 —— 这是**调用方的职责**
    *
    * `Strategy` 的可变状态 (指标序列、判据基准、挂单槽) 都挂在实例上，`handlers` 是捕获
    * 它们的闭包。所以**同一个实例不要装两次**，实盘与影子盘并行时给每个账户 `new` 一个：
    *
    * {{{
    * engine.addStrategy(MyStrategy(...), AccountId.Live)      // 各 new 一个
    * engine.addStrategy(MyStrategy(...), AccountId.Paper(1))
    * }}}
    *
    * 框架不替你查这件事。标的独占按 `(账户, 标的)` 登记，两个账户本就该放行 (那正是影子盘
    * 存在的前提)；而"这两次传进来的是不是同一个对象"是调用方一眼可见、框架却只能靠身份
    * 比较去猜的事 —— 猜错的方向 (误拒一个合法的复用) 比它想防的问题更难查。
    * 需要同一份逻辑跑多个账户，就写一个产生实例的函数，别复用实例。
    */
  def addStrategies(strategies: Seq[Strategy], account: AccountId): Seq[ActorHandle] = synchronized {
    if strategies.isEmpty then return Vector.empty

    val executors = strategies.map(Executor(_, account))
    def keysOf(ex: Executor) = ex.subscription.instruments.map(AccountInstrument(ex.account, _))
    // 策略订阅范围的并集 —— 契约校验、对齐、行情订阅都从这一处派生
    val combined = Subscription(executors.flatMap(_.subscription.interests).toSet)

    // 1. 标的独占检查。命令依赖与处理者基数由 ActorSystem 在整批接线之前统一校验。
    claims.checkAll(executors.map(ex => (ex.name, keysOf(ex))))

    // 2. 原子装配。执行器自带对齐闸门 (见 Executor.awaiting): spawn 之后它的事件循环立即
    // 开跑, 而已经在流动的行情会马上到 —— 闸门必须在构造时就位, 不能靠装配方记得调一下。
    val ids = system.spawnAll(executors)
    try
      claims.claimAll(executors.zip(ids).map((ex, handle) => (handle, ex.name, keysOf(ex))))

      // 3. 启动对齐 —— **两种账户都做**。
      syncAccounts(combined, account)

      // 4. 行情从此刻开始流动
      requestMarketData(combined)

      logger.info(s"${strategies.size} strategies added on $account")
      ids
    catch
      case e: Throwable =>
        // 对齐/行情请求失败同样属于装配事务；不能留下已运行执行器与未释放的独占声明。
        ids.reverse.foreach { handle =>
          try system.stop(handle)
          catch case cleanup: Throwable => e.addSuppressed(cleanup)
          finally releaseClaimIfStopped(handle)
        }
        throw e
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
    val requirements = subscription.marketStreams.map(_._1).map { exchange =>
      hft.actor.Requirement.command(MarketSubscription, exchange, s"行情订阅指令 $exchange 无处理者")
    }
    system.validate(requirements, "watchMarket")
    requestMarketData(subscription)
    logger.info(s"watching ${instruments.size} instruments for ${topics.map(_.name).mkString(",")} (不交易, 不占标的)")
  }

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
    * ActorSystem 的硬依赖校验挡在前面 —— 不存在"既没成功也没失败"的第三种结局。
    *
    * 超时仍然设：一个永久挂起的启动是最难诊断的失效，宁可以明确的错误退出。
    */
  private def syncAccounts(subscription: Subscription, account: AccountId): Unit =
    // 目标集合与执行器的闸门同源 (见 Subscription.alignmentTargets) —— 从前这里按
    // instruments 自己算一遍, 少了"账户级声明直接指名的交易所", 那个差集里的策略会永久
    // 停在闸门后而引擎照常打印"对齐完成"。
    val targets = subscription.alignmentTargets(account)
    if targets.isEmpty then return
    val symbolsByExchange = subscription.instruments.groupMap(_.exchange)(_.symbol)

    val requestId = syncSeq.incrementAndGet()
    val tracker = SyncTracker(targets, requestId)
    // 先订阅再发指令：应答可能在指令发出后立刻回来
    val mailbox = bus.subscribe(targets.map(t => Interest.Keyed(AccountSynced, Set(t))).toSet[Interest])
    val completed = try
      forkDiscard {
        mailbox.events.foreach { event =>
          event.as(AccountSynced).foreach(tracker.acknowledge)
        }
      }
      targets.foreach { target =>
        // 没有交易标的的交易所照样要发 (空标的集): 柜台什么都不用对, 但那条应答是闸门在等的
        val symbols = symbolsByExchange.getOrElse(target.exchange, Set.empty)
        bus.publish(Event.local(AccountSync, AccountSyncRequest(account, target.exchange, requestId, symbols)))
      }
      tracker.await(Engine.SyncTimeoutMs)
    finally
      mailbox.close()
      mailbox.done()
    if !completed then
      throw IllegalStateException(
        s"启动对齐超时 (${Engine.SyncTimeoutMs}ms, req=$requestId): ${tracker.pending.mkString(",")} 中有柜台没有回应。" +
          "对齐未完成就放行会让策略基于空仓位决策"
      )
    logger.info(s"启动对齐完成: ${targets.mkString(",")} (req=$requestId)")

object Engine:
  private val logger = LoggerFactory.getLogger(classOf[Engine])

  /** 启动对齐的等待上限。柜台在自己的 actor 线程上同步跑完对齐，正常情况是几次 REST 往返 */
  val SyncTimeoutMs: Long = 60_000

  /** 启动引擎：装配总线与生命周期树，装上时钟与调用方给的插件。
    *
    * 时钟与插件作为一批启动事务：先快照声明、校验并接线，全部 `onStart` 成功后才提交处理
    * 能力、事件循环和启动期输出。因此插件给定顺序不承担消息安全或停机正确性；硬依赖必须
    * 用 `Requirement` 声明，停机由依赖图与所有权树共同排序。
    *
    * @param plugins          行情源、柜台、观察者
    * @param clockIntervalMs  时钟节拍间隔 (驱动订单超时检测等)
    */
  def start(plugins: Seq[Actor] = Vector.empty, clockIntervalMs: Long = 1000)(using Ox): Engine =
    val bus = EventBus()
    val system = ActorSystem(bus)
    val engine = Engine(bus, system)
    // 时钟与插件作为一批原子装配：任一 onStart 失败则按依赖拓扑整批回滚。
    try system.spawnAll(Clock(clockIntervalMs) +: plugins.toVector)
    catch
      case startupFailure: Throwable =>
        try system.stopAll()
        catch case cleanupFailure: Throwable => startupFailure.addSuppressed(cleanupFailure)
        throw startupFailure
    logger.info(s"Engine started with ${plugins.size} plugins")
    engine

/** 一次账户对齐等待的并发状态：同一 target 只可完成一次，旧请求与未知 target 都无效。 */
private[engine] final class SyncTracker(targets: Set[AccountExchange], requestId: Long):
  private val remaining = ConcurrentHashMap.newKeySet[AccountExchange]()
  remaining.addAll(targets.asJava)
  private val done = CountDownLatch(targets.size)

  def acknowledge(report: AccountSyncReport): Unit =
    if report.requestId == requestId && remaining.remove(report.target) then done.countDown()

  def await(timeoutMs: Long): Boolean = done.await(timeoutMs, TimeUnit.MILLISECONDS)
  def pending: Set[AccountExchange] = remaining.asScala.toSet
