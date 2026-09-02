package hft.actor

import hft.domain.nowMs
import hft.event.{AnyEvent, CommandTopic, EventBus}
import org.slf4j.LoggerFactory
import ox.{OxUnsupervised, uninterruptible}

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}
import java.util.concurrent.{CopyOnWriteArrayList, CountDownLatch, TimeUnit}
import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/** actor 的装配与生命周期树。
  *
  * ## 树形：谁 spawn 的谁负责等
  *
  * [[stop]] 一个 actor 时，系统把所有权树与硬依赖图合成停机拓扑：子孙先于祖先、依赖方
  * 先于提供方。调用返回时整棵子树的 [[Actor.onStop]] 都已经跑完。
  *
  * 不等的后果不是理论问题：actor 的 `onStop` 里有真活要干 (撤挂单)，调用方一返回就把
  * 还在收尾的子孙丢下，等于漏发那批指令。
  *
  * ## 停机不丢已到达的事件
  *
  * 停止信号就是**关闭邮箱**：先退订 (不再有新事件进来)，再 `done()`。ox 的 `done` 保证
  * 已缓冲的值仍被投递完，`Source.foreach` 消费到那时才返回 —— 于是停机时邮箱里积压的
  * 成交回报不会被丢掉。
  *
  * 早先的写法是 `select(停止信号, 事件流)`，那是**竞态**的：两路同时就绪时选谁不确定，
  * 挑中停止信号就把邮箱里没处理的事件连同状态更新一起丢了。
  *
  * ## fail-fast, 但**有序**
  *
  * 任何组件失败 (事件循环抛异常、或它自管的线程抛异常, 例如私有流断线) 都终止整个进程，
  * 由外层重新拉起并靠启动对齐恢复一致。**不做局部重启** —— 一个崩掉的策略留下的挂单与
  * 仓位归谁管是个没有好答案的问题，而重启后的对齐有答案。
  *
  * 但"终止"不等于"就地把作用域取消掉"。失败的组件把异常**上报**给系统
  * ([[reportFailure]])，系统据此走一遍与正常停机完全相同的路径 ([[stopAll]])：
  * 按依赖拓扑停完每个组件、每个 `onStop` 都跑到、最后核心自己退出，然后把原异常重新抛出。
  *
  * 从前是直接让异常炸穿 ox 作用域，于是所有 fork 被中断、`onStop` 一律跳过 ——
  * 策略的撤单指令漏发，挂单原样留在交易所无人跟踪。**收尾要跑到，和进程要死掉，
  * 是两件不冲突的事**。
  *
  * ## 调用方必须在并发作用域树内
  *
  * 本系统起的每条线程都是构造它的那个 `unsupervised` 作用域里的一条 ox fork
  * (见 [[ManagedTask]])，而 ox 只允许**作用域树内的线程**创建 fork。因此
  * `spawn`/`spawnAll`/`stop`/`ActorContext.fork` 等入口只能从以下线程调用：创建作用域的那条
  * 线程、或该作用域 (含子作用域) 里的某条 fork —— 也就是启动器自己、以及任何组件的钩子、
  * 事件循环与受管任务。从一条与作用域无关的裸线程调用会立刻失败并说明原因，
  * 而不是留下一条没人管的线程。
  */
final class ActorSystem(
    private[actor] val bus: EventBus,
    private val failureSink: Throwable => Unit = _ => (),
    private val componentStopTimeoutMs: Long = ActorSystem.ComponentStopTimeoutMs,
)(using OxUnsupervised)
    extends AutoCloseable:
  require(componentStopTimeoutMs > 0, "componentStopTimeoutMs 必须大于 0")
  private val logger = LoggerFactory.getLogger(classOf[ActorSystem])
  private val roots = CopyOnWriteArrayList[ActorHandle]()
  private val accepting = AtomicBoolean(true)
  /** 装配冷路径的互斥锁：校验与接线必须看同一份组件集合。 */
  private val assemblyLock = new Object
  /** 已通过卸载校验、但尚未完成退订的组件。装配校验必须把它们视为不可用。 */
  private val stoppingHandles = scala.collection.mutable.HashSet.empty[ActorHandle]
  /** 已接线但整批启动事务尚未提交的组件。用户钩子在全局锁外执行，其他事务据此等待。 */
  private val assemblingHandles = scala.collection.mutable.HashSet.empty[ActorHandle]
  /** 把系统推向停机的第一个失败。空表示这是一次正常停机。 */
  private val failure = AtomicReference[Throwable](null)
  private val stopFailure = AtomicReference[Throwable](null)
  /** Java 无法安全强杀不合作的插件；隔离后禁止全部 Actor 输出，等待进程退出。 */
  private val quarantined = AtomicBoolean(false)
  /** 读锁允许正常组件并发发布；隔离拿写锁，建立“返回后绝无迟到输出”的线性化边界。 */
  private val publicationGate = ReentrantReadWriteLock()
  private val shutdownRequested = CountDownLatch(1)
  private val allStopped = CountDownLatch(1)

  /** 被停机丢弃的延迟事件计数 —— 丢弃必须说得出数量，见 [[ActorContext.scheduleEvent]]。 */
  private val droppedDelayed = AtomicLong(0L)

  /** 延迟发布用的定时器 (单线程 + 提交序全序，见 [[DelayTimer]])。
    * 它自己的线程死掉意味着全系统延迟事件停摆，因此失败要变成一次有序停机。 */
  private val timer = DelayTimer("actor-delay-timer", e => reportFailure("actor-delay-timer", e))

  /** 系统自己的停机监督 fork。组件从任何受管线程上报失败，都不依赖调用线程属于某个外部作用域。 */
  private val supervisor = ManagedTask.start("actor-system-shutdown") {
    shutdownRequested.await()
    try stopAll()
    catch case NonFatal(e) => logger.error(s"有序停机失败: ${e.getMessage}", e)
  }

  private val healthMonitor = MailboxMonitor.start(() => allHandles, allStopped, logger)

  /** 启动期输出先留在组件本地，整棵事务树提交后才对外可见。 */
  private[actor] def publishFrom(owner: ActorHandle, event: AnyEvent): Unit =
    val publishNow = owner.lifecycleLock.synchronized {
      owner.state match
        case ActorState.Wired | ActorState.Preparing | ActorState.Prepared =>
          if event.topic.isInstanceOf[CommandTopic[?, ?]] then
            throw IllegalStateException(s"组件 ${owner.name} 在 prepare 提交前不能发布命令 ${event.topic}")
          owner.pendingPublications.add(event)
          false
        case ActorState.Starting =>
          owner.pendingPublications.add(event)
          false
        case ActorState.Running => true
        case state => throw IllegalStateException(s"组件 ${owner.name} 已处于 $state, 不能再发布事件 ${event.topic}")
    }
    if publishNow then publishWhileActive(owner, event)

  /** 延迟事件的发布路径：启动期仍缓冲 (提交前的输出不能对外可见)，Running/Stopping 直发。
    *
    * 与 [[publishFrom]] 的差别只在 Stopping：延迟事件是"已经发生的输出等在路上"，
    * 排空阶段仍应送出；而组件在 Stopping 期间主动 `publish` 是越界，仍然抛。 */
  private[actor] def publishDelayedFrom(owner: ActorHandle, event: AnyEvent): Unit =
    if owner.state == ActorState.Stopping then publishWhileActive(owner, event)
    else publishFrom(owner, event)

  /** 仅供事件循环与 onStop 发布返回值：正常排空期间允许 Stopping，隔离后立即熔断。
    *
    * ## 收尾窗口是独立的判据，不是"有没有失败"
    *
    * `onStop` 的返回值必须发得出去 —— 对策略来说那是**撤单指令**。而事件循环异常退出的
    * 组件状态已经是 `Failed`(终态)，若只按状态放行，它的 `onStop` 输出会在这里被拒，
    * 挂单原样留在交易所无人跟踪。`onStop` 允许发布 与 终态禁止发布 是同一模块的两条契约，
    * 冲突点就在这里；解法是把"正在收尾"作为显式事实 ([[ActorHandle.finishing]])，
    * 而不是从状态去猜。
    */
  private def publishOutputFrom(owner: ActorHandle, event: AnyEvent): Unit =
    owner.state match
      case ActorState.Running | ActorState.Stopping => publishWhileActive(owner, event)
      case _ if owner.finishing.get                 => publishWhileActive(owner, event)
      case state => throw IllegalStateException(s"组件 ${owner.name} 已处于 $state, 不能发布运行输出 ${event.topic}")

  private def publishWhileActive(owner: ActorHandle, event: AnyEvent): Unit =
    val read = publicationGate.readLock()
    read.lock()
    try
      if quarantined.get then
        throw IllegalStateException(s"ActorSystem 已隔离卡死组件，拒绝 ${owner.name} 发布 ${event.topic}")
      bus.publish(event)
    finally read.unlock()

  /** 子系统使用独立消息空间，但失败必须沿生命周期所有权上报父组件。 */
  private[actor] def childSystem(owner: ActorHandle, childBus: EventBus): ActorSystem =
    // 子系统的 fork 与本系统同属一个 ox 作用域；停机链接则走 manage (父组件释放时有序停完它)。
    val child = ActorSystem(childBus, e => taskFailed(owner, e), componentStopTimeoutMs)
    manage(owner, child)(_.close())

  /** 排一条归属组件的延迟事件；契约见 [[ActorContext.scheduleEvent]]。
    *
    * 终态即抛：那是调用方的错，静默丢弃只会把它藏起来。停止时未到点的条目由
    * [[stopOwned]] 取消并计入 [[droppedDelayed]]。
    */
  private[actor] def scheduleEvent(owner: ActorHandle, ms: Long, event: AnyEvent): Unit =
    owner.lifecycleLock.synchronized {
      if owner.state.isTerminal then
        throw IllegalStateException(s"组件 ${owner.name} 已处于 ${owner.state}, 不能再排延迟事件 ${event.topic}")
      if ms <= 0 then
        try publishDelayedFrom(owner, event)
        catch case NonFatal(e) => taskFailed(owner, e)
      else
        val ref = AtomicReference[DelayTimer#Entry](null)
        val entry = timer.schedule(ms) {
          try
            owner.lifecycleLock.synchronized {
              if owner.state.isTerminal then droppedDelayed.incrementAndGet(): Unit
              else
                try publishDelayedFrom(owner, event)
                catch case NonFatal(e) => taskFailed(owner, e)
            }
          // 到点即从登记表摘除。少了这一步, schedules 会随每一条延迟回报无界增长
          // (撮合替身与影子柜台每笔成交都排一条), 而停机时"丢弃了多少条"数的就是它。
          finally Option(ref.get).foreach(owner.schedules.remove): Unit
        }
        ref.set(entry)
        owner.schedules.add(entry): Unit
    }

  /** 在组件作用域内起一条 ox fork。组件停止时它会被取消并等待退出。 */
  private[actor] def forkManaged(owner: ActorHandle)(body: => Unit): ManagedTask = owner.lifecycleLock.synchronized {
    if owner.state.isTerminal || owner.state == ActorState.Stopping then
      throw IllegalStateException(s"组件 ${owner.name} 已处于 ${owner.state}, 不能再启动子任务")
    val taskRef = AtomicReference[ManagedTask]()
    val task = ManagedTask.start(s"${owner.name}-task") {
      try body
      catch
        case _: InterruptedException if owner.stopRequested.getCount == 0 => ()
        case e: InterruptedException =>
          Thread.currentThread().interrupt()
          taskFailed(owner, interruptedFailure(owner, "受管任务", e))
        case NonFatal(e) if owner.stopRequested.getCount == 0 =>
          owner.taskCleanupFailures.add(e)
          logger.warn(s"组件 ${owner.name} 的子任务在停机中异常退出: ${e.getMessage}", e)
        case NonFatal(e)  => taskFailed(owner, e)
        case e: Throwable => taskFailed(owner, e)
      finally Option(taskRef.get).foreach(owner.tasks.remove): Unit
    }
    taskRef.set(task)
    owner.tasks.add(task): Unit
    task
  }

  private[actor] def manage[A](owner: ActorHandle, resource: A)(release: A => Unit): A =
    owner.lifecycleLock.synchronized {
      if owner.state.isTerminal || owner.state == ActorState.Stopping then
        release(resource)
        throw IllegalStateException(s"组件 ${owner.name} 已处于 ${owner.state}, 不能再持有新资源")
      owner.cleanups.add(() => release(resource))
      resource
    }

  private def taskFailed(owner: ActorHandle, e: Throwable): Unit =
    val first = assemblyLock.synchronized(owner.terminalFailure.compareAndSet(null, e))
    if first then reportFailure(owner.name, e)
    else logger.warn(s"组件 ${owner.name} 的另一个子任务也失败了: ${e.getMessage}")

  private[actor] def reportManagedFailure(owner: ActorHandle, e: Throwable): Unit = taskFailed(owner, e)

  private def interruptedFailure(
      owner: ActorHandle,
      phase: String,
      cause: InterruptedException,
  ): ComponentInterruptedException =
    ComponentInterruptedException(s"组件 ${owner.name} 的${phase}意外中断", cause)

  /** 起一个顶层 actor */
  def spawn(actor: Actor): ActorHandle = spawnAll(Vector(actor)).head

  /** 原子装配一批顶层组件。
    *
    * 先对整批组件做依赖与处理者基数校验，再把全部邮箱接到总线，最后才依次调用 `onStart`。
    * 任一启动钩子失败，已启动的组件逆序收尾，尚未启动的组件只撤销接线。
    */
  def spawnAll(actors: Seq[Actor]): Vector[ActorHandle] = spawnBatch(None, actors.toVector)

  private[actor] def spawnUnder(parent: Option[ActorHandle], actor: Actor): ActorHandle =
    parent.foreach { owner =>
      owner.state match
        case ActorState.Preparing
            if Option(owner.prepareTask.get).flatMap(_.runnerThread).contains(Thread.currentThread()) => ()
        case ActorState.Running => ()
        case state =>
          throw IllegalStateException(s"组件 ${owner.name} 处于 $state；只能在 onPrepare 同步创建，或在 Running 时动态创建子组件")
    }
    spawnBatch(parent, Vector(actor)).head

  private def spawnBatch(parent: Option[ActorHandle], actors: Vector[Actor]): Vector[ActorHandle] =
    if actors.isEmpty then return Vector.empty
    val specs = actors.map(ComponentGraph.snapshot)
    val wired = scala.collection.mutable.ArrayBuffer.empty[ActorHandle]
    var wiringFailure: Throwable = null
    var joinsActiveTransaction = false
    assemblyLock.synchronized {
      awaitAssemblyTurn(parent)
      try
        if !accepting.get then throw IllegalStateException("ActorSystem 已进入停机阶段，拒绝装配新组件")
        joinsActiveTransaction = parent.exists(p => assemblingHandles.exists(root => descendants(root).contains(p)))
        parent.foreach { p =>
          if (p.state != ActorState.Preparing && p.state != ActorState.Running) || stoppingHandles.contains(p) then
            throw IllegalStateException(s"父组件 ${p.name} 已处于 ${p.state}, 不能再创建子组件")
        }
        ComponentGraph.validateAddition(specs, allHandles, stoppingHandles.toSet)
        specs.foreach(spec => wired += wire(parent, spec))
        // 所有权顺序与显式依赖顺序必须可同时满足，否则 onStop 不可能既守父子契约又守依赖契约。
        ComponentGraph.stopOrder(allHandles.toSet, allHandles)
      catch
        case NonFatal(e) => wiringFailure = e
      assemblingHandles ++= wired
    }

    try
      if wiringFailure != null then throw wiringFailure
      wired.foreach(prepareHook)
      if !joinsActiveTransaction then
        val transactionHandles = wired.toVector.flatMap(descendants).distinct
        val orderedHandles = ComponentGraph.startOrder(transactionHandles.toSet, allHandles)
        orderedHandles.flatMap(handle => Option(handle.terminalFailure.get)).headOption.foreach(throw _)
        // 事件循环先在栅栏后就位；处理能力与启动输出都留到唯一提交点再对外可见。
        orderedHandles.foreach(startLoop)
        orderedHandles.foreach(_.stateRef.set(ActorState.Starting))
        orderedHandles.foreach { handle =>
          startHook(handle)
          ensureTransactionHealthy(orderedHandles)
        }
        // 后台任务失败和启动提交共用 assemblyLock：要么失败先赢、整批回滚；要么提交先赢，
        // 之后的失败属于已运行组件。绝不返回一个已失败却被覆盖成 Running 的句柄。
        assemblyLock.synchronized {
          ensureTransactionHealthyLocked(orderedHandles)
          commitTransaction(orderedHandles)
          orderedHandles.foreach(_.loopCommitted.set(true))
          orderedHandles.foreach { handle =>
            logger.info(
              s"actor started: ${handle.name} interests=${handle.interests.size} " +
                s"handlers=${handle.commandHandlers.size} requirements=${handle.requirements.size}"
            )
          }
          orderedHandles.foreach(_.runGate.countDown())
        }
      wired.toVector
    catch
      case startFailure: Throwable =>
        val rollbackHandles = wired.toVector.flatMap(descendants).toSet
        // 回滚也是一次停止事务。先预留整棵事务树，避免已启动钩子持有的 ctx 在回滚途中再生子组件。
        assemblyLock.synchronized(stoppingHandles ++= rollbackHandles)
        val cleanupFailures =
          try stopHandles(rollbackHandles)
          finally assemblyLock.synchronized(stoppingHandles --= rollbackHandles)
        cleanupFailures.filterNot(_ eq startFailure).foreach(startFailure.addSuppressed)
        throw startFailure
    finally assemblyLock.synchronized {
      assemblingHandles --= wired
      assemblyLock.notifyAll()
    }

  private def ensureTransactionHealthy(handles: Vector[ActorHandle]): Unit = assemblyLock.synchronized {
    ensureTransactionHealthyLocked(handles)
  }

  private def ensureTransactionHealthyLocked(handles: Vector[ActorHandle]): Unit =
    handles.flatMap(handle => Option(handle.terminalFailure.get)).headOption.foreach(throw _)
    Option(failure.get).foreach(throw _)
    if !accepting.get then throw IllegalStateException("ActorSystem 已进入停机阶段，拒绝提交启动事务")
    if quarantined.get then throw IllegalStateException("ActorSystem 已隔离卡死组件，拒绝提交启动事务")

  /** 同一启动事务树内允许继续 spawn；无关装配必须等当前事务提交或回滚。 */
  private def awaitAssemblyTurn(parent: Option[ActorHandle]): Unit =
    def belongsToActiveTree(handle: ActorHandle): Boolean =
      assemblingHandles.exists(root => descendants(root).contains(handle))
    while assemblingHandles.nonEmpty && !parent.exists(belongsToActiveTree) do assemblyLock.wait()

  /** 校验一组不隶属于常驻组件的即时依赖（例如一次 watch 请求）。不产生任何副作用。 */
  def validate(requirements: Set[Requirement], owner: String): Unit = assemblyLock.synchronized {
    awaitAssemblyTurn(None)
    if !accepting.get then throw IllegalStateException(s"ActorSystem 已进入停机阶段，拒绝 $owner")
    ComponentGraph.validateRequirements(requirements, owner, allHandles, stoppingHandles.toSet)
  }

  private def wire(parent: Option[ActorHandle], spec: ActorSpec): ActorHandle =
    val mailbox = bus.subscribeObservations(spec.interests)
    val handle = ActorHandle(
      this,
      spec.actor,
      spec.name,
      spec.interests,
      spec.commandHandlers,
      spec.capabilities,
      spec.requirements,
      mailbox,
      CountDownLatch(1),
      CountDownLatch(1),
      CountDownLatch(1),
    )
    parent match
      case Some(p) => p.children.add(handle): Unit
      case None    => roots.add(handle): Unit
    handle

  private def prepareHook(handle: ActorHandle): Unit =
    handle.stateRef.set(ActorState.Preparing)
    val problem = AtomicReference[Throwable](null)
    val task = ManagedTask.start(s"${handle.name}-prepare") {
      try handle.actor.onPrepare(ActorContext(handle, this))
      catch
        case e: InterruptedException =>
          Thread.currentThread().interrupt()
          problem.set(interruptedFailure(handle, "onPrepare", e))
        case e: Throwable => problem.set(e)
    }
    handle.prepareTask.set(task)
    try
      if !uninterruptible(task.await(componentStopTimeoutMs)) then
        handle.stopHookRun.set(true)
        val timeout = ComponentQuarantinedException(task.timeoutDiagnostic(s"组件 ${handle.name} 的 onPrepare", componentStopTimeoutMs))
        task.cancel()
        quarantine(handle, timeout)
    finally handle.prepareTask.set(null)
    Option(problem.get).foreach(throw _)
    handle.stateRef.set(ActorState.Prepared)

  private def startHook(handle: ActorHandle): Unit =
    // 插件可以连接外部系统；总线输出会留在事务缓冲区，直到整批 onStart 成功。
    val problem = AtomicReference[Throwable](null)
    val task = ManagedTask.start(s"${handle.name}-start") {
      try
        handle.startEntered.set(true)
        handle.actor.onStart(ActorContext(handle, this))
      catch
        case e: InterruptedException =>
          Thread.currentThread().interrupt()
          problem.set(interruptedFailure(handle, "onStart", e))
        case e: Throwable => problem.set(e)
    }
    handle.startTask.set(task)
    try
      if !uninterruptible(task.await(componentStopTimeoutMs)) then
        handle.stopHookRun.set(true)
        val timeout = ComponentQuarantinedException(task.timeoutDiagnostic(s"组件 ${handle.name} 的 onStart", componentStopTimeoutMs))
        task.cancel()
        quarantine(handle, timeout)
    finally handle.startTask.set(null)
    Option(problem.get).foreach(throw _)

  /** 事件循环。
    *
    * ## 正常退出与异常退出走的是两条路
    *
    * **正常退出** = 邮箱已 `done` 并排空完毕，也就是 [[stopOwned]] 发出的停止信号走到了尽头：
    * 收尾 (`onStop`) 就在这条线程上接着跑，这正是"积压事件先处理完、再收尾"的语义。
    *
    * **异常退出** 则不跑 `onStop`。就地跑会让失败组件的收尾抢在它的子组件与依赖方之前，
    * 违反停机拓扑 (依赖方先于提供方、子组件先于父组件)：父会话在 `onEvent` 抛异常时，
    * 它的 `onStop` 会跑在子 Executor 的 `onStop` 之前。收尾改由 [[stopAll]] 按拓扑调度。
    *
    * 异常退出时立刻**摘掉邮箱**：本组件的命令处理者随之从总线索引移除，依赖方随后发出的
    * 命令会在发布时以"处理者基数不足"报错，而不是静默排进一个再也不会被消费的邮箱。
    */
  private def startLoop(handle: ActorHandle): Unit =
    val task = ManagedTask.start(s"${handle.name}-loop") {
      var problem: Throwable = null
      try
        handle.runGate.await()
        if handle.loopCommitted.get then
          handle.mailbox.consumeEach(ev => publishAllFrom(handle, handle.actor.onEvent(ev, nowMs)))
      catch
        case e: InterruptedException =>
          Thread.currentThread().interrupt()
          problem = interruptedFailure(handle, "事件循环", e)
        case e: Throwable => problem = e

      if problem == null then
        if handle.startEntered.get then
          try finish(handle)
          catch
            case cleanup: InterruptedException => problem = interruptedFailure(handle, "onStop", cleanup)
            case cleanup: Throwable            => problem = cleanup
      else detachMailbox(handle)

      if problem != null then handle.terminalFailure.compareAndSet(null, problem)
      if handle.terminalFailure.get != null && handle.state != ActorState.Quarantined then
        handle.stateRef.set(ActorState.Failed)
      handle.finished.countDown()
      Option(handle.terminalFailure.get).foreach { e => if failure.get == null then reportFailure(handle.name, e) }
    }
    handle.loopTask.set(task)
    handle.loopStarted.set(true)

  /** 从总线索引摘除组件邮箱 (含它的命令处理者)。幂等。 */
  private def detachMailbox(handle: ActorHandle): Unit =
    handle.mailbox.close()
    handle.mailboxClosed.set(true)

  /** 锁住整批组件的发布边界，原子激活处理者并冲刷启动输出，然后统一切到 Running。 */
  private def commitTransaction(handles: Vector[ActorHandle]): Unit =
    withLifecycleLocks(handles, 0) {
      val publications = handles.flatMap { handle =>
        val buffered = Vector.newBuilder[AnyEvent]
        var event = handle.pendingPublications.poll()
        while event != null do
          buffered += event
          event = handle.pendingPublications.poll()
        buffered.result()
      }
      val read = publicationGate.readLock()
      read.lock()
      try
        if quarantined.get then throw IllegalStateException("ActorSystem 已隔离卡死组件，拒绝提交启动事务")
        bus.commitHandlersAndPublications(
          handles.map(handle => handle.mailbox -> handle.commandHandlers),
          publications,
        )
        handles.foreach(_.stateRef.set(ActorState.Running))
      finally read.unlock()
    }

  private def withLifecycleLocks[A](handles: Vector[ActorHandle], index: Int)(body: => A): A =
    if index == handles.size then body
    else handles(index).lifecycleLock.synchronized(withLifecycleLocks(handles, index + 1)(body))

  private def publishAllFrom(handle: ActorHandle, events: Vector[AnyEvent]): Unit =
    events.foreach(event => publishOutputFrom(handle, event))

  /** 跑一次 onStop 并发出它产出的事件。**只跑一次** —— 正常退出与异常退出共用这一处。 */
  private def finish(handle: ActorHandle): Unit =
    if handle.stopHookRun.compareAndSet(false, true) then
      // 收尾窗口：期间允许发布，即便组件已因失败落到终态 (见 publishOutputFrom)。
      handle.finishing.set(true)
      // 钩子独占一条线程；只有超时看门狗会 interrupt，届时异常必须可见而非递归等待。
      try publishAllFrom(handle, handle.actor.onStop(nowMs))
      finally handle.finishing.set(false)

  /** 停一个 actor：先停完它的整棵子树，再停它自己。返回时它的 [[Actor.onStop]] 已跑完。
    *
    * 幂等 —— 停机路径上重复调用是常态。
    */
  def stop(handle: ActorHandle): Unit =
    if handle.owner ne this then
      throw IllegalArgumentException(s"组件句柄 ${handle.name} 不属于当前 ActorSystem")
    val attempt = TreeStopAttempt()
    val activeAttempt = handle.treeStopAttempt.compareAndExchange(null, attempt)
    if activeAttempt != null then
      uninterruptible(activeAttempt.finished.await())
      Option(activeAttempt.failure.get).foreach(throw _)
      return
    var reserved = false
    try
      // 临界区只做校验与能力预留，不能持锁等待事件循环/onStop。
      val (removing, newlyReserved) = assemblyLock.synchronized {
        while assemblingHandles.nonEmpty do assemblyLock.wait()
        val removing = descendants(handle).toSet
        ComponentGraph.validateRemoval(removing, allHandles, stoppingHandles.toSet)
        val fresh = removing -- stoppingHandles
        stoppingHandles ++= fresh
        reserved = true
        removing -> fresh
      }
      try
        val failures = stopHandles(removing)
        failures match
          case Vector()      => ()
          case Vector(only)  => throw only
          case many =>
            val aggregate = IllegalStateException(s"停止组件树 ${handle.name} 期间有 ${many.size} 个组件失败")
            many.foreach(aggregate.addSuppressed)
            throw aggregate
      finally assemblyLock.synchronized(stoppingHandles --= newlyReserved)
    catch
      case e: Throwable =>
        attempt.failure.set(e)
        if !reserved then handle.treeStopAttempt.compareAndSet(attempt, null)
        throw e
    finally attempt.finished.countDown()

  private def allHandles: Vector[ActorHandle] = roots.asScala.toVector.flatMap(descendants)

  private def descendants(handle: ActorHandle): Vector[ActorHandle] =
    ComponentGraph.descendants(handle)

  /** 按停机计划尽力停止每个组件。普通清理失败不截断；隔离意味着进程必须立即退出，
    * 继续拆提供者只会让仍在运行的线程访问已释放依赖，因此到此为止。 */
  private def stopHandles(handles: Set[ActorHandle]): Vector[Throwable] =
    val assemblyRank = allHandles.zipWithIndex.toMap.withDefaultValue(-1)
    val (order, planningFailures) =
      try ComponentGraph.stopOrder(handles, allHandles) -> Vector.empty[Throwable]
      catch
        case NonFatal(e) =>
          // 有依据的一条分支, 不是"万一"式兜底: 环确实会被装配校验拒绝, 但那次拒绝发生在
          // **接线之后** (spawnBatch 先 wire 再跑 stopOrder), 于是回滚这一批时图里真的有环。
          // 环意味着不存在同时满足所有权与依赖契约的顺序, 但邮箱、任务和资源仍必须摘掉；
          // 逆装配序让子节点天然先于父节点, 给这种图一个稳定的 best-effort 回收顺序。
          // 环本身作为失败向上传播 (planningFailures)，不会被这条分支吞掉。
          handles.toVector.sortBy(assemblyRank).reverse -> Vector(e)
    val failures = scala.collection.mutable.ArrayBuffer.from(planningFailures)
    val iterator = order.iterator
    var fatal = false
    while iterator.hasNext && !fatal do
      val handle = iterator.next()
      try
        stopInternal(handle)
      catch
        case e: ComponentQuarantinedException =>
          failures += e
          fatal = true
        case e: Throwable => failures += e
    failures.toVector

  /** 不做依赖校验的实际停止；显式 stop 已在入口校验，全系统停机则必须无条件尽力收完。 */
  private def stopInternal(handle: ActorHandle): Unit =
    // Quarantined 只由 quarantine() 设置, 它先 getAndSet 终态原因再改状态 —— 读到这个状态
    // 就一定有原因可抛, 无需再造一个占位异常。
    if handle.state == ActorState.Quarantined then throw handle.terminalFailure.get
    if !handle.stopStarted.compareAndSet(false, true) then
      uninterruptible(handle.stopped.await())
      Option(handle.terminalFailure.get).foreach(throw _)
      return
    uninterruptible(stopOwned(handle))

  /** 单一执行者完成整段停止事务；调用方的中断不能把清理截在半路。 */
  private def stopOwned(handle: ActorHandle): Unit =
    logger.info(s"stopping actor: ${handle.name}")
    val cleanupFailures = scala.collection.mutable.ArrayBuffer.empty[Throwable]
    handle.lifecycleLock.synchronized {
      if !handle.state.isTerminal then handle.stateRef.set(ActorState.Stopping)
      handle.stopRequested.countDown()
      // 未到点的延迟事件已无处可发 (组件即刻被摘除)。丢弃是契约的一部分, 但必须说得出**准确**
      // 数量: cancel() 只在真的拦下一条尚未执行的条目时返回 true, 已经到点跑掉的不算。
      var abandoned = 0
      handle.schedules.forEach(entry => if entry.cancel() then abandoned += 1)
      handle.schedules.clear()
      if abandoned > 0 then
        logger.warn(s"组件 ${handle.name} 停止时丢弃了 $abandoned 条尚未到点的延迟事件 (见 ActorContext.scheduleEvent 契约)")
        droppedDelayed.addAndGet(abandoned.toLong): Unit
      handle.tasks.forEach(_.cancel())
      handle.pendingPublications.clear()
      // 先退订，再关闭邮箱；已缓冲事件仍由事件循环排空。
      detachMailbox(handle)
      handle.mailbox.done()
      handle.runGate.countDown()
    }
    // 事件循环从未起来 (启动回滚)：没有线程会去数 finished，直接放行由下面的收尾钩子接手。
    if !handle.loopStarted.get then handle.finished.countDown()
    if !handle.finished.await(componentStopTimeoutMs, TimeUnit.MILLISECONDS) then
      val timeout = componentTimeout(handle)
      handle.stopHookRun.set(true)
      Option(handle.loopTask.get).foreach(_.cancel())
      quarantine(handle, timeout)
    cleanupFailures ++= runFinishHook(handle)
    cleanupFailures ++= runCleanups(handle)
    cleanupFailures ++= stopTasks(handle)
    removeHandle(handle)
    val terminal = Option(handle.terminalFailure.get)
    val result = if cleanupFailures.nonEmpty then
      val aggregate = IllegalStateException(s"组件 ${handle.name} 停止期间有 ${cleanupFailures.size} 项失败")
      cleanupFailures.distinct.foreach(aggregate.addSuppressed)
      terminal match
        case Some(root) =>
          root.addSuppressed(aggregate)
          root
        case None =>
          handle.terminalFailure.set(aggregate)
          aggregate
    else terminal.orNull
    handle.stateRef.set(if result == null then ActorState.Stopped else ActorState.Failed)
    handle.stopped.countDown()
    logger.info(s"actor stopped: ${handle.name} state=${handle.state}")
    if result != null then throw result

  /** 按停机拓扑跑组件的 `onStop`——**当事件循环没有跑过它的时候**。
    *
    * 两种情形会走到这里：启动回滚 (循环还没起来) 与事件循环异常退出 (见 [[startLoop]])。
    * 正常排空退出的组件已在循环线程上收尾完毕，[[finish]] 的 `stopHookRun` 保证只跑一次。
    *
    * 钩子跑在独立 fork 上并有界等待：插件的 `onStop` 卡死不能把调用者一起拖住。 */
  private def runFinishHook(handle: ActorHandle): Vector[Throwable] =
    if handle.stopHookRun.get || !handle.startEntered.get then return Vector.empty
    val problem = AtomicReference[Throwable](null)
    val task = ManagedTask.start(s"${handle.name}-stop") {
      try finish(handle)
      catch
        case e: InterruptedException =>
          Thread.currentThread().interrupt()
          problem.set(interruptedFailure(handle, "onStop", e))
        case e: Throwable => problem.set(e)
    }
    if !task.await(componentStopTimeoutMs) then
      val timeout = ComponentQuarantinedException(task.timeoutDiagnostic(s"组件 ${handle.name} 的 onStop", componentStopTimeoutMs))
      task.cancel()
      quarantine(handle, timeout)
    Option(problem.get).toVector

  private def componentTimeout(handle: ActorHandle): ComponentQuarantinedException =
    val health = handle.mailboxHealth
    val task = Option(handle.loopTask.get)
    val stack = task.flatMap(_.runnerThread).fold("<线程未启动>")(_.getStackTrace.mkString("\n    at "))
    ComponentQuarantinedException(
      s"组件 ${handle.name} 未在 ${componentStopTimeoutMs}ms 内停止: state=${handle.state} " +
        s"queued=${health.queued} oldestMs=${health.oldestEventAgeMs} inFlightMs=${health.inFlightAgeMs} " +
        s"processed=${health.processed} " +
        s"task=${task.fold("<none>")(_.diagnosticLabel)}\n    at $stack"
    )

  private def quarantine(handle: ActorHandle, cause: ComponentQuarantinedException): Nothing =
    val previous = handle.terminalFailure.getAndSet(cause)
    if previous != null && (previous ne cause) then cause.addSuppressed(previous)
    val write = publicationGate.writeLock()
    write.lock()
    try quarantined.set(true)
    finally write.unlock()
    handle.stateRef.set(ActorState.Quarantined)
    handle.stopped.countDown()
    reportFailure(handle.name, cause)
    logger.error(
      s"组件 ${handle.name} 已隔离：保留尚未释放的资源与依赖，不继续拆除依赖图；进程必须退出",
      cause,
    )
    throw cause

  private def stopTasks(handle: ActorHandle): Vector[Throwable] =
    handle.tasks.asScala.toVector.foreach { task =>
      task.cancel()
      if !task.await(componentStopTimeoutMs) then
        quarantine(
          handle,
          ComponentQuarantinedException(task.diagnostic(handle.name, componentStopTimeoutMs)),
        )
    }
    val taskFailures = handle.taskCleanupFailures.asScala.toVector
    handle.taskCleanupFailures.clear()
    taskFailures

  private def runCleanups(handle: ActorHandle): Vector[Throwable] =
    val registered = handle.lifecycleLock.synchronized {
      handle.cleanups.asScala.toVector.reverse
    }
    val failures = scala.collection.mutable.ArrayBuffer.empty[Throwable]
    val iterator = registered.iterator
    while iterator.hasNext do
      val cleanup = iterator.next()
      val failure = AtomicReference[Throwable](null)
      val task = ManagedTask.start(s"${handle.name}-resource-cleanup") {
        try cleanup()
        catch
          case e: InterruptedException =>
            Thread.currentThread().interrupt()
            failure.set(interruptedFailure(handle, "资源释放", e))
          case e: Throwable => failure.set(e)
      }
      if !task.await(componentStopTimeoutMs) then
        val timeout = ComponentQuarantinedException(
          task.timeoutDiagnostic(s"组件 ${handle.name} 的资源释放", componentStopTimeoutMs)
        )
        task.cancel()
        quarantine(handle, timeout)
      else
        handle.cleanups.remove(cleanup): Unit
        Option(failure.get).foreach(failures += _)
    failures.toVector

  private def removeHandle(handle: ActorHandle): Unit = assemblyLock.synchronized {
    roots.remove(handle): Unit
    allHandles.foreach(_.children.remove(handle))
  }

  // ==================== 有序停机 ====================

  /** 组件失败 —— 记下原因并请求停机。**第一个失败者定调**，之后的只记日志。
    *
    * 停机过程中别的组件跟着失败是常态 (私有流断了, 依赖它的柜台紧接着报错)，
    * 把后续失败也当成"新的停机原因"只会让日志里的根因被淹掉。
    */
  private[actor] def reportFailure(name: String, e: Throwable): Unit =
    val first = assemblyLock.synchronized {
      val won = failure.compareAndSet(null, e)
      if won then accepting.set(false)
      won
    }
    if first then
      logger.error(s"组件 $name 失败, 开始有序停机: ${e.getMessage}", e)
      try failureSink(e)
      catch case callbackFailure: Throwable => if callbackFailure ne e then e.addSuppressed(callbackFailure)
      finally shutdownRequested.countDown()
    else
      logger.warn(s"组件 $name 也失败了 (已在停机中, 根因见上): ${e.getMessage}")

  /** 请求一次正常停机。幂等 —— 中断信号与显式调用可能同时到 */
  def requestShutdown(reason: String): Unit =
    if shutdownRequested.getCount > 0 then logger.warn(s"请求停机: $reason")
    assemblyLock.synchronized(accepting.set(false))
    shutdownRequested.countDown()

  /** 按依赖拓扑停完所有组件：依赖方先于提供方、子组件先于父组件。幂等。
    *
    * 装配顺序只作为无依赖节点之间的稳定倒序，不承载正确性。真正的约束来自显式
    * [[Requirement]] 和生命周期所有权：消费者的 `onStop` 跑完前，提供者必须仍然可用；
    * 子组件的收尾跑完前，父组件也不能先释放它拥有的资源。
    *
    * 普通清理失败不阻断其余；若组件无法停止而进入 Quarantined，则必须保留它仍可能访问的
    * 依赖并尽快退出进程，不能继续拆图后制造 use-after-release。
    */
  def stopAll(): Unit = synchronized {
    shutdownRequested.countDown()
    if allStopped.getCount > 0 then
      // 若失败发生在一批组件的 onStart 内，先等那次装配完成或回滚，再取稳定的根集合。
      val order = assemblyLock.synchronized {
        accepting.set(false)
        while assemblingHandles.nonEmpty do assemblyLock.wait()
        ComponentGraph.stopOrder(allHandles.toSet, allHandles)
      }
      logger.warn(s"有序停机: ${order.size} 个组件, 拓扑顺序 ${order.map(_.name).mkString(" -> ")}")
      val failures = stopHandles(order.toSet).filterNot(_ eq failure.get)
      failures.foreach(e => logger.error(s"停组件时出错, 已继续收尾其余组件: ${e.getMessage}", e))
      if failures.nonEmpty then
        val aggregate = IllegalStateException(s"有序停机期间 ${failures.size} 个组件收尾失败")
        failures.foreach(aggregate.addSuppressed)
        stopFailure.set(aggregate)
      // 关定时器时可能还剩下条目 (例如组件在 Stopping 排空期间排入的), 一并计数。
      val leftover = timer.close()
      if leftover > 0 then droppedDelayed.addAndGet(leftover.toLong): Unit
      allStopped.countDown()
      val dropped = droppedDelayed.get
      if dropped > 0 then logger.warn(s"有序停机共丢弃 $dropped 条尚未到点的延迟事件")
      logger.warn("有序停机完成")
    Option(stopFailure.get).foreach(throw _)
  }

  /** 有序停机 = 关闭系统。
    *
    * 线程回收由 ox 作用域负责 (本系统起的每条 fork 都在构造它的 `unsupervised` 块里)，
    * 但**必须先 close 再离开作用域**：作用域退出会中断并 join 全部 fork，那时再跑 `onStop`
    * 就晚了 —— 撤单指令要在总线与柜台都还活着时发出。见 [[hft.engine.Engine.run]]。 */
  override def close(): Unit = stopAll()

  /** 阻塞到有人请求停机, 停完全部组件, 然后**核心自己退出**。
    *
    * 期间接管中断信号 (SIGINT/SIGTERM): 收到即请求停机, 并让 JVM 的关闭流程等停机跑完
    * —— 否则 hook 一返回 JVM 就退, 撤单指令还在半路上。
    *
    * 若这次停机是由组件失败触发的, 停完之后把那个异常重新抛出: **fail-fast 仍然成立,
    * 只是收尾先跑到了**。调用方的 `awaitShutdown` 因此以原始异常退出。
    */
  def awaitShutdown(): Unit =
    // 唯一不经 ox 的线程: JVM 的 addShutdownHook 只接受 java.lang.Thread, 且它跑在
    // JVM 关闭流程里、不属于任何并发作用域。它不做业务, 只把停机请求转进来并等停机跑完。
    val hook = Thread(
      () =>
        requestShutdown("收到中断信号")
        if !allStopped.await(ActorSystem.ShutdownGraceMs, TimeUnit.MILLISECONDS) then
          logger.error(s"停机未在 ${ActorSystem.ShutdownGraceMs}ms 内完成, JVM 不再等待")
      ,
      "engine-shutdown",
    )
    Runtime.getRuntime.addShutdownHook(hook)
    try
      shutdownRequested.await()
      allStopped.await()
      val cleanup = Option(stopFailure.get)
      Option(failure.get) match
        case Some(root) =>
          cleanup.foreach(root.addSuppressed)
          throw root
        case None => cleanup.foreach(throw _)
    finally
      // JVM 已在关闭流程里时 remove 会抛 —— 那正是 hook 自己触发的这一次, 忽略即可
      try Runtime.getRuntime.removeShutdownHook(hook).discardValue
      catch case _: IllegalStateException => ()

  /** 是否有组件被隔离 —— 即"有一条框架无法停止的线程仍在运行"。
    *
    * 它不是一个可恢复状态：线程还在，任何"替换成功"都是假的。装配层据此把进程终结，
    * 见 [[hft.engine.Engine.run]]。 */
  def isQuarantined: Boolean = quarantined.get

  /** 当前存活的顶层 actor 及其子孙数，供观测与测试 */
  def alive: Int =
    def count(h: ActorHandle): Int = 1 + h.children.asScala.map(count).sum
    roots.asScala.map(count).sum

  extension (b: Boolean) private def discardValue: Unit = ()

object ActorSystem:
  /** 收到中断信号后, 留给有序停机的时间。超过它 JVM 不再等 ——
    * 撤单是几次 REST 往返, 正常在百毫秒级; 留这么宽是为了让积压的邮箱也能排空。 */
  val ShutdownGraceMs: Long = 30_000

  /** 单个组件钩子、事件排空、资源释放与任务退出的最大等待时间；超时后隔离并停止拆图。 */
  val ComponentStopTimeoutMs: Long = 5_000

  val HealthCheckIntervalMs: Long = 1_000
  val MailboxWarnDepth: Long = 10_000
  val MailboxWarnOldestMs: Long = 5_000
  private[actor] val HealthWarningIntervalNanos: Long = 30_000_000_000L
