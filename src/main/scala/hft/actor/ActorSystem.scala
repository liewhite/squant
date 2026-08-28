package hft.actor

import hft.domain.nowMs
import hft.event.{AnyEvent, CommandTopic, EventBus}
import org.slf4j.LoggerFactory
import ox.uninterruptible

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import java.util.concurrent.{CopyOnWriteArrayList, CountDownLatch, Executors, ScheduledExecutorService, ScheduledFuture, TimeUnit}
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
  */
final class ActorSystem(
    private[actor] val bus: EventBus,
    private val failureSink: Throwable => Unit = _ => (),
    private val componentStopTimeoutMs: Long = ActorSystem.ComponentStopTimeoutMs,
) extends AutoCloseable:
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

  /** 系统自己的停机监督线程。组件从任何受管线程上报失败都不依赖调用线程属于某个外部作用域。 */
  Thread
    .ofVirtual()
    .name("actor-system-shutdown")
    .start { () =>
      shutdownRequested.await()
      try stopAll()
      catch case NonFatal(e) => logger.error(s"有序停机失败: ${e.getMessage}", e)
    }

  MailboxMonitor.start(() => allHandles, allStopped, logger)

  /** 延迟发布用的定时器。
    *
    * **单线程是顺序保证的承重墙**：等延迟的事件按提交序 FIFO 投递，把产生它们的那个
    * actor 的输出序原样透过延迟传出去。改成多线程会打乱等延迟事件的相对顺序 ——
    * 对撮合回报来说那意味着"成交先于挂单确认"这类不可能的序列。
    * daemon 线程，进程退出即回收。
    */
  private val scheduler: ScheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor { r =>
      val t = Thread(r, "actor-scheduler")
      t.setDaemon(true)
      t
    }

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

  /** 仅供事件循环与 onStop 发布返回值：正常排空期间允许 Stopping，隔离后立即熔断。 */
  private def publishOutputFrom(owner: ActorHandle, event: AnyEvent): Unit =
    owner.state match
      case ActorState.Running | ActorState.Stopping => publishWhileActive(owner, event)
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
    val child = ActorSystem(childBus, e => taskFailed(owner, e), componentStopTimeoutMs)
    manage(owner, child)(_.stopAll())

  private[actor] def schedule(owner: ActorHandle, ms: Long)(body: => Unit): Unit =
    if ms <= 0 then
      owner.lifecycleLock.synchronized {
        if !owner.state.isTerminal && owner.state != ActorState.Stopping then
          try body
          catch case NonFatal(e) => taskFailed(owner, e)
      }
    else owner.lifecycleLock.synchronized {
      if owner.state.isTerminal || owner.state == ActorState.Stopping then return
      val ref = AtomicReference[ScheduledFuture[?]]()
      val registered = CountDownLatch(1)
      val future = scheduler.schedule(
        (() =>
          try
            registered.await()
            owner.lifecycleLock.synchronized {
              if !owner.state.isTerminal && owner.state != ActorState.Stopping then
                try body
                catch case NonFatal(e) => taskFailed(owner, e)
            }
          catch
            case _: InterruptedException if owner.stopRequested.getCount == 0 => ()
          finally owner.schedules.remove(ref.get): Unit
        ): Runnable,
        ms,
        TimeUnit.MILLISECONDS,
      )
      ref.set(future)
      owner.schedules.add(future)
      registered.countDown()
    }

  /** 在组件作用域内启动一条虚拟线程。组件停止时它会被 interrupt 并等待退出。 */
  private[actor] def forkManaged(owner: ActorHandle)(body: => Unit): ManagedTask = owner.lifecycleLock.synchronized {
    if owner.state.isTerminal || owner.state == ActorState.Stopping then
      throw IllegalStateException(s"组件 ${owner.name} 已处于 ${owner.state}, 不能再启动子任务")
    val taskRef = AtomicReference[ManagedTask]()
    val thread = Thread
      .ofVirtual()
      .name(s"actor-${owner.name}-task")
      .unstarted { () =>
        try body
        catch
          case _: InterruptedException if owner.stopRequested.getCount == 0 => ()
          case e: InterruptedException =>
            Thread.currentThread().interrupt()
            taskFailed(owner, interruptedFailure(owner, "受管任务", e))
          case NonFatal(e) if owner.stopRequested.getCount == 0 =>
            owner.taskCleanupFailures.add(e)
            logger.warn(s"组件 ${owner.name} 的子任务在停机中异常退出: ${e.getMessage}", e)
          case NonFatal(e) => taskFailed(owner, e)
          case e: Throwable => taskFailed(owner, e)
        finally owner.tasks.remove(taskRef.get): Unit
      }
    val task = ManagedTask(thread)
    taskRef.set(task)
    owner.tasks.add(task)
    thread.start()
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
        case ActorState.Preparing if owner.prepareThread.get eq Thread.currentThread() => ()
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
    val finished = CountDownLatch(1)
    val thread = Thread.ofVirtual().name(s"actor-${handle.name}-prepare").unstarted { () =>
      try handle.actor.onPrepare(ActorContext(handle, this))
      catch
        case e: InterruptedException =>
          Thread.currentThread().interrupt()
          problem.set(interruptedFailure(handle, "onPrepare", e))
        case e: Throwable => problem.set(e)
      finally
        handle.prepareThread.set(null)
        finished.countDown()
    }
    handle.prepareThread.set(thread)
    thread.start()
    if !uninterruptible(finished.await(componentStopTimeoutMs, TimeUnit.MILLISECONDS)) then
      handle.stopHookRun.set(true)
      thread.interrupt()
      val timeout = ComponentQuarantinedException(
        s"组件 ${handle.name} 的 onPrepare 未在 ${componentStopTimeoutMs}ms 内完成: thread=${thread.getName}\n" +
          thread.getStackTrace.mkString("    at ", "\n    at ", "")
      )
      quarantine(handle, timeout)
    Option(problem.get).foreach(throw _)
    handle.stateRef.set(ActorState.Prepared)

  private def startHook(handle: ActorHandle): Unit =
    // 插件可以连接外部系统；总线输出会留在事务缓冲区，直到整批 onStart 成功。
    val problem = AtomicReference[Throwable](null)
    val finished = CountDownLatch(1)
    val thread = Thread.ofVirtual().name(s"actor-${handle.name}-start").unstarted { () =>
      try
        handle.startEntered.set(true)
        handle.actor.onStart(ActorContext(handle, this))
      catch
        case e: InterruptedException =>
          Thread.currentThread().interrupt()
          problem.set(interruptedFailure(handle, "onStart", e))
        case e: Throwable => problem.set(e)
      finally
        handle.startThread.set(null)
        finished.countDown()
    }
    handle.startThread.set(thread)
    thread.start()
    if !uninterruptible(finished.await(componentStopTimeoutMs, TimeUnit.MILLISECONDS)) then
      handle.stopHookRun.set(true)
      thread.interrupt()
      val timeout = ComponentQuarantinedException(
        s"组件 ${handle.name} 的 onStart 未在 ${componentStopTimeoutMs}ms 内完成: thread=${thread.getName}\n" +
          thread.getStackTrace.mkString("    at ", "\n    at ", "")
      )
      quarantine(handle, timeout)
    Option(problem.get).foreach(throw _)

  private def startLoop(handle: ActorHandle): Unit =
    val thread = Thread
      .ofVirtual()
      .name(s"actor-${handle.name}-loop")
      .unstarted { () =>
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
        if handle.startEntered.get then
          try finish(handle)
          catch
            case cleanup: InterruptedException =>
              val wrapped = interruptedFailure(handle, "onStop", cleanup)
              if problem == null then problem = wrapped else problem.addSuppressed(wrapped)
            case cleanup: Throwable =>
              if problem == null then problem = cleanup else problem.addSuppressed(cleanup)
        if problem != null then handle.terminalFailure.compareAndSet(null, problem)
        if handle.terminalFailure.get != null && handle.state != ActorState.Quarantined then
          handle.stateRef.set(ActorState.Failed)
        handle.finished.countDown()
        Option(handle.terminalFailure.get).foreach { e => if failure.get == null then reportFailure(handle.name, e) }
      }
    handle.loopThread.set(thread)
    thread.start()
    handle.loopStarted.set(true)

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
      // 钩子独占事件循环线程；只有超时看门狗会 interrupt，届时异常必须可见而非递归等待。
      publishAllFrom(handle, handle.actor.onStop(nowMs))

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
          // 环本身意味着不存在满足全部契约的顺序，但仍必须摘掉邮箱、任务和资源。
          // 逆装配序对子节点天然先于父节点，并给异常图一个稳定的 best-effort 回收顺序。
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
    if handle.state == ActorState.Quarantined then
      throw Option(handle.terminalFailure.get).getOrElse(
        ComponentQuarantinedException(s"组件 ${handle.name} 已隔离")
      )
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
      handle.schedules.forEach(_.cancel(false))
      handle.schedules.clear()
      handle.tasks.forEach(_.cancel())
      handle.pendingPublications.clear()
      // 先退订，再关闭邮箱；已缓冲事件仍由事件循环排空。
      handle.mailbox.close()
      handle.mailboxClosed.set(true)
      handle.mailbox.done()
      handle.runGate.countDown()
    }
    if !handle.loopStarted.get then startRollbackFinish(handle)
    if !handle.finished.await(componentStopTimeoutMs, TimeUnit.MILLISECONDS) then
      val timeout = componentTimeout(handle)
      handle.stopHookRun.set(true)
      Option(handle.loopThread.get).foreach(_.interrupt())
      quarantine(handle, timeout)
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

  /** 启动回滚时还没有事件循环，由独立线程执行 onStop，避免插件钩子卡死调用者。 */
  private def startRollbackFinish(handle: ActorHandle): Unit =
    val thread = Thread.ofVirtual().name(s"actor-${handle.name}-rollback-stop").unstarted { () =>
      var problem: Throwable = null
      if handle.startEntered.get then
        try finish(handle)
        catch
          case e: InterruptedException => problem = interruptedFailure(handle, "onStop", e)
          case e: Throwable            => problem = e
      if problem != null then handle.terminalFailure.compareAndSet(null, problem)
      if handle.terminalFailure.get != null && handle.state != ActorState.Quarantined then
        handle.stateRef.set(ActorState.Failed)
      handle.finished.countDown()
    }
    handle.loopThread.set(thread)
    thread.start()

  private def componentTimeout(handle: ActorHandle): ComponentQuarantinedException =
    val health = handle.mailboxHealth
    val thread = handle.loopThread.get
    val stack = Option(thread).fold("<线程未启动>")(_.getStackTrace.mkString("\n    at "))
    ComponentQuarantinedException(
      s"组件 ${handle.name} 未在 ${componentStopTimeoutMs}ms 内停止: state=${handle.state} " +
        s"queued=${health.queued} oldestMs=${health.oldestEventAgeMs} inFlightMs=${health.inFlightAgeMs} " +
        s"processed=${health.processed} " +
        s"thread=${Option(thread).fold("<none>")(_.getName)}\n    at $stack"
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
      val finished = CountDownLatch(1)
      val thread = Thread.ofVirtual().name(s"actor-${handle.name}-resource-cleanup").start { () =>
        try cleanup()
        catch
          case e: InterruptedException =>
            Thread.currentThread().interrupt()
            failure.set(interruptedFailure(handle, "资源释放", e))
          case e: Throwable => failure.set(e)
        finally finished.countDown()
      }
      if !finished.await(componentStopTimeoutMs, TimeUnit.MILLISECONDS) then
        thread.interrupt()
        val timeout = ComponentQuarantinedException(
          s"组件 ${handle.name} 的资源释放未在 ${componentStopTimeoutMs}ms 内完成: thread=${thread.getName}\n" +
            thread.getStackTrace.mkString("    at ", "\n    at ", "")
        )
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
      scheduler.shutdownNow()
      allStopped.countDown()
      logger.warn("有序停机完成")
    Option(stopFailure.get).foreach(throw _)
  }

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
