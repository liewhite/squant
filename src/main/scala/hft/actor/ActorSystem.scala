package hft.actor

import hft.domain.nowMs
import hft.event.{AnyEvent, EventBus, Interest}
import org.slf4j.LoggerFactory
import ox.{Ox, forkDiscard, uninterruptible}

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{CopyOnWriteArrayList, CountDownLatch, Executors, ScheduledExecutorService, TimeUnit}
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/** 一个已启动 actor 的句柄：停它 (连同它的整棵子树) 的唯一入口。
  *
  * 不另设 id 与登记表 —— 句柄本身就是身份，父子关系挂在句柄上，于是"谁 spawn 的谁持有、
  * 谁持有谁负责等"是结构事实，不需要一张全局表来维护。
  */
final class ActorHandle private[actor] (
    val name: String,
    /** 本 actor 声明的订阅 —— 停它之前, 监督者据此知道它接的是哪些信道。
      * 挂在句柄上而不是另建一张登记表: 句柄本身就是身份, 表要维护、会不同步。 */
    val interests: Set[Interest],
    private[actor] val mailbox: EventBus.Mailbox,
    /** 叫醒自驱动循环的定时等待 (事件循环由 mailbox 的关闭叫醒，见 [[ActorSystem.stop]]) */
    private[actor] val stopRequested: CountDownLatch,
    /** actor 的 onStop 已跑完 */
    private[actor] val finished: CountDownLatch,
):
  private[actor] val children = CopyOnWriteArrayList[ActorHandle]()
  override def toString: String = s"actor($name)"

/** actor 在运行期能对系统做的事。
  *
  * 能力面刻意收窄到四件：发事件、起子 actor、fork 自己的线程、可中断地等待。
  * actor 拿不到 [[ActorSystem.stop]] —— 停谁、何时停是**监督者**的决定，不是被监督者的。
  */
final class ActorContext private[actor] (
    private[actor] val handle: ActorHandle,
    private val system: ActorSystem,
)(using Ox):
  def name: String = handle.name

  /** 发布一条事件到总线 */
  def publish(event: AnyEvent): Unit = system.bus.publish(event)

  /** 起一个子 actor。父停机时会先把它 (及它的子孙) 停完并等到收尾结束 */
  def spawn(child: Actor): ActorHandle = system.spawnUnder(Some(handle), child)

  /** 在本 actor 的作用域内 fork 一条线程 (常驻循环, 或一次性的并发调用)。
    *
    * 抛出的异常**不再直接炸掉作用域**，而是上报给系统触发[[ActorSystem.awaitShutdown 有序停机]]：
    * 私有流断线、REST 续期失败这类"子任务失败"要能让全体组件各自跑完 onStop 再退，
    * 而直接取消作用域会把 onStop 一并跳过 —— 那批撤单指令就漏发了。
    *
    * `InterruptedException` 不在捕获之列 (`NonFatal` 排除了它)：那是停机自身的中断信号，
    * 要原样向上传播让 ox 收尾。
    *
    * 常驻循环仍**不随 [[ActorSystem.stop]] 自动结束**，除非它自己用 [[sleepUnlessStopped]]
    * 轮询停止信号 —— 见 [[Actor]] 的停机说明。
    */
  def fork(body: => Unit): Unit = forkDiscard {
    try body
    catch case NonFatal(e) => system.reportFailure(handle.name, e)
  }

  /** 延迟 `ms` 毫秒后把一条事件发到总线。
    *
    * 用于建模"过一段时间才发生"的事 —— 虚拟柜台的下单在途、回报回传都靠它。
    * 定时器**只负责把发布推迟到点，不触碰任何状态**：事件到点后经总线进入 actor 的邮箱，
    * 仍由 actor 线程串行处理。因此柜台的状态依旧只有一个写者。
    *
    * actor 停止后已排期的事件不再发布 —— 停掉的柜台不该再吐回报。
    */
  def scheduleEvent(ms: Long, event: AnyEvent): Unit =
    system.schedule(ms) { if handle.finished.getCount > 0 then system.bus.publish(event) }

  /** 给自己发一条消息 —— 直接进本 actor 的邮箱，**不经总线**。
    *
    * 用途只有一个：把**外部线程**的输入串行化到 actor 线程。柜台的私有推送在 WS 连接
    * 线程上解析出来，而账本的写者必须只有一个；`tell` 让它排进邮箱，与总线来的事件
    * 一起被同一个线程按序消费。
    *
    * 与 [[publish]] 的区别是**可见性**：publish 的东西是给别人看的，tell 的东西是自己的
    * 内部事务。一件纯内部的事没有理由在总线上流动。
    */
  def tell(event: AnyEvent): Unit = handle.mailbox.offer(event)

  /** 本 actor 所在的并发作用域。
    *
    * 限定 `private[hft]`：业务插件只该用 [[fork]] / [[spawn]]，够不着作用域本身。
    * 框架内部装配**嵌套组件**时需要它 —— 虚拟柜台要在自己的私有总线上装一个上游行情源，
    * 那个行情源是完整的 actor，得有地方跑。
    */
  private[hft] def scope: Ox = summon[Ox]

  /** 睡 `ms` 毫秒，除非期间收到了停止信号。返回 true 表示该收工了。
    *
    * 自驱动循环用它代替裸 `Thread.sleep`，就能被协作式地叫停：
    * {{{ while !ctx.sleepUnlessStopped(1000) do ctx.publish(...) }}}
    */
  def sleepUnlessStopped(ms: Long): Boolean = handle.stopRequested.await(ms, TimeUnit.MILLISECONDS)

/** actor 的装配与生命周期树。
  *
  * ## 树形：谁 spawn 的谁负责等
  *
  * [[stop]] 一个 actor 时先递归停完它的子孙、等到它们的 [[Actor.onStop]] 真正跑完，
  * 再停它自己。没有任何地方需要知道整棵树的形状 —— 每层只管自己那一层，递归自然成立。
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
  * 按逆装配序停完每个组件、每个 `onStop` 都跑到、最后核心自己退出，然后把原异常重新抛出。
  *
  * 从前是直接让异常炸穿 ox 作用域，于是所有 fork 被中断、`onStop` 一律跳过 ——
  * 策略的撤单指令漏发，挂单原样留在交易所无人跟踪。**收尾要跑到，和进程要死掉，
  * 是两件不冲突的事**。
  */
final class ActorSystem(private[actor] val bus: EventBus)(using Ox):
  private val logger = LoggerFactory.getLogger(classOf[ActorSystem])
  private val roots = CopyOnWriteArrayList[ActorHandle]()

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

  private[actor] def schedule(ms: Long)(body: => Unit): Unit =
    if ms <= 0 then body
    else scheduler.schedule((() => body): Runnable, ms, TimeUnit.MILLISECONDS): Unit

  /** 起一个顶层 actor */
  def spawn(actor: Actor): ActorHandle = spawnUnder(None, actor)

  private[actor] def spawnUnder(parent: Option[ActorHandle], actor: Actor): ActorHandle =
    // 顺序要紧：先订阅，再 onStart，最后开循环。
    // 订阅排在 onStart 之前 —— onStart 里可能立刻发事件 (子 actor 的启动对齐)，
    // 那时本 actor 的邮箱必须已经挂在总线上，否则这批事件对它就丢了。
    val mailbox = bus.subscribe(actor.interests)
    val handle = ActorHandle(actor.name, actor.interests, mailbox, CountDownLatch(1), CountDownLatch(1))
    parent.fold(roots.add(handle).discardValue)(_.children.add(handle).discardValue)

    actor.onStart(ActorContext(handle, this))
    forkDiscard {
      try
        mailbox.events.foreach(ev => publishAll(actor.onEvent(ev, nowMs)))
        // 收尾不可中断：它要发的是撤单一类的指令，被打断等于漏发。
        // (ox 自己的 Actor 也是这样保护 close 回调的)
        finish(actor)
      catch
        case NonFatal(e) =>
          // 事件循环自己炸了：先让它把该收的尾收掉 (仍可能有挂单要撤), 再上报 ——
          // 由系统按逆装配序停完其余组件, 而不是就地把作用域取消掉。
          finish(actor)
          reportFailure(actor.name, e)
      finally handle.finished.countDown()
    }
    logger.info(s"actor started: ${actor.name} interests=${actor.interests.size}")
    handle

  private def publishAll(events: Vector[AnyEvent]): Unit = events.foreach(bus.publish)

  /** 跑一次 onStop 并发出它产出的事件。**只跑一次** —— 正常退出与异常退出共用这一处。 */
  private def finish(actor: Actor): Unit =
    if finished.add(actor) then
      try uninterruptible(publishAll(actor.onStop(nowMs)))
      catch case NonFatal(e) => logger.error(s"${actor.name} 的 onStop 出错: ${e.getMessage}", e)

  /** 已经跑过 onStop 的 actor —— 身份比较, 不看名字 (同名实例可以有多个) */
  private val finished = java.util.Collections.newSetFromMap(
    java.util.IdentityHashMap[Actor, java.lang.Boolean]()
  ).asScala

  /** 停一个 actor：先停完它的整棵子树，再停它自己。返回时它的 [[Actor.onStop]] 已跑完。
    *
    * 幂等 —— 停机路径上重复调用是常态。
    */
  def stop(handle: ActorHandle): Unit =
    logger.info(s"stopping actor: ${handle.name}")
    stopChildren(handle)
    handle.stopRequested.countDown() // 叫醒自驱动循环
    handle.mailbox.close()           // 先退订：不再有新事件进这个邮箱
    handle.mailbox.done()            // 再关闭：已缓冲的事件仍会被消费完
    handle.finished.await()
    // 再扫一遍：父的事件循环退出**之前**仍可能在 onEvent 里 ctx.spawn 出新的子。
    // 那批晚到的子若不停，就成了没人管、也没人退订的孤儿。此刻循环已退出，不会再有新的。
    stopChildren(handle)
    roots.remove(handle).discardValue
    logger.info(s"actor stopped: ${handle.name}")

  private def stopChildren(handle: ActorHandle): Unit =
    var batch = handle.children.asScala.toVector
    while batch.nonEmpty do
      batch.foreach(stop)
      handle.children.removeAll(batch.asJava).discardValue
      batch = handle.children.asScala.toVector

  // ==================== 有序停机 ====================

  /** 把系统推向停机的第一个失败。空表示这是一次正常停机 (收到中断信号 / 显式请求) */
  private val failure = AtomicReference[Throwable](null)
  private val shutdownRequested = CountDownLatch(1)
  private val allStopped = CountDownLatch(1)

  /** 组件失败 —— 记下原因并请求停机。**第一个失败者定调**，之后的只记日志。
    *
    * 停机过程中别的组件跟着失败是常态 (私有流断了, 依赖它的柜台紧接着报错)，
    * 把后续失败也当成"新的停机原因"只会让日志里的根因被淹掉。
    */
  private[actor] def reportFailure(name: String, e: Throwable): Unit =
    if failure.compareAndSet(null, e) then
      logger.error(s"组件 $name 失败, 开始有序停机: ${e.getMessage}", e)
      shutdownRequested.countDown()
      // **失败自己把停机跑起来**, 不能指望有人正在 awaitShutdown 上等着 (测试、嵌套系统、
      // 被当库用的场景都没有)。否则失败就成了"被吞掉", 系统带着一个死掉的组件继续跑 ——
      // 那比原来的级联取消更糟。
      //
      // 必须另起一条线程: 在失败者自己的线程里 stopAll 会等到自己的 finished, 死锁。
      forkDiscard {
        stopAll()
        // 收尾都跑完了, 现在才让作用域失败 —— fail-fast 与"onStop 要跑到"不冲突,
        // 只是先后问题。异常原样抛出, 调用方 (启动器的 supervised 块) 据此非零退出。
        throw e
      }
    else
      logger.warn(s"组件 $name 也失败了 (已在停机中, 根因见上): ${e.getMessage}")

  /** 请求一次正常停机。幂等 —— 中断信号与显式调用可能同时到 */
  def requestShutdown(reason: String): Unit =
    if shutdownRequested.getCount > 0 then logger.warn(s"请求停机: $reason")
    shutdownRequested.countDown()

  /** 按**逆装配序**停完所有顶层组件 —— 后装的先停。幂等。
    *
    * 与"消费者先起、生产者后起"的装配序对称: 生产者先停, 它们收尾时补发的最后一批事件
    * 仍有人消费。策略比柜台后装, 于是策略先停 —— 它 `onStop` 里的撤单指令发出去时,
    * 柜台还活着、还接得住。反过来的话那批撤单会发到一条没人接的信道上。
    *
    * 单个组件停不下来不阻断其余: 停机路径上"尽力停完每一个"比"卡在第一个"有用。
    */
  def stopAll(): Unit = synchronized {
    if allStopped.getCount > 0 then
      val order = roots.asScala.toVector.reverse
      logger.warn(s"有序停机: ${order.size} 个顶层组件, 逆装配序 ${order.map(_.name).mkString(" -> ")}")
      order.foreach { h =>
        try stop(h)
        catch case NonFatal(e) => logger.error(s"停 ${h.name} 时出错, 继续停其余: ${e.getMessage}", e)
      }
      allStopped.countDown()
      logger.warn("有序停机完成")
  }

  /** 阻塞到有人请求停机, 停完全部组件, 然后**核心自己退出**。
    *
    * 期间接管中断信号 (SIGINT/SIGTERM): 收到即请求停机, 并让 JVM 的关闭流程等停机跑完
    * —— 否则 hook 一返回 JVM 就退, 撤单指令还在半路上。
    *
    * 若这次停机是由组件失败触发的, 停完之后把那个异常重新抛出: **fail-fast 仍然成立,
    * 只是收尾先跑到了**。调用方 (启动器的 `supervised` 块) 因此以非零状态退出。
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
      stopAll()
      Option(failure.get).foreach(throw _)
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
