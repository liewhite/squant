package hft.actor

import hft.domain.nowMs
import hft.event.{AnyEvent, EventBus, Interest}
import org.slf4j.LoggerFactory
import ox.{Ox, forkDiscard, uninterruptible}

import java.util.concurrent.{CopyOnWriteArrayList, CountDownLatch, Executors, ScheduledExecutorService, TimeUnit}
import scala.jdk.CollectionConverters.*

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
    * 是 supervised fork：抛出的异常级联终止整个引擎 (fail-fast)。常驻循环**不随
    * [[ActorSystem.stop]] 结束**，除非它自己用 [[sleepUnlessStopped]] 轮询停止信号
    * —— 见 [[Actor]] 的停机说明。
    */
  def fork(body: => Unit): Unit = forkDiscard(body)

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
  * ## fail-fast
  *
  * 每个 actor 的事件循环是所在 ox `supervised` 作用域内的 fork，任何异常都级联取消整个
  * 作用域、进程非零退出，由外层重新拉起并靠启动对齐恢复一致。**不做局部重启** ——
  * 一个崩掉的策略留下的挂单与仓位归谁管是个没有好答案的问题，而重启后的对齐有答案。
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
        uninterruptible(publishAll(actor.onStop(nowMs)))
      finally handle.finished.countDown()
    }
    logger.info(s"actor started: ${actor.name} interests=${actor.interests.size}")
    handle

  private def publishAll(events: Vector[AnyEvent]): Unit = events.foreach(bus.publish)

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

  /** 当前存活的顶层 actor 及其子孙数，供观测与测试 */
  def alive: Int =
    def count(h: ActorHandle): Int = 1 + h.children.asScala.map(count).sum
    roots.asScala.map(count).sum

  extension (b: Boolean) private def discardValue: Unit = ()
