package hft.actor

import hft.event.{AnyEvent, CommandHandler, EventBus, Interest}
import hft.kernel.CapabilityProvider

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}
import java.util.concurrent.{ConcurrentLinkedQueue, CopyOnWriteArrayList, CountDownLatch, TimeUnit}

/** 组件生命周期状态。状态可从 [[ActorHandle.state]] 读取，故失败不再只存在于日志里。 */
enum ActorState:
  case Wired, Preparing, Prepared, Starting, Running, Stopping, Stopped, Failed, Quarantined

  def isTerminal: Boolean = this == Stopped || this == Failed || this == Quarantined

private[actor] final class TreeStopAttempt:
  val failure = AtomicReference[Throwable](null)
  val finished = CountDownLatch(1)

private[actor] final class ComponentQuarantinedException(message: String) extends IllegalStateException(message)
private[actor] final class ComponentInterruptedException(message: String, cause: InterruptedException)
    extends IllegalStateException(message, cause)

/** 已装配组件的运行时身份，也是停止它及其子树的唯一入口。 */
final class ActorHandle private[actor] (
    private[actor] val owner: ActorSystem,
    private[actor] val actor: Actor,
    val name: String,
    val interests: Set[Interest],
    val commandHandlers: Set[CommandHandler],
    val capabilities: Set[CapabilityProvider],
    val requirements: Set[Requirement],
    private[actor] val mailbox: EventBus.Mailbox,
    private[actor] val stopRequested: CountDownLatch,
    private[actor] val finished: CountDownLatch,
    private[actor] val stopped: CountDownLatch,
):
  private[actor] val children = CopyOnWriteArrayList[ActorHandle]()
  private[actor] val tasks = CopyOnWriteArrayList[ManagedTask]()
  private[actor] val schedules = CopyOnWriteArrayList[DelayTimer#Entry]()
  private[actor] val cleanups = CopyOnWriteArrayList[() => Unit]()
  private[actor] val taskCleanupFailures = CopyOnWriteArrayList[Throwable]()
  private[actor] val pendingPublications = ConcurrentLinkedQueue[AnyEvent]()
  private[actor] val stateRef = AtomicReference(ActorState.Wired)
  private[actor] val terminalFailure = AtomicReference[Throwable](null)
  private[actor] val startEntered = AtomicBoolean(false)
  private[actor] val loopStarted = AtomicBoolean(false)
  private[actor] val stopHookRun = AtomicBoolean(false)
  /** 正在跑 `onStop` —— **收尾窗口**。见 [[ActorSystem.publishOutputFrom]]。 */
  private[actor] val finishing = AtomicBoolean(false)
  private[actor] val stopStarted = AtomicBoolean(false)
  private[actor] val treeStopAttempt = AtomicReference[TreeStopAttempt](null)
  private[actor] val mailboxClosed = AtomicBoolean(false)
  private[actor] val startTask = AtomicReference[ManagedTask](null)
  private[actor] val prepareTask = AtomicReference[ManagedTask](null)
  private[actor] val loopTask = AtomicReference[ManagedTask](null)
  private[actor] val lastMailboxWarningNanos = AtomicLong(0L)
  private[actor] val runGate = CountDownLatch(1)
  private[actor] val loopCommitted = AtomicBoolean(false)
  private[actor] val lifecycleLock = new Object

  def state: ActorState = stateRef.get
  def managedTasks: Int = tasks.size
  def managedResources: Int = cleanups.size
  def mailboxHealth: EventBus.MailboxHealth = mailbox.health
  override def toString: String = s"actor($name)"

/** 组件能访问的最小运行时能力面；监督与停止权只属于 [[ActorSystem]]。 */
final class ActorContext private[actor] (
    private[actor] val handle: ActorHandle,
    private val system: ActorSystem,
):
  def name: String = handle.name
  def publish(event: AnyEvent): Unit = system.publishFrom(handle, event)
  def spawn(child: Actor): ActorHandle = system.spawnUnder(Some(handle), child)
  def childSystem(childBus: EventBus): ActorSystem = system.childSystem(handle, childBus)
  def fork(body: => Unit): ManagedTask = system.forkManaged(handle)(body)
  /** 同步记录组件终态并触发全系统有序停机。幂等；调用后当前控制流必须立即结束。 */
  def reportFailure(cause: Throwable): Unit = system.reportManagedFailure(handle, cause)
  def manage[A](resource: A)(release: A => Unit): A = system.manage(handle, resource)(release)
  /** 排一条**归属本组件**的延迟事件。
    *
    * 契约：
    *   - `ms <= 0` 立即发布 (与 [[publish]] 同路径)。
    *   - 组件进入终态后调用即抛 —— 与 [[publish]]/[[fork]]/[[manage]] 一致，不静默丢弃。
    *   - **停止会丢弃尚未到点的延迟事件**：组件即将被摘除，那条事件已无处可发。丢弃是
    *     可见的 —— [[ActorSystem]] 会以 WARN 报出被丢弃的条数，不做静默处理。
    *     因此需要保证送达的收尾输出应放在 [[Actor.onStop]] 的返回值里，不要靠延迟。
    */
  def scheduleEvent(ms: Long, event: AnyEvent): Unit = system.scheduleEvent(handle, ms, event)

  /** 把一条事件直接放进**本组件自己**的邮箱 (自投递)。
    *
    * 邮箱在停止时会被关闭并排空，此后 offer 的输入不再被处理 —— 与延迟事件同一个道理：
    * 组件正在被摘除。需要保证处理的输入不能靠自投递。 */
  def tell(event: AnyEvent): Unit = handle.mailbox.offer(event)
  def sleepUnlessStopped(ms: Long): Boolean = handle.stopRequested.await(ms, TimeUnit.MILLISECONDS)
