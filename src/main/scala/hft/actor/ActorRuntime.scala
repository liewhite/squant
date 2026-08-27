package hft.actor

import hft.event.{AnyEvent, CommandHandler, EventBus, Interest}
import hft.kernel.CapabilityProvider

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}
import java.util.concurrent.{ConcurrentLinkedQueue, CopyOnWriteArrayList, CountDownLatch, ScheduledFuture, TimeUnit}

/** 组件生命周期状态。状态可从 [[ActorHandle.state]] 读取，故失败不再只存在于日志里。 */
enum ActorState:
  case Wired, Preparing, Prepared, Starting, Running, Stopping, Stopped, Failed, Quarantined

  def isTerminal: Boolean = this == Stopped || this == Failed || this == Quarantined

/** 组件作用域内的一条受管任务。取消与等待由 [[ActorSystem.stop]] 统一执行。 */
final class ManagedTask private[actor] (private val thread: Thread):
  def isAlive: Boolean = thread.isAlive
  private[actor] def cancel(): Unit = thread.interrupt()
  private[actor] def await(timeoutMs: Long): Boolean =
    thread.join(timeoutMs)
    !thread.isAlive
  private[actor] def diagnostic(owner: String, timeoutMs: Long): String =
    s"组件 $owner 的子任务 ${thread.getName} 未在 ${timeoutMs}ms 内停止\n" +
      thread.getStackTrace.mkString("    at ", "\n    at ", "")

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
  private[actor] val schedules = CopyOnWriteArrayList[ScheduledFuture[?]]()
  private[actor] val cleanups = CopyOnWriteArrayList[() => Unit]()
  private[actor] val taskCleanupFailures = CopyOnWriteArrayList[Throwable]()
  private[actor] val pendingPublications = ConcurrentLinkedQueue[AnyEvent]()
  private[actor] val stateRef = AtomicReference(ActorState.Wired)
  private[actor] val terminalFailure = AtomicReference[Throwable](null)
  private[actor] val prepareEntered = AtomicBoolean(false)
  private[actor] val startCompleted = AtomicBoolean(false)
  private[actor] val loopStarted = AtomicBoolean(false)
  private[actor] val stopHookRun = AtomicBoolean(false)
  private[actor] val stopStarted = AtomicBoolean(false)
  private[actor] val treeStopAttempt = AtomicReference[TreeStopAttempt](null)
  private[actor] val mailboxClosed = AtomicBoolean(false)
  private[actor] val startThread = AtomicReference[Thread](null)
  private[actor] val prepareThread = AtomicReference[Thread](null)
  private[actor] val loopThread = AtomicReference[Thread](null)
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
  def manage[A](resource: A)(release: A => Unit): A = system.manage(handle, resource)(release)
  def scheduleEvent(ms: Long, event: AnyEvent): Unit =
    system.schedule(handle, ms) { system.publishFrom(handle, event) }
  def tell(event: AnyEvent): Unit = handle.mailbox.offer(event)
  def sleepUnlessStopped(ms: Long): Boolean = handle.stopRequested.await(ms, TimeUnit.MILLISECONDS)
