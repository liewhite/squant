package hft.actor

import ox.{CancellableFork, OxUnsupervised, forkCancellable}

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import java.util.concurrent.{CountDownLatch, TimeUnit}

/** 框架里**唯一**的起线程入口：一条 ox fork。
  *
  * ## 为什么不再有裸 `Thread.ofVirtual()`
  *
  * 裸线程的生命周期只由创建它的代码记得去 join —— 忘了就是一条活过所属组件的线程，而"忘了"
  * 没有任何症状。经 [[ox.forkCancellable]] 起的 fork 属于**词法作用域**：所属的 `unsupervised`
  * 块无论正常结束还是抛异常退出，都会中断并 join 掉它。于是"线程活过了它的所有者"从一类
  * 需要靠纪律避免的错误，变成了结构上不可能。
  *
  * [[ActorSystem]] 因此必须在一个并发作用域内构造 (`using OxUnsupervised`)，它起的每一条线程
  * ——事件循环、生命周期钩子、受管任务、定时器、健康监督——都是这里的一条 fork。
  *
  * ## 为什么不是 `fork` 而是 `forkCancellable`
  *
  * 组件失败不能就地炸穿作用域：内核要按依赖拓扑停完每个组件、让每个 `onStop` 都跑到，
  * 再把原异常抛出 (见 [[ActorSystem]])。`fork` (supervised) 的异常会立刻取消整个作用域，
  * 跳过这段收尾；`forkCancellable` 是 unsupervised 的，异常留在 fork 里由本类的 body 自行
  * 上报，同时它又支持定向 `cancel` —— 正是"单个组件可被独立停止"所需。
  *
  * ## 有界等待与诊断
  *
  * ox 的 `Fork.join()` 没有超时，而内核对每个钩子、每条任务的退出都有 `componentStopTimeoutMs`
  * 上限，超时要隔离并打出栈。因此本类在 body 外面套一层完成闸门与运行线程引用：
  * [[await]] 给出有界等待，[[diagnostic]] 给出超时那一刻的栈。
  */
final class ManagedTask private (
    private val label: String,
    private val fork: CancellableFork[Unit],
    private val runner: AtomicReference[Thread],
    private val finished: CountDownLatch,
    /** body 与取消方争这一个 CAS：胜者负责让 [[finished]] 落下。见 [[cancel]]。 */
    private val entered: AtomicBoolean,
):
  /** 任务是否仍在运行 (body 尚未退出) */
  def isAlive: Boolean = finished.getCount > 0

  /** 本任务的诊断名。ox 起的虚拟线程没有名字，因此定位靠它而不是线程名。 */
  private[actor] def diagnosticLabel: String = label

  /** 正在跑 body 的线程；body 启动前为 `None` (被取消时可能永不启动)。 */
  private[actor] def runnerThread: Option[Thread] = Option(runner.get)

  /** 请求停止：结束 fork 的嵌套作用域，从而中断 body。不等待退出，等待用 [[await]]。
    *
    * ## 取消早于 body 启动
    *
    * ox 的 `forkCancellable` 用一个 `started` CAS 让"取消"和"执行 body"互斥：取消抢先时
    * **body 根本不会被调用**。若完成闸门只由 body 负责落下，这条路径上它就永远落不下来 ——
    * [[await]] 恒返回 false，`stopTasks` 于是把一个其实什么都没做的任务判成"卡死线程"，
    * 隔离组件并终结进程。
    *
    * 因此这里用同一个手法：body 与取消方争 [[entered]] 这一个 CAS。
    *   - body 胜出 -> 它真的在跑，由它在结束时落闸；取消方的 CAS 失败，只留下中断。
    *   - 取消方胜出 -> body 见到已置位便整段跳过 (它确实什么都没做)，由取消方落闸。
    * 两种情形下"闸门落下"都如实表示"body 不再运行"。
    */
  private[actor] def cancel(): Unit =
    fork.cancelNow()
    if !entered.getAndSet(true) then finished.countDown()

  /** 有界等待 body 退出。true = 已退出。 */
  private[actor] def await(timeoutMs: Long): Boolean = finished.await(timeoutMs, TimeUnit.MILLISECONDS)

  private[actor] def diagnostic(owner: String, timeoutMs: Long): String =
    val stack = runnerThread.fold("<线程未启动>")(_.getStackTrace.mkString("\n    at "))
    s"组件 $owner 的子任务 $label 未在 ${timeoutMs}ms 内停止\n    at $stack"

  private[actor] def timeoutDiagnostic(phase: String, timeoutMs: Long): String =
    val stack = runnerThread.fold("<线程未启动>")(_.getStackTrace.mkString("\n    at "))
    s"$phase 未在 ${timeoutMs}ms 内完成: task=$label\n    at $stack"

object ManagedTask:
  /** 起一条受管 fork。`body` 自己负责捕获异常 —— fork 是 unsupervised 的，抛出去不会有人看见。 */
  private[actor] def start(label: String)(body: => Unit)(using OxUnsupervised): ManagedTask =
    val runner = AtomicReference[Thread](null)
    val finished = CountDownLatch(1)
    val entered = AtomicBoolean(false)
    val fork = forkCancellable {
      // 与 cancel() 争同一个 CAS —— 输了说明取消已经抢先, 本次执行整段跳过。
      if !entered.getAndSet(true) then
        runner.set(Thread.currentThread())
        try body
        finally finished.countDown()
    }
    new ManagedTask(label, fork, runner, finished, entered)
