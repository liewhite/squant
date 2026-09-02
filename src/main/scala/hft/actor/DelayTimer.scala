package hft.actor

import ox.OxUnsupervised

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

/** 延迟事件的统一定时器：**单线程 + (到点时刻, 提交序) 全序**。
  *
  * ## 单线程是顺序保证的承重墙
  *
  * 等延迟的事件按提交序 FIFO 投递，把产生它们的那个 actor 的输出序原样透过延迟传出去。
  * 改成多线程会打乱等延迟事件的相对顺序 —— 对撮合回报来说那意味着"成交先于挂单确认"
  * 这类不可能的序列。同一到点时刻的并列由提交序打破，因此顺序是确定的 (回测要求同一输入
  * 必得同一结果)。
  *
  * 它是 [[ActorSystem]] 作用域内的一条 [[ManagedTask]]，不再是一个自建的
  * `ScheduledExecutorService`：定时器线程与其余框架线程归同一个 ox 作用域管。
  */
private[actor] final class DelayTimer(label: String, onFailure: Throwable => Unit)(using OxUnsupervised):

  /** 一条已排期的延迟工作。
    *
    * [[cancel]] 幂等，并如实回答"这一次取消是否真的拦下了一条**尚未执行**的条目" ——
    * 停机时"丢弃了多少条延迟事件"这个数字就是这么数出来的，不能拿"历史上排过多少条"充当。 */
  private[actor] final class Entry private[DelayTimer] (
      private[DelayTimer] val dueNanos: Long,
      private[DelayTimer] val seq: Long,
      private[DelayTimer] val run: () => Unit,
  ):
    /** 三态: 未定 -> 已执行 / 已取消。由 [[claimed]] 一个 CAS 决定归属。 */
    private[DelayTimer] val claimed = AtomicBoolean(false)

    /** true = 本次调用拦下了一条尚未执行的条目 */
    def cancel(): Boolean = claimed.compareAndSet(false, true)

  private given Ordering[Entry] = Ordering.by(e => (e.dueNanos, e.seq))

  private val lock = new Object
  private val seqGen = AtomicLong(0L)
  private var pending = scala.collection.immutable.TreeSet.empty[Entry]
  private var open = true

  private val worker = ManagedTask.start(label) {
    // 定时器线程静默死亡的后果是"全系统所有延迟事件都不再投递, 且没有任何症状"。
    // 因此这里必须有异常边界: 正常关闭静默退出, 其余一律上报, 变成一次有序停机。
    try
      var draining = true
      while draining do
        // 取出一条到点的条目；未到点则等到它到点或有更早的条目插进来。
        val due = lock.synchronized {
          if !open then None
          else if pending.isEmpty then
            lock.wait()
            None
          else
            val head = pending.head
            val remainingNanos = head.dueNanos - System.nanoTime()
            if remainingNanos > 0 then
              // wait(0) 是"无限等待", 因此不足 1ms 的剩余量也要至少睡 1ms
              lock.wait(math.max(1L, remainingNanos / 1_000_000L))
              None
            else
              pending = pending.tail
              Some(head)
        }
        due match
          // 在锁外执行, 避免回调再排期时自锁。claim 输给 cancel 的条目不执行。
          case Some(entry) => if entry.claimed.compareAndSet(false, true) then entry.run()
          case None        => draining = lock.synchronized(open)
    catch
      case _: InterruptedException if !lock.synchronized(open) => () // close() 造成的中断
      case e: Throwable                                       => onFailure(e)
  }

  /** 排一条延迟工作。返回的句柄可用于取消。 */
  def schedule(delayMs: Long)(body: => Unit): Entry =
    val entry = Entry(System.nanoTime() + delayMs * 1_000_000L, seqGen.getAndIncrement(), () => body)
    lock.synchronized {
      require(open, s"定时器 $label 已关闭, 不能再排期")
      pending += entry
      lock.notifyAll()
    }
    entry

  /** 关闭定时器：不再接受排期，未到点的条目全部丢弃。
    *
    * @return 实际被丢弃 (尚未执行也未被取消) 的条目数 —— 调用方负责把它报出来。 */
  def close(): Int =
    val dropped = lock.synchronized {
      open = false
      val n = pending.count(_.cancel())
      pending = scala.collection.immutable.TreeSet.empty[Entry]
      lock.notifyAll()
      n
    }
    worker.cancel()
    dropped
