package demo

import org.openjdk.jmh.annotations.*
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicLong
import ox.*
import ox.channels.*

/** JMH 基准：对比"凡事走 mailbox 的 actor"与"直接操作可变状态"的单次自增吞吐。
  *
  * 四个对照组（均为对一个 long 计数器 +1）：
  *   - baseline_atomic        : AtomicLong.incrementAndGet —— 无锁 CAS，理论上限
  *   - baseline_synchronized  : synchronized { var += 1 } —— 传统锁
  *   - actor_ask              : actorRef.ask(_.increment) —— 经 mailbox 往返（含 CompletableFuture）
  *   - actor_tell             : actorRef.tell(_.increment) —— 经 mailbox 单向（背压下=消费速率）
  *
  * 结论看点：actor 的开销不在"闭包做消息"，而在 channel 交接 + 虚拟线程唤醒；
  * ask 还要叠加一次同步往返。和 baseline 的差距即 actor 模型的固有代价。
  */
@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@org.openjdk.jmh.annotations.Fork(1) // 全限定：避开 ox.Fork 同名冲突
class ActorBenchmark:

  // === 被 actor 保护的领域对象：纯可变计数器 ===
  class Counter:
    private var value = 0L
    def increment(): Long = { value += 1; value }

  // === baseline 1: 无锁 CAS ===
  private val atomic = new AtomicLong(0)

  // === baseline 2: synchronized ===
  private val lock = new Object
  private var syncCounter = 0L

  // === actor：fork 活在后台线程持有的 supervised 作用域里，靠 latch 保活 ===
  private var actorRef: ActorRef[Counter] = scala.compiletime.uninitialized
  private var releaseLatch: CountDownLatch = scala.compiletime.uninitialized
  private var scopeThread: Thread = scala.compiletime.uninitialized

  @Setup(Level.Trial)
  def setup(): Unit =
    val ready = new CountDownLatch(1)
    releaseLatch = new CountDownLatch(1)
    scopeThread = Thread.ofPlatform().daemon(true).unstarted { () =>
      supervised:
        given BufferCapacity = BufferCapacity(1024) // 放大 mailbox，让 tell 不至于一直撞背压
        actorRef = Actor.create(new Counter)
        ready.countDown()
        releaseLatch.await() // 阻塞在此 => 作用域不退出 => actor 的 fork 持续存活
    }
    scopeThread.start()
    ready.await()

  @TearDown(Level.Trial)
  def tearDown(): Unit =
    releaseLatch.countDown() // 释放 => supervised 退出 => actor 关闭
    scopeThread.join()

  @Benchmark
  def baseline_atomic(): Long = atomic.incrementAndGet()

  @Benchmark
  def baseline_synchronized(): Long =
    lock.synchronized { syncCounter += 1; syncCounter }

  @Benchmark
  def actor_ask(): Long = actorRef.ask(_.increment())

  @Benchmark
  def actor_tell(): Unit = actorRef.tell(_.increment())
