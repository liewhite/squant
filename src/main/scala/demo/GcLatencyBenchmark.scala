package demo

import org.openjdk.jmh.annotations.*
import java.util.concurrent.{CountDownLatch, TimeUnit}
import ox.*
import ox.channels.*

/** GC 尾延迟对比基准：在持续 GC 压力下，测量 actor `ask` 往返的延迟分布。
  *
  * 用 SampleTime 模式（JMH 会直接吐出 p50/p90/p99/p999/max 分位），而不是吞吐 —
  * 因为 GC 停顿影响的是**尾延迟**而非平均值。
  *
  * 后台 4 个 churn 线程持续分配：
  *   - 每轮 16KB 年轻代垃圾（高分配速率，逼出频繁 young GC）
  *   - 每轮 64KB 写入一个 4096 槽的轮转 liveSet（~256MB 存活集，逼出晋升 + 老年代回收）
  * 这样无论 G1 还是 ZGC 都会在测量窗口内频繁触发回收，停顿就会落到 ask 的尾延迟上。
  *
  * 用法：相同堆 (-Xms2g -Xmx2g) 下分别加 -XX:+UseG1GC / -XX:+UseZGC 各跑一次对比。
  */
@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.SampleTime))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@org.openjdk.jmh.annotations.Fork(1)
@Threads(1)
class GcLatencyBenchmark:

  class Counter:
    private var v = 0L
    def increment(): Long = { v += 1; v }

  // === actor：fork 活在后台线程的 supervised 作用域里，靠 latch 保活 ===
  private var actorRef: ActorRef[Counter] = scala.compiletime.uninitialized
  private var releaseLatch: CountDownLatch = scala.compiletime.uninitialized
  private var scopeThread: Thread = scala.compiletime.uninitialized

  // === GC 压力发生器 ===
  @volatile private var churning = true
  private var churnThreads: List[Thread] = Nil
  private val liveSet = new Array[Array[Byte]](4096) // 轮转存活集 => 强制晋升

  @Setup(Level.Trial)
  def setup(): Unit =
    val ready = new CountDownLatch(1)
    releaseLatch = new CountDownLatch(1)
    scopeThread = Thread.ofPlatform().daemon(true).unstarted { () =>
      supervised:
        given BufferCapacity = BufferCapacity(1024)
        actorRef = Actor.create(new Counter)
        ready.countDown()
        releaseLatch.await()
    }
    scopeThread.start()
    ready.await()

    churning = true
    churnThreads = (0 until 4).toList.map { id =>
      val t = Thread.ofPlatform().daemon(true).unstarted { () =>
        var i = id
        while churning do
          val young = new Array[Byte](16 * 1024) // 年轻代垃圾
          young(0) = 1
          liveSet(i & 4095) = new Array[Byte](64 * 1024) // 晋升到老年代
          i += 1
      }
      t.start()
      t
    }

  @TearDown(Level.Trial)
  def tearDown(): Unit =
    churning = false
    churnThreads.foreach(_.join(1000))
    releaseLatch.countDown()
    scopeThread.join()

  /** 在 GC 压力下测一次 ask 往返的耗时。 */
  @Benchmark
  def ask_under_gc_pressure(): Long = actorRef.ask(_.increment())
