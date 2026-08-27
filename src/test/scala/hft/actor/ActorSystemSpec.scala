package hft.actor

import hft.domain.*
import hft.event.{AnyEvent, CommandHandler, CommandTopic, Event, EventBus, Interest, Topics}
import hft.kernel.{Capability, CapabilityProvider, Cardinality}
import ox.supervised

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import scala.jdk.CollectionConverters.*
import hft.TestUnits.given

/** actor 生命周期：树形停机、收尾、退订、fail-fast。 */
class ActorSystemSpec extends munit.FunSuite:
  private val ex = Exchange.Binance
  private val btc = Instrument(ex, "BTCUSDT")
  private val t0 = 1_700_000_000_000L

  private def bbo(px: Double = 100.0) = BBO(ex, "BTCUSDT", px, Coin(1.0), px + 0.1, Coin(1.0), t0)

  private object TestCommand extends CommandTopic[String, String]("testCommand", Cardinality.ExactlyOne):
    def keyOf(payload: String): String = payload

  private object SharedCommand extends CommandTopic[String, String]("sharedCommand", Cardinality.AtLeastOne):
    def keyOf(payload: String): String = payload

  private val StorageCapability = Capability[String]("storage")

  /** 记录收到的事件；可选地在停机时补发一条 */
  private class Recorder(
      override val name: String,
      seen: ConcurrentLinkedQueue[String],
      emitOnStop: Option[String] = None,
      onEachEvent: CountDownLatch = CountDownLatch(0),
  ) extends Actor:
    override def interests: Set[Interest] = Set(Interest.Keyed(Topics.Bbo, Set(btc)))
    override def onEvent(event: AnyEvent, now: Timestamp): Vector[AnyEvent] =
      event.as(Topics.Bbo).foreach(b => seen.add(s"$name:${b.bidPrice}"))
      onEachEvent.countDown()
      Vector.empty
    override def onStop(now: Timestamp): Vector[AnyEvent] =
      seen.add(s"$name:stopped")
      emitOnStop.map(sym => Event.local(Topics.Fill, Fill(AccountId.Live, ex, sym, Side.Long, 1.0, 1.0, now))).toVector

  test("spawn 后收到订阅的事件, stop 后不再收到 (退订生效)"):
    supervised:
      val bus = EventBus()
      val system = ActorSystem(bus)
      val seen = ConcurrentLinkedQueue[String]()
      val gotFirst = CountDownLatch(1)
      val h = system.spawn(Recorder("a", seen, onEachEvent = gotFirst))

      bus.publish(Event.at(Topics.Bbo, bbo(100.0), t0))
      assert(gotFirst.await(2, TimeUnit.SECONDS), "应收到第一条")

      system.stop(h)
      bus.publish(Event.at(Topics.Bbo, bbo(999.0), t0)) // 停掉之后发的
      assertEquals(seen.asScala.toVector, Vector("a:100.0", "a:stopped"), "停掉后不该再收到任何事件")
      assertEquals(system.alive, 0)

  test("stop 返回时 onStop 已跑完, 且其产出的事件送达了下游"):
    // 收尾意图 (撤单/平仓) 必须在退订之前发出, 那时下游还活着 —— 顺序反了就是漏发指令
    supervised:
      val bus = EventBus()
      val system = ActorSystem(bus)
      val seen = ConcurrentLinkedQueue[String]()
      val downstream = bus.subscribe(Set(Interest.All(Topics.Fill)))
      val h = system.spawn(Recorder("a", seen, emitOnStop = Some("BTCUSDT")))

      system.stop(h)
      assert(seen.asScala.toVector.contains("a:stopped"), "stop 返回时 onStop 必须已跑完")
      assertEquals(downstream.events.receive().as(Topics.Fill).map(_.symbol), Some("BTCUSDT"))

  test("树形停机: 先停子孙、自下而上, 父的 stop 返回时整棵子树已收尾"):
    supervised:
      val bus = EventBus()
      val system = ActorSystem(bus)
      val order = ConcurrentLinkedQueue[String]()

      class Node(override val name: String, depth: Int) extends Actor:
        override def onPrepare(ctx: ActorContext): Unit =
          if depth > 0 then ctx.spawn(Node(s"$name.child", depth - 1))
          ()
        override def onStop(now: Timestamp): Vector[AnyEvent] =
          order.add(name)
          Vector.empty

      val root = system.spawn(Node("root", 2))
      assertEquals(system.alive, 3, "root + 两层子孙")
      system.stop(root)
      assertEquals(
        order.asScala.toVector,
        Vector("root.child.child", "root.child", "root"),
        "必须自下而上收尾",
      )
      assertEquals(system.alive, 0, "整棵子树都被摘除")

  test("并发重复 stop(parent) 共享同一棵树停止事务, 不会越过子组件"):
    val system = ActorSystem(EventBus())
    val childStopping = CountDownLatch(1)
    val releaseChild = CountDownLatch(1)
    val returned = CountDownLatch(2)
    val order = ConcurrentLinkedQueue[String]()

    class Child extends Actor:
      override def name = "blocking-child"
      override def onStop(now: Timestamp): Vector[AnyEvent] =
        childStopping.countDown()
        releaseChild.await()
        order.add(name)
        Vector.empty

    class Parent extends Actor:
      override def name = "parent"
      override def onPrepare(ctx: ActorContext): Unit = ctx.spawn(Child()): Unit
      override def onStop(now: Timestamp): Vector[AnyEvent] =
        order.add(name)
        Vector.empty

    val parent = system.spawn(Parent())
    val first = Thread.ofVirtual().start { () =>
      try system.stop(parent)
      finally returned.countDown()
    }
    assert(childStopping.await(2, TimeUnit.SECONDS))
    val second = Thread.ofVirtual().start { () =>
      try system.stop(parent)
      finally returned.countDown()
    }
    Thread.sleep(50)
    assertEquals(returned.getCount, 2L, "重复调用必须等待整棵树完成")
    assert(order.isEmpty, "父组件不能越过仍在收尾的子组件")
    releaseChild.countDown()
    first.join(); second.join()
    assertEquals(order.asScala.toVector, Vector("blocking-child", "parent"))

  test("慢消费者的邮箱积压、高水位与处理耗时由核心统一观测"):
    val bus = EventBus()
    val system = ActorSystem(bus)
    val entered = CountDownLatch(1)
    val release = CountDownLatch(1)
    val processed = CountDownLatch(3)

    class Slow extends Actor:
      override def name = "slow-consumer"
      override def interests = Set(Interest.Keyed(Topics.Bbo, Set(btc)))
      override def onEvent(event: AnyEvent, now: Timestamp): Vector[AnyEvent] =
        entered.countDown()
        release.await()
        processed.countDown()
        Vector.empty

    val handle = system.spawn(Slow())
    bus.publish(Event.at(Topics.Bbo, bbo(100), t0))
    assert(entered.await(2, TimeUnit.SECONDS))
    bus.publish(Event.at(Topics.Bbo, bbo(101), t0))
    bus.publish(Event.at(Topics.Bbo, bbo(102), t0))

    val lagging = handle.mailboxHealth
    assertEquals(lagging.queued, 2L)
    assert(lagging.highWaterMark >= 2L)
    release.countDown()
    assert(processed.await(2, TimeUnit.SECONDS))
    val recovered = handle.mailboxHealth
    assertEquals(recovered.queued, 0L)
    assertEquals(recovered.processed, 3L)
    assert(recovered.maxProcessingNanos > 0L)
    system.stop(handle)
    system.close()

  test("事件处理超时后隔离组件: 保留资源且恢复线程不能发布迟到命令"):
    val bus = EventBus()
    val system = ActorSystem(bus, componentStopTimeoutMs = 100)
    val entered = CountDownLatch(1)
    val release = CountDownLatch(1)
    val resourceReleased = CountDownLatch(1)
    val lateCommand = CountDownLatch(1)

    class Sink extends Actor:
      override def name = "command-sink"
      override def commandHandlers = Set(CommandHandler.command(TestCommand, "k"))
      override def onEvent(event: AnyEvent, now: Timestamp): Vector[AnyEvent] =
        event.as(TestCommand).foreach(_ => lateCommand.countDown())
        Vector.empty

    class Stuck extends Actor:
      override def name = "stuck-consumer"
      override def interests = Set(Interest.Keyed(Topics.Bbo, Set(btc)))
      override def onStart(ctx: ActorContext): Unit =
        ctx.manage("owned-resource")(_ => resourceReleased.countDown())
      override def onEvent(event: AnyEvent, now: Timestamp): Vector[AnyEvent] =
        entered.countDown()
        while release.getCount > 0 do
          try release.await()
          catch case _: InterruptedException => ()
        Vector(Event.local(TestCommand, "k"))

    system.spawn(Sink())
    val handle = system.spawn(Stuck())
    bus.publish(Event.at(Topics.Bbo, bbo(), t0))
    assert(entered.await(2, TimeUnit.SECONDS))
    Thread.sleep(10)
    assert(handle.mailboxHealth.inFlightAgeMs > 0L, "单条正在处理的消息也必须暴露卡住时长")
    val started = System.nanoTime()
    val failure = intercept[IllegalStateException](system.stop(handle))
    val elapsedMs = (System.nanoTime() - started) / 1_000_000L
    assert(elapsedMs < 2_000L, s"stop 不得永久挂起, 实际 ${elapsedMs}ms")
    assert(failure.getMessage.contains("stuck-consumer"), failure.getMessage)
    assert(failure.getMessage.contains("thread=actor-stuck-consumer-loop"), failure.getMessage)
    assertEquals(handle.state, ActorState.Quarantined)
    assertEquals(resourceReleased.getCount, 1L, "线程未退出时不能释放它仍可能访问的资源")
    assertEquals(handle.managedResources, 1)

    release.countDown()
    assert(!lateCommand.await(300, TimeUnit.MILLISECONDS), "隔离组件恢复后不得发布迟到命令")
    assertEquals(handle.state, ActorState.Quarantined)
    assertEquals(system.alive, 2, "隔离句柄与其依赖都保留到进程退出，不能伪装成已回收")

  test("资源释放超时后不并发执行更早登记的 cleanup"):
    val system = ActorSystem(EventBus(), componentStopTimeoutMs = 100)
    val releaseNewest = CountDownLatch(1)
    val olderCleanup = CountDownLatch(1)

    class ResourceOwner extends Actor:
      override def name = "stuck-cleanup"
      override def onStart(ctx: ActorContext): Unit =
        ctx.manage("older")(_ => olderCleanup.countDown())
        ctx.manage("newer") { _ =>
          while releaseNewest.getCount > 0 do
            try releaseNewest.await()
            catch case _: InterruptedException => ()
        }

    val handle = system.spawn(ResourceOwner())
    intercept[IllegalStateException](system.stop(handle))
    assertEquals(handle.state, ActorState.Quarantined)
    assertEquals(olderCleanup.getCount, 1L, "后登记资源没释放完时不能越过它释放底层资源")
    assertEquals(handle.managedResources, 2)
    releaseNewest.countDown()

  test("受管任务忽略中断时进入隔离, 不摘除仍有活线程的句柄"):
    val system = ActorSystem(EventBus(), componentStopTimeoutMs = 100)
    val taskStarted = CountDownLatch(1)
    val releaseTask = CountDownLatch(1)

    class TaskOwner extends Actor:
      override def name = "stuck-task"
      override def onStart(ctx: ActorContext): Unit =
        ctx.fork {
          taskStarted.countDown()
          while releaseTask.getCount > 0 do
            try releaseTask.await()
            catch case _: InterruptedException => ()
        }

    val handle = system.spawn(TaskOwner())
    assert(taskStarted.await(2, TimeUnit.SECONDS))
    intercept[IllegalStateException](system.stop(handle))
    assertEquals(handle.state, ActorState.Quarantined)
    assertEquals(handle.managedTasks, 1)
    assertEquals(system.alive, 1)
    releaseTask.countDown()

  test("onStart 卡死会限时隔离, 不再永久占住装配与停机"):
    val system = ActorSystem(EventBus(), componentStopTimeoutMs = 100)
    val entered = CountDownLatch(1)
    val releaseStart = CountDownLatch(1)

    class StuckStart extends Actor:
      override def name = "stuck-start"
      override def onStart(ctx: ActorContext): Unit =
        entered.countDown()
        while releaseStart.getCount > 0 do
          try releaseStart.await()
          catch case _: InterruptedException => ()

    val started = System.nanoTime()
    val failure = intercept[IllegalStateException](system.spawn(StuckStart()))
    val elapsedMs = (System.nanoTime() - started) / 1_000_000L
    assert(entered.getCount == 0)
    assert(elapsedMs < 2_000L, s"onStart 超时必须解除装配等待, 实际 ${elapsedMs}ms")
    assert(failure.getMessage.contains("onStart"), failure.getMessage)
    val shutdown = intercept[IllegalStateException](system.awaitShutdown())
    assert(shutdown.getMessage.contains("onStart"), shutdown.getMessage)
    assertEquals(system.alive, 1, "隔离启动线程仍可能运行，句柄不能伪装成已回收")
    releaseStart.countDown()

  test("onPrepare 只允许同步装配子组件, 异步任务不能逃出启动事务"):
    supervised:
      val system = ActorSystem(EventBus())
      val attempted = CountDownLatch(1)
      val rejected = ConcurrentLinkedQueue[Throwable]()

      class Child extends Actor:
        override def name = "async-child"

      class Parent extends Actor:
        override def name = "waiting-parent"
        override def onPrepare(ctx: ActorContext): Unit =
          ctx.fork {
            try ctx.spawn(Child())
            catch case e: IllegalStateException => rejected.add(e)
            finally attempted.countDown()
          }
          if !attempted.await(2, TimeUnit.SECONDS) then sys.error("异步 spawn 没有完成")

      val handle = system.spawn(Parent())
      assertEquals(rejected.size, 1)
      assert(rejected.peek().getMessage.contains("只能在 onPrepare 同步创建"))
      assertEquals(system.alive, 1)
      system.stop(handle)

  test("停机排空邮箱: 积压事件必须处理完才退出"):
    // 早先用 select(停止信号, 事件流) 的写法在这里会丢事件 —— 两路同时就绪时选谁不确定。
    // 丢掉的若是成交回报, 本地仓位就与交易所发散了。
    supervised:
      val bus = EventBus()
      val system = ActorSystem(bus)
      val gate = CountDownLatch(1)
      val processed = ConcurrentLinkedQueue[Double]()

      class Slow extends Actor:
        override def name = "slow"
        override def interests: Set[Interest] = Set(Interest.Keyed(Topics.Bbo, Set(btc)))
        override def onEvent(event: AnyEvent, now: Timestamp): Vector[AnyEvent] =
          gate.await() // 第一条卡住, 后续几条在邮箱里积压
          event.as(Topics.Bbo).foreach(b => processed.add(b.bidPrice.value))
          Vector.empty

      val h = system.spawn(Slow())
      Vector(1.0, 2.0, 3.0).foreach(px => bus.publish(Event.at(Topics.Bbo, bbo(px), t0)))

      val stopping = ox.fork(system.stop(h))
      Thread.sleep(50) // 让 stop 先走到关闭邮箱那一步
      gate.countDown()
      stopping.join()

      assertEquals(processed.asScala.toVector, Vector(1.0, 2.0, 3.0), "积压的事件一条都不能丢")

  test("停机后向该 actor 投递不抛 —— 退订与投递重叠是正常竞态"):
    supervised:
      val bus = EventBus()
      val system = ActorSystem(bus)
      val h = system.spawn(Recorder("a", ConcurrentLinkedQueue[String]()))
      system.stop(h)
      // 若 publish 用的是会抛的 send, 这里会把发布方 (另一个 actor 的循环) 炸掉并级联整机
      bus.publish(Event.at(Topics.Bbo, bbo(), t0))

  test("stop 幂等: 重复停、停不存在的 id 都不抛"):
    supervised:
      val bus = EventBus()
      val system = ActorSystem(bus)
      val h = system.spawn(Recorder("a", ConcurrentLinkedQueue[String]()))
      system.stop(h)
      system.stop(h) // 停机路径上重复调用是常态

  test("跨 ActorSystem 的 handle 在任何停止副作用前被拒绝"):
    supervised:
      val first = ActorSystem(EventBus())
      val second = ActorSystem(EventBus())
      val foreign = second.spawn(Recorder("foreign", ConcurrentLinkedQueue[String]()))

      val rejected = intercept[IllegalArgumentException](first.stop(foreign))
      assert(rejected.getMessage.contains("不属于当前 ActorSystem"), rejected.getMessage)
      assertEquals(foreign.state, ActorState.Running)
      assertEquals(second.alive, 1)
      second.stop(foreign)

  test("组件停止后旧 context 不能继续向总线发布"):
    supervised:
      val system = ActorSystem(EventBus())

      class CapturesContext extends Actor:
        override def name = "captures-context"
        override def onStart(ctx: ActorContext): Unit = CapturesContext.ctx = ctx
      object CapturesContext:
        @volatile var ctx: ActorContext = scala.compiletime.uninitialized

      val handle = system.spawn(CapturesContext())
      system.stop(handle)
      val rejected = intercept[IllegalStateException] {
        CapturesContext.ctx.publish(Event.at(Topics.Bbo, bbo(), t0))
      }
      assert(rejected.getMessage.contains("不能再发布事件"), rejected.getMessage)

  test("sleepUnlessStopped: 自驱动循环能被协作式叫停"):
    supervised:
      val bus = EventBus()
      val system = ActorSystem(bus)
      val ticks = ConcurrentLinkedQueue[Long]()
      val started = CountDownLatch(1)

      class Ticker extends Actor:
        override def name = "ticker"
        override def onStart(ctx: ActorContext): Unit =
          ctx.fork {
            started.countDown()
            while !ctx.sleepUnlessStopped(5) do ticks.add(1L)
          }

      val h = system.spawn(Ticker())
      assert(started.await(2, TimeUnit.SECONDS))
      assertEquals(h.managedTasks, 1, "ctx.fork 必须登记到组件句柄")
      system.stop(h)
      val afterStop = ticks.size
      Thread.sleep(60) // 若循环没停, 这段时间会再攒十来个 tick
      assertEquals(ticks.size, afterStop, "停止信号必须叫醒定时等待")
      assertEquals(h.managedTasks, 0, "stop 返回时受管任务必须全部回收")

  test("受管任务在停机中抛出的非中断异常成为可见的停止失败"):
    supervised:
      val system = ActorSystem(EventBus())
      val started = CountDownLatch(1)

      class FailingCleanupTask extends Actor:
        override def name = "failing-cleanup-task"
        override def onStart(ctx: ActorContext): Unit = ctx.fork {
          started.countDown()
          try CountDownLatch(1).await()
          catch case _: InterruptedException => throw IllegalStateException("task cleanup failed")
        }

      val handle = system.spawn(FailingCleanupTask())
      assert(started.await(2, TimeUnit.SECONDS))
      val failure = intercept[IllegalStateException](system.stop(handle))
      assert(failure.getSuppressed.exists(_.getMessage == "task cleanup failed"), failure.toString)
      assertEquals(handle.state, ActorState.Failed)

  test("受管任务在正常运行期收到非框架中断会触发全系统失败"):
    val system = ActorSystem(EventBus())
    val taskStarted = CountDownLatch(1)
    val throwUnexpectedInterrupt = CountDownLatch(1)

    class UnexpectedInterrupt extends Actor:
      override def name = "unexpected-task-interrupt"
      override def onStart(ctx: ActorContext): Unit = ctx.fork {
        taskStarted.countDown()
        throwUnexpectedInterrupt.await()
        throw InterruptedException("task interrupted outside shutdown")
      }

    val handle = system.spawn(UnexpectedInterrupt())
    assert(taskStarted.await(2, TimeUnit.SECONDS))
    throwUnexpectedInterrupt.countDown()
    val failure = intercept[IllegalStateException](system.awaitShutdown())
    assert(failure.getMessage.contains("受管任务意外中断"), failure.getMessage)
    assertEquals(failure.getCause.getMessage, "task interrupted outside shutdown")
    assertEquals(handle.state, ActorState.Failed)
    assertEquals(system.alive, 0)

  test("受管资源: onStop 后按逆序释放, 单项失败不跳过其余且状态可见"):
    supervised:
      val system = ActorSystem(EventBus())
      val trace = ConcurrentLinkedQueue[String]()

      class OwnsResources extends Actor:
        override def name = "resource-owner"
        override def onStart(ctx: ActorContext): Unit =
          ctx.manage("first")(_ => trace.add("release:first"))
          ctx.manage("second") { _ =>
            trace.add("release:second")
            throw IllegalStateException("release failed")
          }
          ()
        override def onStop(now: Timestamp): Vector[AnyEvent] =
          trace.add("stop")
          Vector.empty

      val handle = system.spawn(OwnsResources())
      assertEquals(handle.managedResources, 2)
      val e = intercept[IllegalStateException](system.stop(handle))
      assert(e.getMessage.contains("停止期间"), e.getMessage)
      assertEquals(trace.asScala.toVector, Vector("stop", "release:second", "release:first"))
      assertEquals(handle.managedResources, 0)
      assertEquals(handle.state, ActorState.Failed)
      assertEquals(system.alive, 0)

  test("资源 release 抛 InterruptedException 仍可见且继续逆序清理"):
    val system = ActorSystem(EventBus())
    val trace = ConcurrentLinkedQueue[String]()

    class InterruptedRelease extends Actor:
      override def name = "interrupted-release"
      override def onStart(ctx: ActorContext): Unit =
        ctx.manage("first")(_ => trace.add("release:first"))
        ctx.manage("second") { _ =>
          trace.add("release:second")
          throw InterruptedException("release interrupted")
        }

    val handle = system.spawn(InterruptedRelease())
    val failure = intercept[IllegalStateException](system.stop(handle))
    assert(
      failure.getSuppressed.exists(_.getCause.isInstanceOf[InterruptedException]),
      failure.toString,
    )
    assertEquals(trace.asScala.toVector, Vector("release:second", "release:first"))
    assertEquals(handle.managedResources, 0)
    assertEquals(handle.state, ActorState.Failed)
    assertEquals(system.alive, 0)
    system.close()

  test("批量启动事务: 后一个 onStart 失败 -> 已接线组件逆序回滚, 不留订阅"):
    supervised:
      val bus = EventBus()
      val system = ActorSystem(bus)
      val trace = ConcurrentLinkedQueue[String]()

      class Starts(override val name: String, fail: Boolean) extends Actor:
        override def interests: Set[Interest] = Set(Interest.Keyed(Topics.Bbo, Set(btc)))
        override def onStart(ctx: ActorContext): Unit =
          trace.add(s"start:$name")
          if fail then sys.error(s"$name start failed")
        override def onStop(now: Timestamp): Vector[AnyEvent] =
          trace.add(s"stop:$name")
          Vector.empty

      val e = intercept[RuntimeException](system.spawnAll(Vector(Starts("a", false), Starts("b", true))))
      assert(e.getMessage.contains("b start failed"))
      assertEquals(trace.asScala.toVector, Vector("start:a", "start:b", "stop:b", "stop:a"))
      assertEquals(system.alive, 0)
      assertEquals(bus.subscriberCount(Topics.Bbo, btc), 0, "回滚必须撤销全部接线")

  test("onStart 抛 InterruptedException 仍完整回滚订阅与资源"):
    val bus = EventBus()
    val system = ActorSystem(bus)
    val trace = ConcurrentLinkedQueue[String]()

    class InterruptedStart extends Actor:
      override def name = "interrupted-start"
      override def interests: Set[Interest] = Set(Interest.Keyed(Topics.Bbo, Set(btc)))
      override def onStart(ctx: ActorContext): Unit =
        ctx.manage("resource")(_ => trace.add("release"))
        throw InterruptedException("start interrupted")
      override def onStop(now: Timestamp): Vector[AnyEvent] =
        trace.add("stop")
        Vector.empty

    val failure = intercept[IllegalStateException](system.spawn(InterruptedStart()))
    assert(failure.getMessage.contains("onStart意外中断"), failure.getMessage)
    assertEquals(failure.getCause.getMessage, "start interrupted")
    assertEquals(trace.asScala.toVector, Vector("stop", "release"))
    assertEquals(bus.subscriberCount(Topics.Bbo, btc), 0)
    assertEquals(system.alive, 0)
    system.close()

  test("onStop 抛 InterruptedException 会传播且重复 stop 仍返回同一失败"):
    val system = ActorSystem(EventBus())

    class InterruptedStop extends Actor:
      override def name = "interrupted-stop"
      override def onStop(now: Timestamp): Vector[AnyEvent] =
        throw InterruptedException("stop interrupted")

    val handle = system.spawn(InterruptedStop())
    val first = intercept[IllegalStateException](system.stop(handle))
    val repeated = intercept[IllegalStateException](system.stop(handle))
    assert(first.getMessage.contains("onStop意外中断"), first.getMessage)
    assertEquals(first.getCause.getMessage, "stop interrupted")
    assert(repeated eq first, "重复 stop 必须观察同一个终止根因")
    assertEquals(handle.state, ActorState.Failed)
    assertEquals(system.alive, 0)

  test("prepare 提交前命令处理能力不可见"):
    val bus = EventBus()
    val system = ActorSystem(bus)
    val entered = CountDownLatch(1)
    val proceed = CountDownLatch(1)
    val handles = ConcurrentLinkedQueue[ActorHandle]()
    val failures = ConcurrentLinkedQueue[Throwable]()

    class SlowProvider extends Actor:
      override def name = "slow-provider"
      override def commandHandlers: Set[CommandHandler] = Set(CommandHandler.command(TestCommand, "k"))
      override def onPrepare(ctx: ActorContext): Unit =
        entered.countDown()
        proceed.await()

    val starter = Thread.ofVirtual().start { () =>
      try handles.add(system.spawn(SlowProvider()))
      catch case e: Throwable => failures.add(e)
    }
    assert(entered.await(2, TimeUnit.SECONDS))
    val notCommitted = intercept[IllegalStateException](bus.publish(Event.local(TestCommand, "k")))
    assert(notCommitted.getMessage.contains("实际 0 个"), notCommitted.getMessage)
    proceed.countDown()
    starter.join()

    assert(failures.isEmpty, failures.asScala.mkString("; "))
    assertEquals(bus.handlerCount(TestCommand, "k"), 1)
    system.stop(handles.remove())

  test("prepare 禁止命令副作用, start 在能力激活后可以发起命令"):
    supervised:
      val bus = EventBus()
      val system = ActorSystem(bus)

      class IllegalPrepare extends Actor:
        override def name = "illegal-prepare-command"
        override def onPrepare(ctx: ActorContext): Unit = ctx.publish(Event.local(TestCommand, "k"))

      val rejected = intercept[IllegalStateException](system.spawn(IllegalPrepare()))
      assert(rejected.getMessage.contains("prepare 提交前不能发布命令"), rejected.getMessage)
      assertEquals(system.alive, 0)

      val handled = CountDownLatch(1)
      class StartsWithCommand extends Actor:
        override def name = "starts-with-command"
        override def commandHandlers: Set[CommandHandler] = Set(CommandHandler.command(TestCommand, "k"))
        override def onStart(ctx: ActorContext): Unit = ctx.publish(Event.local(TestCommand, "k"))
        override def onEvent(event: AnyEvent, now: Timestamp): Vector[AnyEvent] =
          event.as(TestCommand).foreach(_ => handled.countDown())
          Vector.empty

      val handle = system.spawn(StartsWithCommand())
      assert(handled.await(2, TimeUnit.SECONDS), "start 发出的命令应在整批进入 Running 后由事件循环消费")
      system.stop(handle)

  test("组件声明在装配入口只求值一次, 验证与接线共享同一快照"):
    supervised:
      val bus = EventBus()
      val system = ActorSystem(bus)
      val interestReads = AtomicInteger()
      val handlerReads = AtomicInteger()
      val capabilityReads = AtomicInteger()
      val requirementReads = AtomicInteger()

      class StatefulDeclarations extends Actor:
        override def name = "stateful-declarations"
        override def interests: Set[Interest] =
          interestReads.incrementAndGet()
          Set(Interest.Keyed(Topics.Bbo, Set(btc)))
        override def commandHandlers: Set[CommandHandler] =
          val read = handlerReads.incrementAndGet()
          if read == 1 then Set(CommandHandler.command(TestCommand, "k")) else Set.empty
        override def capabilities: Set[CapabilityProvider] =
          capabilityReads.incrementAndGet()
          Set.empty
        override def requirements: Set[Requirement] =
          requirementReads.incrementAndGet()
          Set.empty

      val handle = system.spawn(StatefulDeclarations())
      assertEquals(interestReads.get, 1)
      assertEquals(handlerReads.get, 1)
      assertEquals(capabilityReads.get, 1)
      assertEquals(requirementReads.get, 1)
      assertEquals(bus.handlerCount(TestCommand, "k"), 1)
      system.stop(handle)

  test("prepare 期普通事件对已运行组件隔离, 失败丢弃、成功提交后冲刷"):
    supervised:
      val bus = EventBus()
      val system = ActorSystem(bus)
      val commands = ConcurrentLinkedQueue[String]()
      val commandSeen = CountDownLatch(1)

      class Provider extends Actor:
        override def name = "command-provider"
        override def commandHandlers: Set[CommandHandler] = Set(CommandHandler.command(TestCommand, "k"))
        override def onEvent(event: AnyEvent, now: Timestamp): Vector[AnyEvent] =
          event.as(TestCommand).foreach { value =>
            commands.add(value)
            commandSeen.countDown()
          }
          Vector.empty

      class ExistingStrategy extends Actor:
        override def name = "existing-strategy"
        override def interests: Set[Interest] = Set(Interest.Keyed(Topics.Bbo, Set(btc)))
        override def requirements: Set[Requirement] = Set(Requirement.command(TestCommand, "k"))
        override def onEvent(event: AnyEvent, now: Timestamp): Vector[AnyEvent] =
          event.as(Topics.Bbo).map(_ => Event.local(TestCommand, "k")).toVector

      class StartupPublisher extends Actor:
        override def name = "startup-publisher"
        override def onPrepare(ctx: ActorContext): Unit = ctx.publish(Event.at(Topics.Bbo, bbo(), t0))

      class FailingSibling extends Actor:
        override def name = "failing-sibling"
        override def onStart(ctx: ActorContext): Unit = sys.error("startup failed")

      val existing = system.spawnAll(Vector(Provider(), ExistingStrategy()))
      intercept[RuntimeException](system.spawnAll(Vector(StartupPublisher(), FailingSibling())))
      assertEquals(commands.asScala.toVector, Vector.empty, "回滚批次的普通事件不能泄露为外部命令")

      val publisher = system.spawn(StartupPublisher())
      assert(commandSeen.await(2, TimeUnit.SECONDS), "提交后必须冲刷启动期事件")
      assertEquals(commands.asScala.toVector, Vector("k"))
      system.stop(publisher)
      existing.reverse.foreach(system.stop)

  test("onPrepare 嵌套 spawn 与最外层共享提交点, 兄弟失败时一起回滚"):
    supervised:
      val bus = EventBus()
      val system = ActorSystem(bus)

      class ChildProvider extends Actor:
        override def name = "nested-provider"
        override def commandHandlers: Set[CommandHandler] = Set(CommandHandler.command(TestCommand, "k"))

      class Parent extends Actor:
        override def name = "parent"
        override def onPrepare(ctx: ActorContext): Unit = ctx.spawn(ChildProvider()): Unit

      class FailingSibling extends Actor:
        override def name = "failing-sibling"
        override def onStart(ctx: ActorContext): Unit = sys.error("sibling failed")

      val e = intercept[RuntimeException](system.spawnAll(Vector(Parent(), FailingSibling())))
      assert(e.getMessage.contains("sibling failed"), e.getMessage)
      assertEquals(bus.handlerCount(TestCommand, "k"), 0, "嵌套子组件不能提前提交处理能力")
      assertEquals(system.alive, 0)

  test("硬依赖环在启动副作用前拒绝, 且接线完整回滚"):
    supervised:
      val bus = EventBus()
      val system = ActorSystem(bus)
      val capabilityA = Capability[String]("capability-a")
      val capabilityB = Capability[String]("capability-b")
      val starts = ConcurrentLinkedQueue[String]()

      class ComponentA extends Actor:
        override def name = "component-a"
        override def interests: Set[Interest] = Set(Interest.Keyed(Topics.Bbo, Set(btc)))
        override def capabilities: Set[CapabilityProvider] = Set(CapabilityProvider.provide(capabilityA, "k"))
        override def requirements: Set[Requirement] = Set(Requirement.capability(capabilityB, "k"))
        override def onStart(ctx: ActorContext): Unit = starts.add(name)

      class ComponentB extends Actor:
        override def name = "component-b"
        override def interests: Set[Interest] = Set(Interest.Keyed(Topics.Bbo, Set(btc)))
        override def capabilities: Set[CapabilityProvider] = Set(CapabilityProvider.provide(capabilityB, "k"))
        override def requirements: Set[Requirement] = Set(Requirement.capability(capabilityA, "k"))
        override def onStart(ctx: ActorContext): Unit = starts.add(name)

      val cycle = intercept[IllegalStateException](system.spawnAll(Vector(ComponentA(), ComponentB())))
      assert(cycle.getMessage.contains("形成停机环"), cycle.getMessage)
      assert(starts.isEmpty, "依赖环必须在 onStart 之前拒绝")
      assertEquals(system.alive, 0)
      assertEquals(bus.subscriberCount(Topics.Bbo, btc), 0)

  test("硬依赖在 onStart 之前校验, 同一批中的提供者可以满足依赖"):
    supervised:
      val bus = EventBus()
      val system = ActorSystem(bus)
      val starts = ConcurrentLinkedQueue[String]()

      class Consumer extends Actor:
        override def name = "consumer"
        override def requirements: Set[Requirement] = Set(Requirement.command(TestCommand, "k", "测试能力"))
        override def onStart(ctx: ActorContext): Unit = starts.add(name)

      val missing = intercept[IllegalStateException](system.spawn(Consumer()))
      assert(missing.getMessage.contains("硬依赖未满足"), missing.getMessage)
      assert(starts.isEmpty, "校验失败时不能调用 onStart")

      class Provider extends Actor:
        override def name = "provider"
        override def commandHandlers: Set[CommandHandler] = Set(CommandHandler.command(TestCommand, "k"))

      val handles = system.spawnAll(Vector(Consumer(), Provider()))
      assertEquals(starts.asScala.toVector, Vector("consumer"), "整批先接线, 顺序不影响依赖满足")
      handles.foreach(system.stop)

  test("非消息组件也能用通用 capability 声明上下游硬依赖"):
    supervised:
      val system = ActorSystem(EventBus())
      class Storage extends Actor:
        override def name = "storage"
        override def capabilities: Set[CapabilityProvider] =
          Set(CapabilityProvider.provide(StorageCapability, "positions"))
      class Consumer extends Actor:
        override def name = "storage-consumer"
        override def requirements: Set[Requirement] =
          Set(Requirement.capability(StorageCapability, "positions"))

      val missing = intercept[IllegalStateException](system.spawn(Consumer()))
      assert(missing.getMessage.contains("storage@positions"), missing.getMessage)
      val handles = system.spawnAll(Vector(Storage(), Consumer()))
      system.stop(handles.last)
      system.stop(handles.head)

  test("动态停止提供者若会让存活组件丢失硬依赖 -> 在停止副作用前拒绝"):
    supervised:
      val system = ActorSystem(EventBus())
      class Provider extends Actor:
        override def name = "provider"
        override def commandHandlers: Set[CommandHandler] = Set(CommandHandler.command(TestCommand, "k"))
      class Consumer extends Actor:
        override def name = "consumer"
        override def requirements: Set[Requirement] = Set(Requirement.command(TestCommand, "k"))

      val handles = system.spawnAll(Vector(Provider(), Consumer()))
      val provider = handles.head
      val consumer = handles.last
      val e = intercept[IllegalStateException](system.stop(provider))
      assert(e.getMessage.contains("丢失硬依赖"), e.getMessage)
      assertEquals(provider.state, ActorState.Running, "拒绝必须发生在停止副作用之前")
      system.stop(consumer)
      system.stop(provider)

  test("全系统停机按硬依赖拓扑执行, 不依赖组件装配顺序"):
    val bus = EventBus()
    val system = ActorSystem(bus)
    val trace = ConcurrentLinkedQueue[String]()

    class Consumer extends Actor:
      override def name = "consumer"
      override def requirements: Set[Requirement] = Set(Requirement.command(TestCommand, "k"))
      override def onStop(now: Timestamp): Vector[AnyEvent] =
        trace.add(name)
        Vector(Event.local(TestCommand, "k"))

    class Provider extends Actor:
      override def name = "provider"
      override def commandHandlers: Set[CommandHandler] = Set(CommandHandler.command(TestCommand, "k"))
      override def onEvent(event: AnyEvent, now: Timestamp): Vector[AnyEvent] =
        trace.add("handled")
        Vector.empty
      override def onStop(now: Timestamp): Vector[AnyEvent] =
        trace.add(name)
        Vector.empty

    system.spawnAll(Vector(Consumer(), Provider())) // 故意把 provider 后装，逆装配序会停错
    system.stopAll()
    assertEquals(trace.asScala.toVector, Vector("consumer", "handled", "provider"))

  test("依赖方 onStop 尚未完成时, 并发停止提供者必须被拒绝"):
    val system = ActorSystem(EventBus())
    val enteredStop = CountDownLatch(1)
    val finishStop = CountDownLatch(1)
    val stopFailures = ConcurrentLinkedQueue[Throwable]()

    class Provider extends Actor:
      override def name = "provider"
      override def commandHandlers: Set[CommandHandler] = Set(CommandHandler.command(TestCommand, "k"))

    class Consumer extends Actor:
      override def name = "consumer"
      override def requirements: Set[Requirement] = Set(Requirement.command(TestCommand, "k"))
      override def onStop(now: Timestamp): Vector[AnyEvent] =
        enteredStop.countDown()
        finishStop.await()
        Vector.empty

    val handles = system.spawnAll(Vector(Provider(), Consumer()))
    val stoppingConsumer = Thread.ofVirtual().start { () =>
      try system.stop(handles.last)
      catch case e: Throwable => stopFailures.add(e)
    }
    assert(enteredStop.await(2, TimeUnit.SECONDS))
    val rejected = intercept[IllegalStateException](system.stop(handles.head))
    assert(rejected.getMessage.contains("丢失硬依赖"), rejected.getMessage)
    finishStop.countDown()
    stoppingConsumer.join()
    assert(stopFailures.isEmpty, stopFailures.asScala.mkString("; "))
    system.stop(handles.head)

  test("命令观察者不是能力提供者, 同组件观察并处理也只收到一次"):
    supervised:
      val bus = EventBus()
      val system = ActorSystem(bus)

      class OverlappingProvider extends Actor:
        override def name = "overlapping-provider"
        override def interests: Set[Interest] = Set(
          Interest.Keyed(TestCommand, Set("k")),
          Interest.Keyed(TestCommand, Set("k", "other")),
        )
        override def commandHandlers: Set[CommandHandler] = Set(CommandHandler.command(TestCommand, "k"))

      val provider = system.spawn(OverlappingProvider())
      assertEquals(bus.handlerCount(TestCommand, "k"), 1)
      assertEquals(bus.subscriberCount(TestCommand, "k"), 1, "观察声明与处理能力必须分开计数")
      bus.publish(Event.local(TestCommand, "k"))
      system.stop(provider)

  test("并发停止共享能力提供者: 校验与退订原子化, 至少保留一个"):
    supervised:
      val bus = EventBus()
      val system = ActorSystem(bus)
      class Provider(override val name: String) extends Actor:
        override def commandHandlers: Set[CommandHandler] = Set(CommandHandler.command(SharedCommand, "k"))
      class Consumer extends Actor:
        override def name = "shared-consumer"
        override def requirements: Set[Requirement] = Set(Requirement.command(SharedCommand, "k"))

      val handles = system.spawnAll(Vector(Provider("p1"), Provider("p2"), Consumer()))
      val gate = CountDownLatch(1)
      val failures = ConcurrentLinkedQueue[Throwable]()
      val stops = handles.take(2).map { provider =>
        ox.fork {
          gate.await()
          try system.stop(provider)
          catch case e: IllegalStateException => failures.add(e)
        }
      }
      gate.countDown()
      stops.foreach(_.join())

      assertEquals(failures.size, 1, "第二个卸载必须看到第一个已经退订并被拒绝")
      assertEquals(bus.handlerCount(SharedCommand, "k"), 1)
      system.stop(handles.last)
      handles.take(2).find(_.state == ActorState.Running).foreach(system.stop)

  test("父组件停止与 onEvent 创建子组件并发: 不死锁也不留下孤儿"):
    supervised:
      val bus = EventBus()
      val system = ActorSystem(bus)
      val entered = CountDownLatch(1)
      val proceed = CountDownLatch(1)
      val rejected = ConcurrentLinkedQueue[Throwable]()

      class Child extends Actor:
        override def name = "late-child"

      class Parent extends Actor:
        override def name = "parent"
        override def interests: Set[Interest] = Set(Interest.Keyed(Topics.Bbo, Set(btc)))
        override def onEvent(event: AnyEvent, now: Timestamp): Vector[AnyEvent] =
          entered.countDown()
          proceed.await()
          Vector.empty
        override def onStart(ctx: ActorContext): Unit = Parent.ctx = ctx

      object Parent:
        @volatile var ctx: ActorContext = scala.compiletime.uninitialized

      val parent = system.spawn(Parent())
      bus.publish(Event.at(Topics.Bbo, bbo(), t0))
      assert(entered.await(2, TimeUnit.SECONDS))
      val stopping = ox.fork(system.stop(parent))
      val deadline = System.currentTimeMillis() + 2000
      while parent.state != ActorState.Stopping && System.currentTimeMillis() < deadline do Thread.sleep(1)
      try Parent.ctx.spawn(Child())
      catch case e: IllegalStateException => rejected.add(e)
      proceed.countDown()
      stopping.join()

      assertEquals(rejected.size, 1, "停止预留之后必须拒绝创建子组件")
      assertEquals(system.alive, 0, "不能留下脱离父树的子组件")

  test("fail-fast: actor 处理事件时抛异常 -> 级联终止整个作用域"):
    intercept[RuntimeException] {
      supervised:
        val bus = EventBus()
        val system = ActorSystem(bus)
        class Exploding extends Actor:
          override def name = "boom"
          override def interests: Set[Interest] = Set(Interest.All(Topics.Bbo))
          override def onEvent(event: AnyEvent, now: Timestamp): Vector[AnyEvent] =
            sys.error("boom")
        system.spawn(Exploding())
        bus.publish(Event.at(Topics.Bbo, bbo(), t0))
        system.awaitShutdown()
    }

  test("failureSink 抛 InterruptedException 不能截断核心停机"):
    val bus = EventBus()
    val callbackFailure = InterruptedException("failure sink interrupted")
    val system = ActorSystem(bus, failureSink = _ => throw callbackFailure)

    class ExplodingWithBrokenSink extends Actor:
      override def name = "boom-with-broken-sink"
      override def interests: Set[Interest] = Set(Interest.All(Topics.Bbo))
      override def onEvent(event: AnyEvent, now: Timestamp): Vector[AnyEvent] =
        throw RuntimeException("component failed")

    val handle = system.spawn(ExplodingWithBrokenSink())
    bus.publish(Event.at(Topics.Bbo, bbo(), t0))
    val root = intercept[RuntimeException](system.awaitShutdown())
    assertEquals(root.getMessage, "component failed")
    assert(root.getSuppressed.exists(_ eq callbackFailure), root.toString)
    assertEquals(handle.state, ActorState.Failed)
    assertEquals(system.alive, 0)

  test("私有子系统失败沿所有权上报主系统, 不会静默停更"):
    val failNow = CountDownLatch(1)
    val system = ActorSystem(EventBus())

    class FailingChild extends Actor:
      override def name = "failing-child"
      override def onStart(ctx: ActorContext): Unit = ctx.fork {
        failNow.await()
        sys.error("child system failed")
      }

    class Parent extends Actor:
      override def name = "parent-with-private-bus"
      override def onStart(ctx: ActorContext): Unit =
        ctx.childSystem(EventBus()).spawn(FailingChild())

    system.spawn(Parent())
    failNow.countDown()
    val failure = intercept[RuntimeException](system.awaitShutdown())
    assert(failure.getMessage.contains("child system failed"), failure.getMessage)
    assertEquals(system.alive, 0)

  test("子任务失败 -> 全体有序停机, 每个 onStop 都跑到, 收尾完了才抛"):
    // 交易所插件自管的 WS 断了这类"子任务失败", 从前直接炸穿 ox 作用域: 所有 fork 被中断、
    // onStop 一律跳过, 于是策略的撤单指令漏发、挂单留在交易所无人跟踪。
    // 现在它先把全体组件按逆装配序停完 (onStop 逐个跑到), 再把原异常抛出。
    val stopped = ConcurrentLinkedQueue[String]()
    val failNow = CountDownLatch(1)
    class Quiet(override val name: String) extends Actor:
      override def onStop(now: Timestamp): Vector[AnyEvent] =
        stopped.add(name); Vector.empty
    class SelfManagedFailure extends Actor:
      override def name = "feed"
      override def onStart(ctx: ActorContext): Unit = ctx.fork {
        failNow.await()
        sys.error("私有流断了")
      }
      override def onStop(now: Timestamp): Vector[AnyEvent] =
        stopped.add(name); Vector.empty

    intercept[RuntimeException] {
      supervised:
        val system = ActorSystem(EventBus())
        system.spawn(Quiet("gateway")) // 先装
        system.spawn(SelfManagedFailure())
        system.spawn(Quiet("strategy")) // 后装
        failNow.countDown()
        system.awaitShutdown()         // 等失败把停机跑起来并最终抛出
    }

    assertEquals(
      stopped.asScala.toVector,
      Vector("strategy", "feed", "gateway"),
      "逆装配序: 后装的先停 —— 策略的撤单指令发出去时, 柜台还活着接得住",
    )

  test("请求停机 -> 同样是逆装配序, 且不抛异常"):
    val stopped = ConcurrentLinkedQueue[String]()
    class Quiet(override val name: String) extends Actor:
      override def onStop(now: Timestamp): Vector[AnyEvent] =
        stopped.add(name); Vector.empty
    supervised:
      val system = ActorSystem(EventBus())
      system.spawn(Quiet("gateway"))
      system.spawn(Quiet("strategy"))
      system.requestShutdown("测试")
      system.awaitShutdown() // 正常停机: 没有 failure, 不该抛
      assertEquals(system.alive, 0, "全体已停")
    assertEquals(stopped.asScala.toVector, Vector("strategy", "gateway"))

  test("进入全系统停机后拒绝新装配, 不让快照之外的组件漏网"):
    supervised:
      val system = ActorSystem(EventBus())
      system.stopAll()
      val e = intercept[IllegalStateException](system.spawn(Recorder("late", ConcurrentLinkedQueue[String]())))
      assert(e.getMessage.contains("停机阶段"), e.getMessage)
      assertEquals(system.alive, 0)

  test("actor 产出的事件被发布到总线"):
    supervised:
      val bus = EventBus()
      val system = ActorSystem(bus)
      class Relay extends Actor:
        override def name = "relay"
        override def interests: Set[Interest] = Set(Interest.Keyed(Topics.Bbo, Set(btc)))
        override def onEvent(event: AnyEvent, now: Timestamp): Vector[AnyEvent] =
          event.as(Topics.Bbo).map(b => Event.local(Topics.Fill, Fill(AccountId.Live, ex, b.symbol, Side.Long, b.bidPrice, 1.0, now))).toVector
      val downstream = bus.subscribe(Set(Interest.All(Topics.Fill)))
      system.spawn(Relay())
      bus.publish(Event.at(Topics.Bbo, bbo(123.0), t0))
      assertEquals(downstream.events.receive().as(Topics.Fill).map(_.price.value), Some(123.0))
