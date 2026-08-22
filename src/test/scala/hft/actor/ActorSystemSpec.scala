package hft.actor

import hft.domain.*
import hft.event.{AnyEvent, Event, EventBus, Interest, Topics}
import ox.supervised

import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import scala.jdk.CollectionConverters.*

/** actor 生命周期：树形停机、收尾、退订、fail-fast。 */
class ActorSystemSpec extends munit.FunSuite:
  private val ex = Exchange.Binance
  private val btc = Instrument(ex, "BTCUSDT")
  private val t0 = 1_700_000_000_000L

  private def bbo(px: Double = 100.0) = BBO(ex, "BTCUSDT", px, 1.0, px + 0.1, 1.0, t0)

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
        override def onStart(ctx: ActorContext): Unit =
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
          event.as(Topics.Bbo).foreach(b => processed.add(b.bidPrice))
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
      system.stop(h)
      val afterStop = ticks.size
      Thread.sleep(60) // 若循环没停, 这段时间会再攒十来个 tick
      assertEquals(ticks.size, afterStop, "停止信号必须叫醒定时等待")

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
        // 阻塞等待级联取消; 若 actor 的异常被吞掉, 这里会一直等下去而不是抛出
        CountDownLatch(1).await()
    }

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
      assertEquals(downstream.events.receive().as(Topics.Fill).map(_.price), Some(123.0))
