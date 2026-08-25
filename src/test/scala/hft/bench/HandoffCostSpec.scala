package hft.bench

import hft.TestUnits.given

import hft.domain.*
import hft.event.*
import ox.supervised

import java.util.concurrent.CountDownLatch

/** 测量当前设计的热路径成本：一条行情从 publish 到消费者处理完，跨线程移交花了多少。
  *
  * 不是断言性能的测试（数字随机器变），而是把"这个架构选择的代价"变成可复现的数字。
  * 用 `BENCH=1 sbt testOnly ...` 运行。
  */
class HandoffCostSpec extends munit.FunSuite:
  override def munitIgnore: Boolean = sys.env.get("BENCH").isEmpty

  private val ex = Exchange.Binance
  private val inst = Instrument(ex, "BTCUSDT")
  private val N = 200_000

  private def bbo(i: Int) = BBO(ex, "BTCUSDT", 100.0 + i % 10, Coin(1.0), 100.1, Coin(1.0), i.toLong)

  test("经总线投递 vs 同线程直接调用"):
    // A. 同线程直接调用：消费者就是一个函数
    var sink = 0.0
    val t0 = System.nanoTime()
    var i = 0
    while i < N do
      val ev = Event.at(Topics.Bbo, bbo(i), i.toLong)
      ev.as(Topics.Bbo).foreach(b => sink += b.bidPrice.value)
      i += 1
    val direct = System.nanoTime() - t0

    // B. 经总线：publish -> channel -> 消费者线程
    val viaBus = supervised {
      val bus = EventBus()
      val mailbox = bus.subscribe(Set(Interest.Keyed(Topics.Bbo, Set(inst))))
      val done = CountDownLatch(1)
      var got = 0.0
      ox.forkDiscard {
        var n = 0
        while n < N do
          mailbox.events.receive().as(Topics.Bbo).foreach(b => got += b.bidPrice.value)
          n += 1
        done.countDown()
      }
      val t1 = System.nanoTime()
      var j = 0
      while j < N do
        bus.publish(Event.at(Topics.Bbo, bbo(j), j.toLong))
        j += 1
      done.await()
      System.nanoTime() - t1
    }

    val directNs = direct.toDouble / N
    val busNs = viaBus.toDouble / N
    println(f"""
      |[热路径成本, N=$N]
      |  同线程直接调用 : $directNs%8.0f ns/事件
      |  经总线投递     : $busNs%8.0f ns/事件
      |  跨线程移交开销 : ${busNs - directNs}%8.0f ns/事件  (${busNs / directNs}%.1f×)
      |""".stripMargin)
    assert(sink != 0.0)

  test("间歇投递的单条响应延迟 —— 消费者已 park, 需要唤醒"):
    // 满负荷时 channel 走 CAS 路径不 park, 成本很低; 但真实行情是间歇的,
    // 消费者多半在 park 状态。HFT 关心的正是这个: 一条行情到达, 多久被处理完。
    val rounds = 20_000
    val lat = new Array[Long](rounds)
    supervised {
      val bus = EventBus()
      val mailbox = bus.subscribe(Set(Interest.Keyed(Topics.Bbo, Set(inst))))
      val ready = new java.util.concurrent.SynchronousQueue[Long]()
      ox.forkDiscard {
        while true do
          val ev = mailbox.events.receive()
          ev.as(Topics.Bbo).foreach(_ => ready.put(System.nanoTime()))
      }
      // 预热
      for _ <- 0 until 2000 do
        bus.publish(Event.at(Topics.Bbo, bbo(0), 0L)); ready.take(): Unit
      var i = 0
      while i < rounds do
        val t = System.nanoTime()
        bus.publish(Event.at(Topics.Bbo, bbo(i), i.toLong))
        val done = ready.take()
        lat(i) = done - t
        i += 1
        Thread.sleep(0, 20_000) // 20µs 间隔, 让消费者回到 park
    }
    java.util.Arrays.sort(lat)
    def pct(p: Double): Long = lat((rounds * p).toInt.min(rounds - 1))
    println(f"""
      |[间歇投递的单条响应延迟, N=$rounds]
      |  p50 : ${pct(0.50)}%7d ns
      |  p99 : ${pct(0.99)}%7d ns
      |  max : ${lat(rounds - 1)}%7d ns
      |""".stripMargin)

  test("事件封装本身的成本"):
    val n = 500_000
    var sink = 0.0
    // 裸载荷: 直接处理 BBO
    val payloads = Array.tabulate(1000)(bbo)
    var t = System.nanoTime()
    var i = 0
    while i < n do { sink += payloads(i % 1000).bidPrice.value; i += 1 }
    val raw = (System.nanoTime() - t).toDouble / n
    // 封装成 Event 再取回
    t = System.nanoTime()
    i = 0
    while i < n do
      val ev = Event.at(Topics.Bbo, payloads(i % 1000), i.toLong)
      ev.as(Topics.Bbo).foreach(b => sink += b.bidPrice.value)
      i += 1
    val wrapped = (System.nanoTime() - t).toDouble / n
    println(f"""
      |[事件封装成本, N=$n]
      |  裸载荷处理     : $raw%6.1f ns
      |  Event 封装+取回: $wrapped%6.1f ns  (+${wrapped - raw}%.1f ns: 一次对象分配 + keyOf + 引用比较)
      |""".stripMargin)
    assert(sink != 0.0)
