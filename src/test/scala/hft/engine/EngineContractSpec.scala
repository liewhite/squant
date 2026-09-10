package hft.engine

import hft.domain.*
import hft.event.Commands.{AccountSync, AccountSyncReport, MarketSubscription}
import hft.event.{AnyEvent, Interest, Topics}
import hft.exchange.{MarketFeed, TradingGateway}
import hft.strategy.{Strategy, StrategyHandlers}
import ox.supervised

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.{CountDownLatch, CyclicBarrier, TimeUnit}
import scala.jdk.CollectionConverters.*
import hft.TestUnits.given

/** 指令契约校验：策略将要发出的每条指令都必须有人接，没有就**拒绝启动**。
  *
  * 这是插件化最主要的正确性收益。一条没人接的指令是零症状的静默失效 ——
  * 行情永远不会到、订单永远不会发出、对齐永远不完成，而不会有任何异常或错误日志。
  */
class EngineContractSpec extends munit.FunSuite:
  private val ex = Exchange.Binance
  private val sym = "BTCUSDT"
  private val inst = Instrument.perp(ex, sym)
  private val meta = SymbolMeta(ex, sym, tickSize = 0.1, sizeStep = 0.001, minOrderSize = 0.001, contractSize = 1.0)

  test("账户对齐按 target 去重, 重复应答不能冒充另一个柜台"):
    val liveBinance = AccountExchange(AccountId.Live, Exchange.Binance)
    val liveOkx = AccountExchange(AccountId.Live, Exchange.Okx)
    val readiness = AlignmentReadiness(Set(liveBinance, liveOkx), requestId = 7L)

    assert(!readiness.acknowledge(AccountSyncReport(AccountId.Live, Exchange.Binance, 7L))(()))
    assert(!readiness.acknowledge(AccountSyncReport(AccountId.Live, Exchange.Binance, 7L))(()))
    assert(!readiness.acknowledge(AccountSyncReport(AccountId.Live, Exchange.Okx, 6L))(()))
    assertEquals(readiness.pending, Set(liveOkx))

    assert(readiness.acknowledge(AccountSyncReport(AccountId.Live, Exchange.Okx, 7L))(()))

  test("最后一个对齐 ACK 与超时竞争时只有一个赢家"):
    val target = AccountExchange(AccountId.Live, Exchange.Binance)
    val report = AccountSyncReport(AccountId.Live, Exchange.Binance, 11L)

    (1 to 200).foreach { _ =>
      val readiness = AlignmentReadiness(Set(target), requestId = 11L)
      val start = CyclicBarrier(3)
      val done = CountDownLatch(2)
      val activated = AtomicInteger(0)
      val ackWon = AtomicBoolean(false)
      val timeoutWon = AtomicBoolean(false)

      Thread.ofVirtual().start { () =>
        start.await()
        ackWon.set(readiness.acknowledge(report)(activated.incrementAndGet(): Unit))
        done.countDown()
      }
      Thread.ofVirtual().start { () =>
        start.await()
        timeoutWon.set(readiness.timeout(50L)(_ => ()))
        done.countDown()
      }
      start.await()
      assert(done.await(2, TimeUnit.SECONDS), "竞争线程必须结束")

      if ackWon.get then
        assert(!timeoutWon.get)
        assertEquals(activated.get, 1)
      else
        assert(timeoutWon.get, "ACK 没赢时必须由超时赢")
        assertEquals(activated.get, 0, "超时先完成后不能再激活行情")
    }

  test("对齐超时先同步上报组件失败, 再唤醒 Engine 等待者"):
    val target = AccountExchange(AccountId.Live, Exchange.Binance)
    val readiness = AlignmentReadiness(Set(target), requestId = 12L)
    val reporting = CountDownLatch(1)
    val allowReport = CountDownLatch(1)
    val finished = CountDownLatch(1)

    Thread.ofVirtual().start { () =>
      readiness.timeout(50L) { _ =>
        reporting.countDown()
        allowReport.await()
      }
      finished.countDown()
    }

    assert(reporting.await(2, TimeUnit.SECONDS), "测试前提: timeout 已锁定失败并进入同步上报")
    assert(!readiness.await(50L), "失败上报完成前不能唤醒 Engine 执行补偿 stop")
    allowReport.countDown()
    assert(finished.await(2, TimeUnit.SECONDS))
    val failure = intercept[IllegalStateException](readiness.awaitReady("test-session"))
    assert(failure.getMessage.contains("启动对齐超时"), failure.getMessage)

  /** 指令投递是异步的 (经总线进插件邮箱)，断言要等它到达 */
  private def eventually(what: => String)(cond: => Boolean): Unit =
    val deadline = System.nanoTime() + 3_000_000_000L
    while System.nanoTime() < deadline && !cond do Thread.sleep(5)
    assert(cond, s"等待超时: $what")

  /** 只声明订阅、不做任何事的策略 */
  private class Watcher extends Strategy:
    def handlers = StrategyHandlers.empty.market(Topics.Bbo, inst) { (_, _, _) => Vector.empty }

  /** 记录收到哪些订阅指令的假行情插件 */
  private class RecordingFeed(exch: Exchange, log: ConcurrentLinkedQueue[String]) extends MarketFeed:
    override def exchange: Exchange = exch
    override protected def connect(): Unit = ()
    override protected def subscribeToExchange(kinds: Set[SubscriptionKind]): Unit =
      log.add(s"market:$exch:${kinds.map(_.subscribedSymbol).toVector.sorted.mkString(",")}"): Unit

  /** 记录收到哪些指令的假柜台 */
  private class RecordingGateway(exch: Exchange, acct: AccountId, log: ConcurrentLinkedQueue[String]) extends TradingGateway:
    override def exchange: Exchange = exch
    override def account: AccountId = acct
    override protected def accountRefreshMs: Long = 100_000 // 别让周期刷新干扰断言
    override protected def connect(): Unit = ()
    override protected def metaOf(symbol: Symbol): SymbolMeta = meta
    override protected def placeAligned(order: Order, now: Timestamp): Unit = log.add(s"place:${order.symbol}"): Unit
    override protected def cancelOrder(instrument: Instrument, ref: OrderRef, now: Timestamp): Unit = ()
    override protected def syncSnapshot(instruments: Set[Instrument]): TradingGateway.AccountSnapshot =
      log.add(s"sync:$exch:${instruments.toVector.map(_.symbol).sorted.mkString(",")}")
      TradingGateway.AccountSnapshot(Vector.empty, Vector.empty)
    override protected def currentAccountInfo(): AccountInfo = AccountInfo(acct, exch, 10_000.0)
    override protected def currentWallet(): Map[String, Double] = Map("USDT" -> 10_000.0)

  test("没有装行情插件 -> 拒绝启动, 而不是让策略订个空"):
    supervised:
      val log = ConcurrentLinkedQueue[String]()
      Engine.run(plugins = Vector(RecordingGateway(ex, AccountId.Live, log))) { engine =>
        val e = intercept[IllegalStateException](engine.addStrategy(Watcher(), AccountId.Live))
        assert(e.getMessage.contains("行情订阅指令"), e.getMessage)
        assert(e.getMessage.contains("Binance"), e.getMessage)
      }

  test("没有装柜台 -> 拒绝启动, 而不是让订单永远发不出去"):
    supervised:
      val log = ConcurrentLinkedQueue[String]()
      Engine.run(plugins = Vector(RecordingFeed(ex, log))) { engine =>
        val e = intercept[IllegalStateException](engine.addStrategy(Watcher(), AccountId.Live))
        assert(e.getMessage.contains("下单指令"), e.getMessage)
      }

  test("柜台装在别的交易所 -> 一样拒绝 (校验精确到交易所, 不只是 topic)"):
    // 这正是"插件声明我提供什么"表达不了的那种精度: "我提供下单执行"丢掉了交易所维度。
    supervised:
      val log = ConcurrentLinkedQueue[String]()
      Engine.run(plugins = Vector(RecordingFeed(ex, log), RecordingGateway(Exchange.Okx, AccountId.Live, log))) { engine =>
        val e = intercept[IllegalStateException](engine.addStrategy(Watcher(), AccountId.Live))
        assert(e.getMessage.contains("下单指令"), e.getMessage)
        assert(e.getMessage.contains("live@Binance"), e.getMessage)
      }

  test("柜台装在别的账户 -> 一样拒绝 (影子柜台接不了实盘的单)"):
    supervised:
      val log = ConcurrentLinkedQueue[String]()
      Engine.run(plugins = Vector(RecordingFeed(ex, log), RecordingGateway(ex, AccountId.Paper(1), log))) { engine =>
        val e = intercept[IllegalStateException](engine.addStrategy(Watcher(), AccountId.Live))
        assert(e.getMessage.contains("下单指令"), e.getMessage)
      }

  test("旁观者冒充不了接单者 —— 订 Interest.All(OrderIntent) 的记录器不算数"):
    // 校验数的是**定向**订阅者。把全量订阅者算进去的话, 一个意图记录/监控插件就能让
    // "柜台没装"这件事通过校验, 而订单永远发不出去 —— 那正是这道校验要防的失效。
    supervised:
      val log = ConcurrentLinkedQueue[String]()
      Engine.run(plugins = Vector(RecordingFeed(ex, log))) { engine =>
        val spy = engine.subscribe(Set(Interest.All(hft.event.Commands.OrderIntent)))
        try
          val e = intercept[IllegalStateException](engine.addStrategy(Watcher(), AccountId.Live))
          assert(e.getMessage.contains("下单指令"), e.getMessage)
        finally spy.close()
      }

  test("同一个 (账户, 交易所) 上装第二个柜台 -> 装配期拒绝 (否则静默双执行)"):
    // 路由键带交易所维度防住的是"拆出多个柜台"这一种成因, 防不住"同一个键上装了两台";
    // 后者只能靠装配期数一数。运行期唯一的症状是仓位莫名其妙翻倍。
    supervised:
      val log = ConcurrentLinkedQueue[String]()
      Engine.run(plugins = Vector(RecordingGateway(ex, AccountId.Live, log))) { engine =>
        val e = intercept[IllegalStateException](engine.install(RecordingGateway(ex, AccountId.Live, log)))
        assert(e.getMessage.contains("已有提供者或本批重复提供"), e.getMessage)
      }

  test("两台柜台都从 Engine.run 的 plugins 进 -> 同样要拒绝"):
    // 这是实盘装配的真实形状 (启动器一律 Engine.run(plugins = Vector(gateway, feed))),
    // 而从前 start 是直接 spawn、不走 install, 于是那道闸在生产路径上从未生效过。
    supervised:
      val log = ConcurrentLinkedQueue[String]()
      val e = intercept[IllegalStateException](
        Engine.run(plugins =
          Vector(RecordingGateway(ex, AccountId.Live, log), RecordingGateway(ex, AccountId.Live, log))
        )(_ => ())
      )
      assert(e.getMessage.contains("已有提供者或本批重复提供"), e.getMessage)

  test("同一个交易所上装两个柜台, 账户不同 -> 允许 (实盘与影子盘并行的前提)"):
    supervised:
      val log = ConcurrentLinkedQueue[String]()
      Engine.run(plugins = Vector(RecordingGateway(ex, AccountId.Live, log))) { engine =>
        engine.install(RecordingGateway(ex, AccountId.Paper(1), log)) // 不该抛
      }

  test("同一个交易所上装两个行情插件 -> 允许 (一个接盘口、一个接希腊值)"):
    // 行情订阅不是独占指令: 多订一次最多浪费一次往返, 与多下一次单不是一回事。
    supervised:
      val log = ConcurrentLinkedQueue[String]()
      Engine.run(plugins = Vector(RecordingFeed(ex, log))) { engine =>
        engine.install(RecordingFeed(ex, log)) // 不该抛
      }

  test("拒绝发生在策略启动之前 —— 起来了再拒绝就得再把它停回去"):
    supervised:
      val log = ConcurrentLinkedQueue[String]()
      Engine.run(plugins = Vector(RecordingGateway(ex, AccountId.Live, log))) { engine =>
        intercept[IllegalStateException](engine.addStrategy(Watcher(), AccountId.Live))
        // 校验不通过时对齐指令根本没发出去 —— 说明装配在闸门之后才开始
        assert(!log.asScala.exists(_.startsWith("sync:")), log.asScala.toVector.toString)
      }

  test("装齐了就能起, 且**对齐先于行情订阅**"):
    // 顺序是这条因果链的全部意义: 策略必须在拿到初始仓位之后才看到第一条行情,
    // 否则它基于"仓位为空"这个错误前提做第一次决策。
    supervised:
      val log = ConcurrentLinkedQueue[String]()
      Engine.run(plugins = Vector(RecordingGateway(ex, AccountId.Live, log), RecordingFeed(ex, log))) { engine =>
        engine.addStrategy(Watcher(), AccountId.Live)

        eventually(log.asScala.toVector.toString)(log.size == 2)
        assertEquals(log.asScala.toVector, Vector(s"sync:$ex:$sym", s"market:$ex:$sym"), "对齐必须排在行情订阅之前")
      }

  test("影子账户同样走对齐 —— 否则它永远收不到那批初始零仓快照"):
    // 策略被教导"拿到初始仓位之前不要动作"。实盘给了、影子不给的话, 同一份逻辑在影子盘上
    // 永远不交易 —— 虚实分叉, 而且恰好废掉影子盘的对照价值。影子柜台的对齐是本地的, 不打 REST。
    supervised:
      val log = ConcurrentLinkedQueue[String]()
      val paper = AccountId.Paper(1)
      Engine.run(plugins = Vector(RecordingGateway(ex, paper, log), RecordingFeed(ex, log))) { engine =>
        engine.addStrategy(Watcher(), paper)

        eventually(log.asScala.toVector.toString)(log.size == 2)
        assertEquals(log.asScala.toVector, Vector(s"sync:$ex:$sym", s"market:$ex:$sym"), "对齐仍排在行情之前")
      }

  test("策略会话拥有标的租约: 重复接管被拒绝, 停止父会话后自动释放"):
    supervised:
      val log = ConcurrentLinkedQueue[String]()
      Engine.run(plugins = Vector(RecordingGateway(ex, AccountId.Live, log), RecordingFeed(ex, log))) { engine =>
        val first = engine.addStrategy(Watcher(), AccountId.Live)

        val conflict = intercept[IllegalStateException](engine.addStrategy(Watcher(), AccountId.Live))
        assert(conflict.getMessage.contains("已被 strategy-session"), conflict.getMessage)

        engine.removeStrategy(first)
        engine.addStrategy(Watcher(), AccountId.Live)
      }

  test("停掉柜台后, 那条指令信道确实空了 —— 再加策略会被同一道闸拦下"):
    // 校验查的是**当下**的订阅事实, 不是启动时的一张快照。插件被撤下之后,
    // 同一道闸自然而然地开始拦人 —— 这正是"订阅本身就是声明"的好处。
    supervised:
      val log = ConcurrentLinkedQueue[String]()
      Engine.run(plugins = Vector(RecordingFeed(ex, log))) { engine =>
        val gateway = engine.install(RecordingGateway(ex, AccountId.Live, log))
        val strategy = engine.addStrategy(Watcher(), AccountId.Live)

        engine.removeStrategy(strategy) // 先撤策略, 否则拦下它的会是标的独占那道闸
        engine.stop(gateway)
        val e = intercept[IllegalStateException](engine.addStrategy(Watcher(), AccountId.Live))
        assert(e.getMessage.contains("下单指令"), e.getMessage)
      }

  test("watchMarket 同样要校验 —— 扫描器订了个空是一样的静默失效"):
    supervised:
      Engine.run() { engine =>
        val e = intercept[IllegalStateException](engine.watchMarket(Set(inst), Set(Topics.Trade)))
        assert(e.getMessage.contains("行情订阅指令"), e.getMessage)
      }

  test("watchMarket 不占标的、不对齐 —— 只是把数据引过来"):
    supervised:
      val log = ConcurrentLinkedQueue[String]()
      Engine.run(plugins = Vector(RecordingFeed(ex, log))) { engine =>
        engine.watchMarket(Set(inst), Set(Topics.Trade))
        eventually(log.asScala.toVector.toString)(log.size == 1)
        assertEquals(log.asScala.toVector, Vector(s"market:$ex:$sym"))
        // 没占标的, 所以之后仍能在同一标的上起交易策略 (装上柜台之后)
        assert(!log.asScala.exists(_.startsWith("sync:")))
      }

  test("Engine.run: body 抛出时仍走有序停机 —— onStop 不能被跳过"):
    // 从前 start 返回 Engine、由启动器最后调 awaitShutdown, 于是从 start 到 awaitShutdown
    // 之间任何一次抛出都会跳过整个停机流程: 已连上交易所的柜台不跑 onStop, 挂单留在交易所。
    supervised:
      val stopped = new java.util.concurrent.CountDownLatch(1)

      class Cleanup extends hft.actor.Actor:
        override def name = "needs-cleanup"
        override def onStop(now: Timestamp): Vector[AnyEvent] =
          stopped.countDown()
          Vector.empty

      val boom = intercept[RuntimeException](
        Engine.run(plugins = Vector(Cleanup())) { _ => sys.error("装配之后、awaitShutdown 之前炸了") }
      )
      assertEquals(boom.getMessage, "装配之后、awaitShutdown 之前炸了")
      assert(stopped.getCount == 0L, "body 抛出也必须跑完 onStop")
