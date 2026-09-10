package hft.exchange

import hft.actor.ActorSystem
import hft.domain.*
import hft.event.Commands.{AccountOutcome, AccountSync, AccountSyncRequest, AccountSynced, OrderIntent, OutcomeEvent}
import hft.event.{AnyEvent, Event, EventBus, Interest, Topics}
import hft.exchange.AccountReport
import ox.supervised

import java.util.concurrent.{ConcurrentLinkedQueue, TimeUnit}
import scala.jdk.CollectionConverters.*
import hft.TestUnits.given

/** 柜台的执行语义:
  *   - 明确拒单 (4xx) / dry-run / **精度收不下** -> OrderUpdate(Error) 回流, 三者同一条路径
  *   - 结果不确定 (网络错误) -> 抛错终止作用域
  */
class TradingGatewaySpec extends munit.FunSuite:
  private val meta = SymbolMeta(Exchange.Binance, "BTCUSDT", tickSize = 0.1, sizeStep = 0.001, minOrderSize = 0.001, contractSize = 1.0)
  private val metas = Map[Symbol, SymbolMeta]("BTCUSDT" -> meta)

  /** 不推送任何东西的汇报面 —— 本测试只关心执行 */
  private object SilentFeed extends AccountFeed:
    override def exchange: Exchange = Exchange.Binance
    override def connect(sink: AccountReport => Unit, fork: (=> Unit) => Unit, sleepUnlessStopped: Long => Boolean): Unit = ()

  /** 可手动喂报告的汇报面 —— 用来驱动柜台的记账与排序 */
  private class ManualFeed extends AccountFeed:
    @volatile private var sink: AccountReport => Unit = scala.compiletime.uninitialized
    override def exchange: Exchange = Exchange.Binance
    override def connect(s: AccountReport => Unit, fork: (=> Unit) => Unit, sleepUnlessStopped: Long => Boolean): Unit = sink = s
    def emit(report: AccountReport): Unit = sink(report)

  /** 有求必应的桩客户端 —— 驱动汇报面路径时不该被执行面打扰 */
  private class QuietClient extends TradingClient:
    override def exchange: Exchange = Exchange.Binance
    override def placeOrder(order: ExchangeOrder) = Right("ignored")
    override def fetchAllSymbolMetas() = Right(Vector(meta))
    override def cancelOrder(instrument: Instrument, ref: OrderRef) = Right(())
    override def fetchPendingOrders(instrument: Instrument) = Right(Vector.empty)
    override def fetchAccountInfo() = Right(AccountInfo(AccountId.Live, Exchange.Binance, 10_000.0))
    override def fetchWallet() = Right(Map("USDT" -> 10_000.0))
    override def fetchPositions() = Right(Vector.empty)

  private def eventually(what: => String)(cond: => Boolean): Unit =
    val deadline = System.nanoTime() + 3_000_000_000L
    while System.nanoTime() < deadline && !cond do Thread.sleep(5)
    assert(cond, s"等待超时: $what")

  /** 让柜台先完成一次对齐 —— **对齐之前它忽略一切报告**：那时它既不知道自己管哪些标的，
    * 账本也还没有初值，处理了只会算错。 */
  private def align(bus: EventBus, instruments: Set[Instrument] = Set(Instrument.perp(Exchange.Binance, "BTCUSDT")))(using ox.Ox): Unit =
    val done = bus.subscribe(Set(Interest.All(AccountSynced)))
    bus.publish(Event.local(AccountSync, AccountSyncRequest(AccountId.Live, Exchange.Binance, 1L, instruments)))
    done.events.receive(): Unit
    done.close()

  /** 装一台真实形态的柜台, 对齐之后用手动汇报面驱动它 */
  private def withFeed(body: (ManualFeed, ConcurrentLinkedQueue[AnyEvent]) => Unit): Unit =
    supervised:
      val bus = EventBus()
      val feed = ManualFeed()
      ActorSystem(bus).spawn(RestTradingGateway(QuietClient(), feed, AccountId.Live, metas))
      align(bus)
      // 对齐之后才开始收 —— 对齐本身会推一条零仓快照, 那不是本测试的对象
      val seen = ConcurrentLinkedQueue[AnyEvent]()
      val mailbox = bus.subscribe(Set(Interest.All(Topics.Position), Interest.All(Topics.Fill), Interest.All(Topics.OrderUpdate)))
      ox.forkDiscard { mailbox.events.foreach(seen.add) }
      body(feed, seen)

  /** 一笔成交明细 —— 不参与记账 */
  private def executed(qty: Double) =
    AccountReport.Executed(Instrument.perp(Exchange.Binance, "BTCUSDT"), Side.Long, Price(100.0), Coin(qty), 1L)

  /** `orderPrice` 是委托价, `avgFill` 是成交均价 —— 两者刻意不同, 记账只能用后者 */
  private def statusChanged(
      orderId: String,
      status: OrderStatus,
      filled: Double,
      orderPrice: Double = 100.0,
      avgFill: Double = 100.0,
  ) =
    AccountReport.OrderStatusChanged(
      orderId, Some("c1"), Instrument.perp(Exchange.Binance, "BTCUSDT"), Side.Long, status,
      Price(orderPrice), Price(avgFill), Coin(1.0), Coin(filled), false, 1L,
    )

  private def kinds(seen: ConcurrentLinkedQueue[AnyEvent]): Vector[String] =
    seen.asScala.toVector.map(ev =>
      if ev.is(Topics.Position) then "position"
      else if ev.is(Topics.Fill) then "fill"
      else "orderUpdate"
    )

  private def positionSizes(seen: ConcurrentLinkedQueue[AnyEvent]): Vector[Double] =
    seen.asScala.toVector.flatMap(_.as(Topics.Position)).map(_.size.value)

  test("订单回报: 仓位先于订单状态发出"):
    // 反过来的话策略会看到"挂单已消失、仓位还没更新" —— 它据此认为自己既没单也没仓位,
    // 于是**再下一单**。那是危险侧的中间状态, 这条顺序就是为了消灭它。
    withFeed { (feed, seen) =>
      feed.emit(statusChanged("o1", OrderStatus.Filled, filled = 0.5))
      eventually(kinds(seen).toString)(kinds(seen).size == 2)
      assertEquals(kinds(seen), Vector("position", "orderUpdate"))
      assertEquals(positionSizes(seen), Vector(0.5))
    }

  test("成交明细不参与记账 —— 它只变成 Fill"):
    // 记账的唯一依据是订单回报的累计量。让成交也参与记账, 就得为"只给单笔量的交易所"
    // 本地累加, 而累加又要去重、会漂、会与另一个来源不一致 —— 一串补丁的源头。
    withFeed { (feed, seen) =>
      feed.emit(executed(qty = 0.5))
      eventually("应产出成交明细")(kinds(seen).size == 1)
      Thread.sleep(100)
      assertEquals(kinds(seen), Vector("fill"), "成交只产明细, 不产仓位")
      assertEquals(positionSizes(seen), Vector.empty[Double])
    }

  test("重复的订单回报不重复记账 —— 累计量没涨就什么都不发"):
    withFeed { (feed, seen) =>
      feed.emit(statusChanged("o1", OrderStatus.PartiallyFilled(Coin(0.5)), filled = 0.5))
      eventually("首笔应已入账")(kinds(seen).size == 2)
      feed.emit(statusChanged("o1", OrderStatus.PartiallyFilled(Coin(0.5)), filled = 0.5)) // 同一条重放
      eventually("重放仍会转发订单状态")(kinds(seen).size == 3)
      Thread.sleep(100)
      assertEquals(kinds(seen), Vector("position", "orderUpdate", "orderUpdate"), "重放不该再记一次账")
      assertEquals(positionSizes(seen), Vector(0.5))
    }

  test("分批成交按增量入账, 每次都先发新仓位"):
    withFeed { (feed, seen) =>
      feed.emit(statusChanged("o1", OrderStatus.PartiallyFilled(Coin(0.3)), filled = 0.3))
      feed.emit(statusChanged("o1", OrderStatus.Filled, filled = 0.8)) // 又成交 0.5
      eventually(kinds(seen).toString)(kinds(seen).size == 4)
      assertEquals(kinds(seen), Vector("position", "orderUpdate", "position", "orderUpdate"))
      assertEquals(positionSizes(seen), Vector(0.3, 0.8), "记的是增量, 报的是新仓位")
    }

  test("成交均价为零 -> 柜台失败并触发有序停机, 不是'记一条 error 然后继续'"):
    // 市价单的委托价是空的 (多家给 0)，非正的**成交均价**却只能是适配层填错了字段。
    // 从前柜台打一条 error 后继续: 明知有一笔成交却不入账并接着交易, 账本从此确定性少一笔,
    // 策略看到旧仓位再下一单。已经确认是 bug 的输入不该有"继续"这条路。
    val failure = java.util.concurrent.atomic.AtomicReference[Throwable](null)
    val failed = java.util.concurrent.CountDownLatch(1)
    supervised:
      val bus = EventBus()
      val feed = ManualFeed()
      val system = ActorSystem(bus, failureSink = e => { failure.set(e); failed.countDown() })
      system.spawn(RestTradingGateway(QuietClient(), feed, AccountId.Live, metas))
      align(bus)
      val seen = ConcurrentLinkedQueue[AnyEvent]()
      val mailbox = bus.subscribe(Set(Interest.All(Topics.Position), Interest.All(Topics.Fill)))
      ox.forkDiscard { mailbox.events.foreach(seen.add) }

      feed.emit(statusChanged("o1", OrderStatus.Filled, filled = 0.5, orderPrice = 0.0, avgFill = 0.0))
      assert(failed.await(3, TimeUnit.SECONDS), "零均价必须让柜台失败, 而不是继续交易")
      assert(failure.get.getMessage.contains("成交均价"), failure.get.getMessage)
      assertEquals(kinds(seen), Vector.empty, "那一笔既没入账, 也不该产出仓位事件")

  test("记账认的是成交均价那一路 —— 委托价为空照样入账"):
    withFeed { (feed, seen) =>
      feed.emit(statusChanged("o1", OrderStatus.Filled, filled = 0.5, orderPrice = 0.0, avgFill = 101.5))
      eventually(kinds(seen).toString)(kinds(seen).size == 2)
      assertEquals(kinds(seen), Vector("position", "orderUpdate"))
      assertEquals(positionSizes(seen), Vector(0.5))
    }

  test("本地累加的浮点尾巴不产生幻影成交"):
    // 0.3 + 0.5 在浮点里是 0.8000000000000001。严格比较会让它产出一笔 1e-16 的成交,
    // 连带一条幻影仓位事件和一行流水。
    withFeed { (feed, seen) =>
      feed.emit(statusChanged("o1", OrderStatus.PartiallyFilled(Coin(0.8)), filled = 0.8))
      eventually("首笔应入账")(kinds(seen).size == 2)
      feed.emit(statusChanged("o1", OrderStatus.PartiallyFilled(Coin(0.8)), filled = 0.3 + 0.5)) // = 0.8000000000000001
      eventually("重放仍会转发订单状态")(kinds(seen).size == 3)
      Thread.sleep(100)
      assertEquals(positionSizes(seen), Vector(0.8), "尾巴不该被当成一笔新成交")
    }

  test("contractSize != 1: 容差换到币本位域, 小额真实成交不被吞掉"):
    // sizeStep 是**张**, 而增量是币。直接拿 sizeStep 当阈值, 在 contractSize=0.01 的品种上
    // 阈值被放大一百倍 —— 真实成交静默丢弃, 不入账、不发回报、连告警都没有。
    val coarse = SymbolMeta(Exchange.Binance, "BTCUSDT", tickSize = 0.1, sizeStep = 1.0, minOrderSize = 1.0, contractSize = 0.01)
    supervised:
      val bus = EventBus()
      val feed = ManualFeed()
      ActorSystem(bus).spawn(RestTradingGateway(QuietClient(), feed, AccountId.Live, Map("BTCUSDT" -> coarse)))
      align(bus)
      val seen = ConcurrentLinkedQueue[AnyEvent]()
      val mailbox = bus.subscribe(Set(Interest.All(Topics.Position)))
      ox.forkDiscard { mailbox.events.foreach(seen.add) }

      feed.emit(statusChanged("o1", OrderStatus.Filled, filled = 0.03)) // 3 张 = 0.03 币; 旧的错误阈值是 0.5 币
      eventually("小额成交应照常入账")(seen.size == 1)
      assertEqualsDouble(seen.asScala.head.as(Topics.Position).get.size.value, 0.03, 1e-12)

  test("清理判据: 活着的订单永不清, 终态过了保留期才清"):
    // 清掉一张还活着的订单的记账进度, 下一条累计量会以"已记 0"重新记一遍 ——
    // 此前入账的全部数量再记一次, 仓位近乎翻倍且没有自愈路径。
    val active = PositionBook.Settlement(Coin(0.5), firstSeenAt = 0L, terminalAt = None)
    val justDone = PositionBook.Settlement(Coin(0.5), firstSeenAt = 0L, terminalAt = Some(1_000L))
    val longDone = PositionBook.Settlement(Coin(0.5), firstSeenAt = 0L, terminalAt = Some(1_000L))

    val muchLater = 1_000L + PositionBook.StaleSettlementMs * 2
    assert(PositionBook.retains(active, muchLater), "非终态的永远留着, 哪怕很久没动静")
    assert(PositionBook.retains(justDone, 1_000L + PositionBook.SettledRetentionMs), "保留期内还要留着接晚到的成交")
    assert(!PositionBook.retains(longDone, 1_000L + PositionBook.SettledRetentionMs + 1), "过了保留期就该清")

  test("对齐时接管带部分成交的挂单, 后续回报不把那部分重记一遍"):
    // 少了这一步: 拉到的仓位里本已含着那 0.3, 而记账进度是空的, 于是 cumExecQty=0.5
    // 被算成增量 0.5 而不是 0.2 —— 仓位凭空多出 0.3, 且没有任何症状。
    supervised:
      val bus = EventBus()
      val feed = ManualFeed()
      // 对齐时账户里已有一张成交了 0.3 的挂单, 仓位也已是 0.3
      class PartialClient extends QuietClient:
        override def fetchPositions() =
          Right(Vector(Position(AccountId.Live, Exchange.Binance, "BTCUSDT", Coin(0.3))))
        override def fetchPendingOrders(instrument: Instrument) = Right(Vector(
          OrderUpdate(AccountId.Live, "o1", Some("c1"), Exchange.Binance, "BTCUSDT", Side.Long,
            OrderStatus.PartiallyFilled(Coin(0.3)), Price(100.0), Coin(1.0), Coin(0.3), false, 1L)
        ))
      ActorSystem(bus).spawn(RestTradingGateway(PartialClient(), feed, AccountId.Live, metas))
      align(bus)

      val seen = ConcurrentLinkedQueue[AnyEvent]()
      val mailbox = bus.subscribe(Set(Interest.All(Topics.Position)))
      ox.forkDiscard { mailbox.events.foreach(seen.add) }

      feed.emit(statusChanged("o1", OrderStatus.Filled, filled = 0.5)) // 又成交 0.2
      eventually("应有新仓位")(seen.size == 1)
      assertEqualsDouble(
        seen.asScala.head.as(Topics.Position).get.size.value, 0.5, 1e-12,
        "0.3 + 增量 0.2 = 0.5; 若把 0.5 整个当增量就会变成 0.8",
      )

  test("第二批策略的对齐不该抹掉第一批的账本"):
    // 引擎为每批新加的策略都发一次对齐指令, symbols 只含那一批。整本替换会让先装的
    // 策略从此按零仓决策 —— 没有任何症状。
    supervised:
      val bus = EventBus()
      val feed = ManualFeed()
      val eth = "ETHUSDT"
      val ethMeta = SymbolMeta(Exchange.Binance, eth, 0.1, 0.001, 0.001, 1.0)
      class TwoSymbolClient extends QuietClient:
        override def fetchPositions() = Right(Vector(
          Position(AccountId.Live, Exchange.Binance, "BTCUSDT", Coin(0.5))
        ))
      ActorSystem(bus).spawn(RestTradingGateway(TwoSymbolClient(), feed, AccountId.Live, metas + (eth -> ethMeta)))

      align(bus, Set(Instrument.perp(Exchange.Binance, "BTCUSDT")))          // 第一批: BTC, 拉到 0.5
      align(bus, Set(Instrument.perp(Exchange.Binance, eth)))                // 第二批: ETH, 交易所没返回它 -> 零仓

      val seen = ConcurrentLinkedQueue[AnyEvent]()
      val mailbox = bus.subscribe(Set(Interest.All(Topics.Position)))
      ox.forkDiscard { mailbox.events.foreach(seen.add) }

      // BTC 上再成交 0.2 —— 若第二批对齐把 BTC 的账抹了, 这里会算成 0.2 而不是 0.7
      feed.emit(statusChanged("o1", OrderStatus.Filled, filled = 0.2))
      eventually("应有新仓位")(seen.size == 1)
      assertEqualsDouble(
        seen.asScala.head.as(Topics.Position).get.size.value, 0.7, 1e-12,
        "第一批的 0.5 必须还在",
      )

  test("三方对账: 谁跟谁对不上, 决定了这是什么性质的问题"):
    import PositionBook.{Kind, Verdict}
    val tol = 1e-9

    // 三本账一致
    assertEquals(
      PositionBook.compare(Coin(0.5), Coin(0.5), Some(Coin(0.5)), tol).collect { case v: Verdict.Agreed => v.kind },
      Vector(Kind.Internal, Kind.External),
    )

    // 两条**内部**渠道对不上 -> 我们这边的 bug (漏解析、字段读错、去重去多了)
    PositionBook.compare(Coin(0.5), Coin(0.3), Some(Coin(0.5)), tol) match
      case Vector(Verdict.Internal(o, f), Verdict.Agreed(Kind.External)) =>
        assertEqualsDouble(o.value, 0.5, tol); assertEqualsDouble(f.value, 0.3, tol)
      case other => fail(s"应报内部不一致: $other")

    // 账本与**交易所**对不上 -> 外部改了账户 (强平/手动/资金费)
    PositionBook.compare(Coin(0.5), Coin(0.5), Some(Coin(0.9)), tol) match
      case Vector(Verdict.Agreed(Kind.Internal), Verdict.External(o, r)) =>
        assertEqualsDouble(o.value, 0.5, tol); assertEqualsDouble(r.value, 0.9, tol)
      case other => fail(s"应报外部不一致: $other")

    // 交易所还没推过 -> 只做内部比对, 不假装"外部一致"
    assertEquals(PositionBook.compare(Coin(0.5), Coin(0.5), None, tol).size, 1)

    // 容差之内不算不一致
    assert(PositionBook.compare(Coin(0.5), Coin(0.5 + 1e-12), Some(Coin(0.5)), tol).forall(_.isInstanceOf[Verdict.Agreed]))

  test("交易所报的仓位不进总线 —— 总线上的仓位只有账本一个来源"):
    withFeed { (feed, seen) =>
      feed.emit(AccountReport.PositionReported(Instrument.perp(Exchange.Binance, "BTCUSDT"), Coin(9.9), 1L))
      Thread.sleep(100)
      assert(seen.asScala.isEmpty, s"对账用的读数不该外流: ${kinds(seen)}")
    }


  /** 只实现 placeOrder 的 stub，其余方法不应被触达 */
  private class StubClient(placeResult: Either[ExchangeError, OrderId]) extends TradingClient:
    override def exchange: Exchange = Exchange.Binance
    override def placeOrder(order: ExchangeOrder): Either[ExchangeError, OrderId] = placeResult
    override def fetchAllSymbolMetas() = fail("unexpected call")
    override def cancelOrder(instrument: Instrument, ref: OrderRef) = fail("unexpected call")
    override def fetchPendingOrders(instrument: Instrument) = fail("unexpected call")
    // 柜台启动即周期刷净值 —— 给一个固定读数, 免得测试依赖网络
    override def fetchAccountInfo() = Right(AccountInfo(AccountId.Live, Exchange.Binance, 10_000.0))
    override def fetchWallet() = Right(Map("USDT" -> 10_000.0))
    override def fetchPositions() = fail("unexpected call")

  private def orderOf(quantity: Coin) = Order(
    id = "",
    exchange = Exchange.Binance,
    symbol = "BTCUSDT",
    side = Side.Long,
    orderType = OrderType.Market,
    quantity = quantity,
    reduceOnly = false,
    clientOrderId = "c1",
  )

  private def intentOf(order: Order): AnyEvent =
    Event.local(OrderIntent, AccountOutcome(AccountId.Live, OutcomeEvent.PlaceOrders(Vector(order), "test")))

  /** 只取订单回报 —— 柜台启动时会周期发净值, 那不是本测试的对象 */
  private def receivedError(incomes: ox.channels.Source[AnyEvent]): OrderUpdate =
    val ev = incomes.receive()
    ev.as(Topics.OrderUpdate).getOrElse(fail(s"unexpected event: $ev"))

  private def runGateway(client: TradingClient)(
      body: (EventBus, ox.channels.Source[AnyEvent], ActorSystem) => Unit
  ): Unit =
    supervised:
      val bus = EventBus()
      val incomes = bus.subscribe(Set(Interest.All(Topics.OrderUpdate)))
      val system = ActorSystem(bus)
      system.spawn(RestTradingGateway(client, SilentFeed, AccountId.Live, metas))
      body(bus, incomes.events, system)

  test("dry-run (DryRunClient): 信号以 OrderUpdate(Error) 回流清理 pending"):
    // dry-run 不是柜台里的开关, 而是换一个客户端实现 —— 它以 Rejected 返回,
    // 走的正是既有的"确定性失败"通道, 所以这里的期望与真实拒单那条用例完全一致。
    runGateway(DryRunClient(StubClient(Right("ignored")))) { (bus, incomes, _) =>
      bus.publish(intentOf(orderOf(0.001)))
      val update = receivedError(incomes)
      assertEquals(update.clientOrderId, Some("c1"))
      assert(update.status.isInstanceOf[OrderStatus.Error])
    }

  test("交易所明确拒单 (Rejected): OrderUpdate(Error) 回流策略"):
    // 判据是**语义**不是 HTTP 状态码: 三家表达业务拒单的形状不同 (Binance 4xx、
    // OKX 200+sCode、Bybit 200+retCode), 各自在边界翻译成 Rejected。
    runGateway(StubClient(Left(ExchangeError.Rejected("-2019", "Margin is insufficient.")))) { (bus, incomes, _) =>
      bus.publish(intentOf(orderOf(0.001)))
      val update = receivedError(incomes)
      assertEquals(update.clientOrderId, Some("c1"))
      assert(update.status.isInstanceOf[OrderStatus.Error])
    }

  test("限频 (RateLimited): 终止, 不按拒单回流"):
    // 按拒单回流会让策略重挂 -> 更多请求 -> 重试风暴。它意味着"订单生命周期自然限速"
    // 这个前提已经不成立。
    val failed = java.util.concurrent.CountDownLatch(1)
    supervised:
      val bus = EventBus()
      val system = ActorSystem(bus, failureSink = _ => failed.countDown())
      system.spawn(RestTradingGateway(StubClient(Left(ExchangeError.RateLimited("HTTP 429"))), SilentFeed, AccountId.Live, metas))
      bus.publish(intentOf(orderOf(0.001)))
      assert(failed.await(2, TimeUnit.SECONDS), "限频必须触发全系统停机")

  test("未归类的失败 (Other): 订单是否成立不确定 -> 终止"):
    // OKX/Bybit 的请求级错误 (含系统错误) 落在这里: 逐单结果都没拿到, 不能当成"确定未成立"。
    val failed = java.util.concurrent.CountDownLatch(1)
    supervised:
      val bus = EventBus()
      val system = ActorSystem(bus, failureSink = _ => failed.countDown())
      system.spawn(RestTradingGateway(StubClient(Left(ExchangeError.Other("OKX API error: code=50013"))), SilentFeed, AccountId.Live, metas))
      bus.publish(intentOf(orderOf(0.001)))
      assert(failed.await(2, TimeUnit.SECONDS), "结果不确定必须终止")

  test("精度收不下的单: 不发往交易所, 以同一种拒单回流"):
    // 客户端的 placeOrder 会 fail —— 它被触达就说明这一单不该发却发了。
    runGateway(StubClient(Left(ExchangeError.Network("客户端不该被触达")))) { (bus, incomes, _) =>
      bus.publish(intentOf(orderOf(0.0004))) // 低于 minOrderSize, 且取整后为 0
      val update = receivedError(incomes)
      assertEquals(update.clientOrderId, Some("c1"))
      update.status match
        case OrderStatus.Error(reason) => assert(reason.contains("最小下单量"), reason)
        case other                     => fail(s"应是精度拒单: $other")
    }

  test("网络错误下单结果不确定 -> 抛错终止作用域"):
    intercept[IllegalStateException] {
      runGateway(StubClient(Left(ExchangeError.Network("connection reset")))) { (bus, _, system) =>
        bus.publish(intentOf(orderOf(0.001)))
        system.awaitShutdown()
      }
    }
