package hft.exchange

import hft.actor.ActorSystem
import hft.domain.*
import hft.event.Commands.{AccountOutcome, OrderIntent, OutcomeEvent}
import hft.event.{AnyEvent, Event, EventBus, Interest, Topics}
import hft.exchange.AccountReport
import ox.supervised

import java.util.concurrent.ConcurrentLinkedQueue
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
    override def connect(sink: AccountReport => Unit, fork: (=> Unit) => Unit): Unit = ()

  /** 可手动喂报告的汇报面 —— 用来驱动柜台的记账与排序 */
  private class ManualFeed extends AccountFeed:
    @volatile private var sink: AccountReport => Unit = scala.compiletime.uninitialized
    override def exchange: Exchange = Exchange.Binance
    override def connect(s: AccountReport => Unit, fork: (=> Unit) => Unit): Unit = sink = s
    def emit(report: AccountReport): Unit = sink(report)

  /** 有求必应的桩客户端 —— 驱动汇报面路径时不该被执行面打扰 */
  private class QuietClient extends TradingClient:
    override def exchange: Exchange = Exchange.Binance
    override def placeOrder(order: ExchangeOrder) = Right("ignored")
    override def fetchAllSymbolMetas() = Right(Vector(meta))
    override def cancelOrder(symbol: Symbol, ref: OrderRef) = Right(())
    override def fetchPendingOrders(symbol: Symbol) = Right(Vector.empty)
    override def setLeverage(symbol: Symbol, leverage: Int) = Right(())
    override def fetchAccountInfo() = Right(AccountInfo(AccountId.Live, Exchange.Binance, 10_000.0, 0.0))
    override def fetchPositions() = Right(Vector.empty)

  private def eventually(what: => String)(cond: => Boolean): Unit =
    val deadline = System.nanoTime() + 3_000_000_000L
    while System.nanoTime() < deadline && !cond do Thread.sleep(5)
    assert(cond, s"等待超时: $what")

  /** 装一台真实形态的柜台, 用手动汇报面驱动它 */
  private def withFeed(body: (ManualFeed, ConcurrentLinkedQueue[AnyEvent]) => Unit): Unit =
    supervised:
      val bus = EventBus()
      val seen = ConcurrentLinkedQueue[AnyEvent]()
      val mailbox = bus.subscribe(Set(Interest.All(Topics.Position), Interest.All(Topics.Fill), Interest.All(Topics.OrderUpdate)))
      ox.forkDiscard { mailbox.events.foreach(seen.add) }
      val feed = ManualFeed()
      ActorSystem(bus).spawn(RestTradingGateway(QuietClient(), feed, AccountId.Live, metas))
      body(feed, seen)

  private def executed(orderId: String, cumulative: Double) =
    AccountReport.Executed(orderId, "BTCUSDT", Side.Long, Price(100.0), Coin(cumulative), 1L)

  private def statusChanged(orderId: String, status: OrderStatus, filled: Double) =
    AccountReport.OrderStatusChanged(orderId, Some("c1"), "BTCUSDT", Side.Long, status, Price(100.0), Coin(1.0), Coin(filled), 1L)

  private def kinds(seen: ConcurrentLinkedQueue[AnyEvent]): Vector[String] =
    seen.asScala.toVector.map(ev =>
      if ev.is(Topics.Position) then "position"
      else if ev.is(Topics.Fill) then "fill"
      else "orderUpdate"
    )

  private def positionSizes(seen: ConcurrentLinkedQueue[AnyEvent]): Vector[Double] =
    seen.asScala.toVector.flatMap(_.as(Topics.Position)).map(_.size.value)

  test("一笔成交出柜台的顺序: 仓位快照先于其余回报"):
    // 顺序是**构造**出来的 —— 柜台先记账、立刻发仓位, 不依赖各家推送的到达次序。
    withFeed { (feed, seen) =>
      feed.emit(executed("o1", cumulative = 0.5))
      feed.emit(statusChanged("o1", OrderStatus.Filled, filled = 0.5))
      eventually(kinds(seen).toString)(kinds(seen).size == 3)
      assertEquals(kinds(seen), Vector("position", "fill", "orderUpdate"))
      assertEquals(positionSizes(seen), Vector(0.5))
    }

  test("重复推送不重复记账 —— 累计量没涨就什么都不发"):
    withFeed { (feed, seen) =>
      feed.emit(executed("o1", cumulative = 0.5))
      feed.emit(executed("o1", cumulative = 0.5)) // 同一条重放
      eventually("首笔应已入账")(kinds(seen).size >= 2)
      Thread.sleep(100)
      assertEquals(kinds(seen), Vector("position", "fill"), "重放不该产生第二笔")
      assertEquals(positionSizes(seen), Vector(0.5))
    }

  test("跨频道乱序: 订单回报先到也能记上账, 成交随后到达时不重复计"):
    // Bybit 的成交与订单状态来自两条频道, 交易所不保证先后。柜台以累计量为准,
    // 谁先报谁触发记账 —— 后到的那条增量为零。
    withFeed { (feed, seen) =>
      feed.emit(statusChanged("o1", OrderStatus.Filled, filled = 0.5)) // 订单回报先到, 带累计量
      eventually("订单回报应已触发记账")(kinds(seen).size == 3)
      assertEquals(kinds(seen), Vector("position", "fill", "orderUpdate"), "先到的那条同样先发仓位")

      feed.emit(executed("o1", cumulative = 0.5)) // 成交频道随后到达
      Thread.sleep(100)
      assertEquals(positionSizes(seen), Vector(0.5), "同一笔不该被记两次")
    }

  test("分批成交按增量入账, 每次都先发新仓位"):
    withFeed { (feed, seen) =>
      feed.emit(executed("o1", cumulative = 0.3))
      feed.emit(executed("o1", cumulative = 0.8)) // 又成交 0.5
      eventually(kinds(seen).toString)(kinds(seen).size == 4)
      assertEquals(kinds(seen), Vector("position", "fill", "position", "fill"))
      assertEquals(positionSizes(seen), Vector(0.3, 0.8))
      val fills = seen.asScala.toVector.flatMap(_.as(Topics.Fill)).map(_.size.value)
      assertEquals(fills, Vector(0.3, 0.5), "发出去的是增量, 不是累计")
    }

  test("交易所报的仓位不进总线 —— 总线上的仓位只有账本一个来源"):
    withFeed { (feed, seen) =>
      feed.emit(AccountReport.PositionReported("BTCUSDT", Coin(9.9), 1L))
      Thread.sleep(100)
      assert(seen.asScala.isEmpty, s"对账用的读数不该外流: ${kinds(seen)}")
    }


  /** 只实现 placeOrder 的 stub，其余方法不应被触达 */
  private class StubClient(placeResult: Either[ExchangeError, OrderId]) extends TradingClient:
    override def exchange: Exchange = Exchange.Binance
    override def placeOrder(order: ExchangeOrder): Either[ExchangeError, OrderId] = placeResult
    override def fetchAllSymbolMetas() = fail("unexpected call")
    override def cancelOrder(symbol: Symbol, ref: OrderRef) = fail("unexpected call")
    override def fetchPendingOrders(symbol: Symbol) = fail("unexpected call")
    override def setLeverage(symbol: Symbol, leverage: Int) = fail("unexpected call")
    // 柜台启动即周期刷净值 —— 给一个固定读数, 免得测试依赖网络
    override def fetchAccountInfo() = Right(AccountInfo(AccountId.Live, Exchange.Binance, 10_000.0, 0.0))
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

  private def runGateway(client: TradingClient)(body: (EventBus, ox.channels.Source[AnyEvent]) => Unit): Unit =
    supervised:
      val bus = EventBus()
      val incomes = bus.subscribe(Set(Interest.All(Topics.OrderUpdate)))
      ActorSystem(bus).spawn(RestTradingGateway(client, SilentFeed, AccountId.Live, metas))
      body(bus, incomes.events)

  test("dry-run (DryRunClient): 信号以 OrderUpdate(Error) 回流清理 pending"):
    // dry-run 不是柜台里的开关, 而是换一个客户端实现 —— 它以 4xx 拒单返回,
    // 走的正是既有的"确定性失败"通道, 所以这里的期望与真实拒单那条用例完全一致。
    runGateway(DryRunClient(StubClient(Right("ignored")))) { (bus, incomes) =>
      bus.publish(intentOf(orderOf(0.001)))
      val update = receivedError(incomes)
      assertEquals(update.clientOrderId, Some("c1"))
      assert(update.status.isInstanceOf[OrderStatus.Error])
    }

  test("交易所明确拒单 (4xx): OrderUpdate(Error) 回流策略"):
    runGateway(StubClient(Left(ExchangeError.Http(400, """{"code":-2019,"msg":"Margin is insufficient."}""")))) { (bus, incomes) =>
      bus.publish(intentOf(orderOf(0.001)))
      val update = receivedError(incomes)
      assertEquals(update.clientOrderId, Some("c1"))
      assert(update.status.isInstanceOf[OrderStatus.Error])
    }

  test("精度收不下的单: 不发往交易所, 以同一种拒单回流"):
    // 客户端的 placeOrder 会 fail —— 它被触达就说明这一单不该发却发了。
    runGateway(StubClient(Left(ExchangeError.Network("客户端不该被触达")))) { (bus, incomes) =>
      bus.publish(intentOf(orderOf(0.0004))) // 低于 minOrderSize, 且取整后为 0
      val update = receivedError(incomes)
      assertEquals(update.clientOrderId, Some("c1"))
      update.status match
        case OrderStatus.Error(reason) => assert(reason.contains("最小下单量"), reason)
        case other                     => fail(s"应是精度拒单: $other")
    }

  test("网络错误下单结果不确定 -> 抛错终止作用域"):
    intercept[IllegalStateException] {
      runGateway(StubClient(Left(ExchangeError.Network("connection reset")))) { (bus, incomes) =>
        bus.publish(intentOf(orderOf(0.001)))
        incomes.receive() // 阻塞至下单 fork 失败取消作用域
      }
    }
