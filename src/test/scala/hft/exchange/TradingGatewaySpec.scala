package hft.exchange

import hft.actor.ActorSystem
import hft.domain.*
import hft.event.Commands.{AccountOutcome, OrderIntent, OutcomeEvent}
import hft.event.{AnyEvent, Event, EventBus, Interest, Topics}
import ox.supervised
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
    override def connect(account: AccountId, publish: AnyEvent => Unit, fork: (=> Unit) => Unit): Unit = ()

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
