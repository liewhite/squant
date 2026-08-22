package hft.engine

import hft.domain.*
import hft.exchange.ExchangeClient
import hft.event.{AnyEvent, Event, EventBus, Interest, Topics}
import hft.strategy.{OrderIntent, OutcomeEvent}
import ox.supervised

/** OutcomeProcessor 的 fail-fast 语义:
  *   - 明确拒单 (4xx) / dry-run -> OrderUpdate(Error) 事件回流
  *   - 结果不确定 (网络错误) -> 抛错终止作用域
  */
class OutcomeProcessorSpec extends munit.FunSuite:

  /** 只实现 placeOrder 的 stub，其余方法不应被触达 */
  private class StubClient(placeResult: Either[ExchangeError, OrderId]) extends ExchangeClient:
    override def exchange: Exchange = Exchange.Binance
    override def placeOrder(order: Order): Either[ExchangeError, OrderId] = placeResult
    override def fetchAllSymbolMetas() = fail("unexpected call")
    override def cancelOrder(symbol: Symbol, orderId: OrderId) = fail("unexpected call")
    override def fetchPendingOrders(symbol: Symbol) = fail("unexpected call")
    override def setLeverage(symbol: Symbol, leverage: Int) = fail("unexpected call")
    override def fetchAccountInfo() = fail("unexpected call")
    override def fetchPositions() = fail("unexpected call")

  private val order = Order(
    id = "",
    exchange = Exchange.Binance,
    symbol = "BTCUSDT",
    side = Side.Long,
    orderType = OrderType.Market,
    quantity = 0.001,
    reduceOnly = false,
    clientOrderId = "c1",
  )

  private def receivedError(incomes: ox.channels.Source[AnyEvent]): OrderUpdate =
    val ev = incomes.receive()
    ev.as(Topics.OrderUpdate).getOrElse(fail(s"unexpected event: $ev"))

  test("dry-run: 信号以 OrderUpdate(Error) 回流清理 pending"):
    supervised:
      val bus = EventBus()
      val incomes = bus.subscribe(Set(Interest.All(Topics.OrderUpdate)))

      OutcomeProcessor(Map.empty, bus, dryRun = true).run()
      bus.publish(Event.local(OrderIntent, OutcomeEvent.PlaceOrders(Vector(order), "test")))

      val update = receivedError(incomes)
      assertEquals(update.clientOrderId, Some("c1"))
      assert(update.status.isInstanceOf[OrderStatus.Error])

  test("交易所明确拒单 (4xx): OrderUpdate(Error) 事件回流策略"):
    supervised:
      val bus = EventBus()
      val incomes = bus.subscribe(Set(Interest.All(Topics.OrderUpdate)))
      val clients = Map[Exchange, ExchangeClient](Exchange.Binance -> StubClient(Left(ExchangeError.Http(400, """{"code":-2019,"msg":"Margin is insufficient."}"""))))

      OutcomeProcessor(clients, bus, dryRun = false).run()
      bus.publish(Event.local(OrderIntent, OutcomeEvent.PlaceOrders(Vector(order), "test")))

      val update = receivedError(incomes)
      assertEquals(update.clientOrderId, Some("c1"))
      assert(update.status.isInstanceOf[OrderStatus.Error])

  test("网络错误下单结果不确定 -> 抛错终止作用域"):
    intercept[IllegalStateException] {
      supervised:
        val bus = EventBus()
        val incomes = bus.subscribe(Set(Interest.All(Topics.OrderUpdate)))
        val clients = Map[Exchange, ExchangeClient](Exchange.Binance -> StubClient(Left(ExchangeError.Network("connection reset"))))

        OutcomeProcessor(clients, bus, dryRun = false).run()
        bus.publish(Event.local(OrderIntent, OutcomeEvent.PlaceOrders(Vector(order), "test")))
        incomes.receive() // 阻塞至下单 fork 失败取消作用域
    }

  test("策略引用未配置的交易所 -> 装配错误，作用域终止"):
    intercept[IllegalStateException] {
      supervised:
        val bus = EventBus()
        val incomes = bus.subscribe(Set(Interest.All(Topics.OrderUpdate)))

        OutcomeProcessor(Map.empty, bus, dryRun = false).run()
        bus.publish(Event.local(OrderIntent, OutcomeEvent.PlaceOrders(Vector(order), "test")))
        incomes.receive()
    }
