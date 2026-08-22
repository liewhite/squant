package hft.state

import hft.domain.*
import hft.event.{AnyEvent, Event, Topics}

class SymbolStateSpec extends munit.FunSuite:
  private val symbol = "BTCUSDT"
  private val t0 = 1_700_000_000_000L

  private def newOrder(clientOrderId: String, side: Side = Side.Long): Order =
    Order(
      id = "",
      exchange = Exchange.Binance,
      symbol = symbol,
      side = side,
      orderType = OrderType.Limit(50000.0, TimeInForce.GTC),
      quantity = 0.01,
      reduceOnly = false,
      clientOrderId = clientOrderId,
    )

  private def orderUpdate(clientOrderId: String, status: OrderStatus, fillSize: Quantity = 0.0): AnyEvent =
    Event.at(Topics.OrderUpdate, OrderUpdate(
          orderId = "ex-1",
          clientOrderId = Some(clientOrderId),
          exchange = Exchange.Binance,
          symbol = symbol,
          side = Side.Long,
          status = status,
          price = 50000.0,
          quantity = 0.01,
          filledQuantity = 0.0,
          fillSize = fillSize,
          timestamp = t0,
        ), t0)

  test("订单生命周期: 登记 -> 交易所确认 (回填 orderId) -> 终态移除"):
    val state = SymbolState(symbol)
    state.addPendingOrder(newOrder("c1"), t0)
    assert(state.hasPendingOrders)
    assertEquals(state.pendingOrders.head.status, OrderStatus.Created)

    state.apply(orderUpdate("c1", OrderStatus.Pending))
    val confirmed = state.pendingOrders.head
    assertEquals(confirmed.status, OrderStatus.Pending)
    assertEquals(confirmed.order.id, "ex-1") // orderId 已回填

    state.apply(orderUpdate("c1", OrderStatus.Filled))
    assert(!state.hasPendingOrders)

  test("启动时同步的未知挂单注册到 pending"):
    val state = SymbolState(symbol)
    state.apply(orderUpdate("external-1", OrderStatus.Pending))
    assert(state.hasPendingOrders)
    assertEquals(state.pendingOrders.head.order.clientOrderId, "external-1")

  test("Created 订单超时未确认 -> 结果不确定，抛错终止"):
    val state = SymbolState(symbol)
    state.addPendingOrder(newOrder("created"), t0)
    intercept[RuntimeException] {
      state.failOnTimedOutOrders(now = t0 + 6000, timeoutMs = 5000)
    }

  test("已确认挂单不参与超时校验"):
    val state = SymbolState(symbol)
    state.addPendingOrder(newOrder("confirmed"), t0)
    state.apply(orderUpdate("confirmed", OrderStatus.Pending))
    state.failOnTimedOutOrders(now = t0 + 60000, timeoutMs = 5000) // 不抛

  test("超时未到不抛; timeoutMs=0 关闭校验"):
    val state = SymbolState(symbol)
    state.addPendingOrder(newOrder("c1"), t0)
    state.failOnTimedOutOrders(t0 + 1000, timeoutMs = 5000) // 不抛
    state.failOnTimedOutOrders(t0 + 60000, timeoutMs = 0)   // 不抛

  test("Fill 事件按方向乐观更新仓位，无仓位时创建"):
    val state = SymbolState(symbol)
    val fill = Fill(Exchange.Binance, symbol, Side.Long, price = 50000.0, size = 0.01, timestamp = t0)
    state.apply(Event.at(Topics.Fill, fill, t0))
    assertEqualsDouble(state.positionSize(Exchange.Binance), 0.01, 1e-12)

    state.apply(Event.at(Topics.Fill, fill.copy(side = Side.Short, size = 0.015), t0))
    assertEqualsDouble(state.positionSize(Exchange.Binance), -0.005, 1e-12)

  test("PositionUpdate 仅初始化一次，之后由 Fill 维护"):
    val state = SymbolState(symbol)
    val initial = Position(Exchange.Binance, symbol, size = 1.0, entryPrice = 50000.0, unrealizedPnl = 0.0)
    state.apply(Event.at(Topics.Position, initial, t0))
    assertEqualsDouble(state.positionSize(Exchange.Binance), 1.0, 1e-12)

    // 第二次 PositionUpdate 被忽略
    state.apply(Event.at(Topics.Position, initial.copy(size = 9.0), t0))
    assertEqualsDouble(state.positionSize(Exchange.Binance), 1.0, 1e-12)

  test("hasPendingSide 区分方向"):
    val state = SymbolState(symbol)
    state.addPendingOrder(newOrder("c1", Side.Short), t0)
    assert(state.hasPendingSide(Side.Short))
    assert(!state.hasPendingSide(Side.Long))
