package hft.messaging

import hft.domain.*

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

  private def orderUpdate(clientOrderId: String, status: OrderStatus, fillSize: Quantity = 0.0): IncomeEvent =
    IncomeEvent.at(
      t0,
      EventData.OrderUpdated(
        OrderUpdate(
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
        )
      ),
    )

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

  test("超时清理只移除 Created 状态的订单，已确认挂单不动"):
    val state = SymbolState(symbol)
    state.addPendingOrder(newOrder("created"), t0)
    state.addPendingOrder(newOrder("confirmed"), t0)
    state.apply(orderUpdate("confirmed", OrderStatus.Pending))

    val removed = state.removeTimedOutOrders(now = t0 + 6000, timeoutMs = 5000)
    assertEquals(removed, 1)
    assertEquals(state.pendingOrders.map(_.order.clientOrderId).toList, List("confirmed"))

  test("超时未到不清理; timeoutMs=0 关闭清理"):
    val state = SymbolState(symbol)
    state.addPendingOrder(newOrder("c1"), t0)
    assertEquals(state.removeTimedOutOrders(t0 + 1000, timeoutMs = 5000), 0)
    assertEquals(state.removeTimedOutOrders(t0 + 60000, timeoutMs = 0), 0)

  test("Fill 事件按方向乐观更新仓位，无仓位时创建"):
    val state = SymbolState(symbol)
    val fill = Fill(Exchange.Binance, symbol, Side.Long, price = 50000.0, size = 0.01, timestamp = t0)
    state.apply(IncomeEvent.at(t0, EventData.FillUpdate(fill)))
    assertEqualsDouble(state.positionSize(Exchange.Binance), 0.01, 1e-12)

    state.apply(IncomeEvent.at(t0, EventData.FillUpdate(fill.copy(side = Side.Short, size = 0.015))))
    assertEqualsDouble(state.positionSize(Exchange.Binance), -0.005, 1e-12)

  test("PositionUpdate 仅初始化一次，之后由 Fill 维护"):
    val state = SymbolState(symbol)
    val initial = Position(Exchange.Binance, symbol, size = 1.0, entryPrice = 50000.0, unrealizedPnl = 0.0)
    state.apply(IncomeEvent.at(t0, EventData.PositionUpdate(initial)))
    assertEqualsDouble(state.positionSize(Exchange.Binance), 1.0, 1e-12)

    // 第二次 PositionUpdate 被忽略
    state.apply(IncomeEvent.at(t0, EventData.PositionUpdate(initial.copy(size = 9.0))))
    assertEqualsDouble(state.positionSize(Exchange.Binance), 1.0, 1e-12)

  test("symbol 不匹配的事件被忽略"):
    val state = SymbolState(symbol)
    val other = BBO(Exchange.Binance, "ETHUSDT", 1.0, 1.0, 2.0, 1.0, t0)
    state.apply(IncomeEvent.at(t0, EventData.BboUpdate(other)))
    assertEquals(state.bbo(Exchange.Binance), None)

  test("hasPendingSide 区分方向"):
    val state = SymbolState(symbol)
    state.addPendingOrder(newOrder("c1", Side.Short), t0)
    assert(state.hasPendingSide(Side.Short))
    assert(!state.hasPendingSide(Side.Long))
