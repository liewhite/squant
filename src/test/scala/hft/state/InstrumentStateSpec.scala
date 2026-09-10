package hft.state

import hft.domain.*
import hft.event.{AnyEvent, Event, Topics}
import hft.TestUnits.given

class InstrumentStateSpec extends munit.FunSuite:
  private val instrument = Instrument.perp(Exchange.Binance, "BTCUSDT")
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

  private def orderUpdate(clientOrderId: String, status: OrderStatus, reduceOnly: Boolean = false): AnyEvent =
    Event.at(Topics.OrderUpdate, OrderUpdate(
          account = AccountId.Live,
          orderId = "ex-1",
          clientOrderId = Some(clientOrderId),
          exchange = Exchange.Binance,
          symbol = symbol,
          side = Side.Long,
          status = status,
          price = 50000.0,
          quantity = 0.01,
          filledQuantity = 0.0,
          reduceOnly = reduceOnly,
          timestamp = t0,
        ), t0)

  test("订单生命周期: 登记 -> 交易所确认 (回填 orderId) -> 终态移除"):
    val state = InstrumentState(instrument)
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
    val state = InstrumentState(instrument)
    state.apply(orderUpdate("external-1", OrderStatus.Pending))
    assert(state.hasPendingOrders)
    assertEquals(state.pendingOrders.head.order.clientOrderId, "external-1")

  test("Created 订单超时未确认 -> 结果不确定，抛错终止"):
    val state = InstrumentState(instrument)
    state.addPendingOrder(newOrder("created"), t0)
    intercept[RuntimeException] {
      state.failOnTimedOutOrders(now = t0 + 6000, timeoutMs = 5000)
    }

  test("已确认挂单不参与超时校验"):
    val state = InstrumentState(instrument)
    state.addPendingOrder(newOrder("confirmed"), t0)
    state.apply(orderUpdate("confirmed", OrderStatus.Pending))
    state.failOnTimedOutOrders(now = t0 + 60000, timeoutMs = 5000) // 不抛

  test("超时未到不抛; timeoutMs=0 关闭校验"):
    val state = InstrumentState(instrument)
    state.addPendingOrder(newOrder("c1"), t0)
    state.failOnTimedOutOrders(t0 + 1000, timeoutMs = 5000) // 不抛
    state.failOnTimedOutOrders(t0 + 60000, timeoutMs = 0)   // 不抛

  test("成交不再改仓位 —— 那本账在柜台, 这里只接住它的快照"):
    // 从前这里靠 Fill 自己累加。改到柜台之后, 策略侧不再有第二份仓位算法:
    // 两份实现迟早在某个边界 (反手、reduceOnly 截断) 上分叉, 而没有任何症状。
    val state = InstrumentState(instrument)
    val fill = Fill(AccountId.Live, Exchange.Binance, symbol, Side.Long, price = 50000.0, size = 0.01, timestamp = t0)
    state.apply(Event.at(Topics.Fill, fill, t0))
    assertEqualsDouble(state.positionSize.value, 0.0, 1e-12, "成交不该被这里算进仓位")

  test("仓位快照始终覆盖 —— 柜台发的每一条都比上一条新"):
    val state = InstrumentState(instrument)
    val initial = Position(AccountId.Live, Exchange.Binance, symbol, size = 1.0)
    state.apply(Event.at(Topics.Position, initial, t0))
    assertEqualsDouble(state.positionSize.value, 1.0, 1e-12)

    // 从前这条会被忽略 (只认第一条), 于是漂移永远修不回来
    state.apply(Event.at(Topics.Position, initial.copy(size = 9.0), t0))
    assertEqualsDouble(state.positionSize.value, 9.0, 1e-12)

  test("hasPendingSide 区分方向"):
    val state = InstrumentState(instrument)
    state.addPendingOrder(newOrder("c1", Side.Short), t0)
    assert(state.hasPendingSide(Side.Short))
    assert(!state.hasPendingSide(Side.Long))

  test("对齐接管的挂单: reduceOnly 取回报里的真值, 不本地填 false"):
    // 策略拿 reduceOnly 给 resting 单分槽 (止盈槽 vs 加仓槽, 见 MacdGridStrategy.placeGrid)。
    // 从前这里本地伪造成 false: 重启后**真正的止盈单被归进加仓槽**, 策略以为没有止盈单
    // 于是再挂一张, 而那张真的止盈单还在簿上。
    val state = InstrumentState(instrument)
    state.apply(orderUpdate("adopted-tp", OrderStatus.Pending, reduceOnly = true))
    val adopted = state.pendingOrders.headOption.getOrElse(fail("接管的挂单应登记到 pending"))
    assertEquals(adopted.order.reduceOnly, true, "reduceOnly 必须来自回报, 不能本地填 false")
    assert(state.hasPendingSide(Side.Long))

  test("对齐接管的挂单: reduceOnly=false 也如实记下"):
    val state = InstrumentState(instrument)
    state.apply(orderUpdate("adopted-add", OrderStatus.Pending, reduceOnly = false))
    assertEquals(state.pendingOrders.head.order.reduceOnly, false)

  test("没订阅的标的取不到状态 —— 那个 0 不是空仓, 是问错了地方"):
    // 从前状态按交易对索引, "没声明因而从未对齐的交易所"与"已对齐的空仓"读数都是 0,
    // 只能在 positionSize(exchange) 上加一道运行时守卫挡着。现在按标的索引, 问错了地方
    // 根本拿不到状态 —— 同一条不变量, 从运行时守卫变成了结构保证。
    val mgr = StateManager(List(instrument), orderTimeoutMs = 0L)
    assertEquals(
      mgr.instrumentState(instrument).map(_.positionSize.value),
      Some(0.0),
      "已订阅的标的: 没记录 = 空仓",
    )
    assertEquals(
      mgr.instrumentState(Instrument.perp(Exchange.Okx, instrument.symbol)),
      None,
      "没订阅的标的拿不到状态, 不必再靠运行时守卫挡",
    )
