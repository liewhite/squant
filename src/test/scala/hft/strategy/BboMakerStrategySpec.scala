package hft.strategy

import hft.domain.*
import hft.messaging.{EventData, IncomeEvent, StateManager}

class BboMakerStrategySpec extends munit.FunSuite:
  private val symbol = "BTCUSDT"
  private val t0 = 1_700_000_000_000L

  private def newStrategy(maxLeverage: Double = 2.0) =
    BboMakerStrategy(
      targetExchange = Exchange.Binance,
      symbol = symbol,
      offsetRatio = 0.0001,
      orderSize = 0.002,
      maxLeverage = maxLeverage,
    )

  private def newState() = StateManager(List(symbol), orderTimeoutMs = 5000)

  private def bboEvent(bid: Price, ask: Price): IncomeEvent =
    IncomeEvent.at(t0, EventData.BboUpdate(BBO(Exchange.Binance, symbol, bid, 1.0, ask, 1.0, t0)))

  /** 复刻 Executor 的顺序: 先更新状态再调用策略 */
  private def feed(strategy: Strategy, state: StateManager, event: IncomeEvent): Vector[OutcomeEvent] =
    state.apply(event)
    strategy.onEvent(event, state)

  private def inject(state: StateManager, data: EventData): Unit =
    state.apply(IncomeEvent.at(t0, data))

  private def placedOrders(signals: Vector[OutcomeEvent]): Vector[Order] =
    signals.collect { case OutcomeEvent.PlaceOrders(orders, _) => orders }.flatten

  /** 模拟 Executor 对下单信号的 pending 登记 */
  private def registerPending(state: StateManager, order: Order, clientOrderId: String): Unit =
    state.addPendingOrder(order.copy(clientOrderId = clientOrderId), 0L)

  private def confirm(state: StateManager, clientOrderId: String, orderId: OrderId, side: Side): Unit =
    inject(
      state,
      EventData.OrderUpdated(
        OrderUpdate(orderId, Some(clientOrderId), Exchange.Binance, symbol, side, OrderStatus.Pending, 0.0, 0.002, 0.0, 0.0, t0)
      ),
    )

  test("账户净值未知时不挂任何单 (安全侧)"):
    val (strategy, state) = (newStrategy(), newState())
    assertEquals(feed(strategy, state, bboEvent(50000.0, 50001.0)), Vector.empty)

  test("双边挂单: 买单 bid 下方 0.01%，卖单 ask 上方 0.01%，PostOnly"):
    val (strategy, state) = (newStrategy(), newState())
    inject(state, EventData.AccountInfoUpdate(Exchange.Binance, AccountInfo(equity = 1000.0, notional = 0.0)))
    inject(state, EventData.MarkPriceUpdate(MarkPrice(Exchange.Binance, symbol, 50000.0, t0)))

    val orders = placedOrders(feed(strategy, state, bboEvent(50000.0, 50001.0)))
    assertEquals(orders.map(_.side).toSet, Set(Side.Long, Side.Short))
    val buy = orders.find(_.side == Side.Long).get
    val sell = orders.find(_.side == Side.Short).get
    assertEquals(buy.orderType, OrderType.Limit(50000.0 * 0.9999, TimeInForce.PostOnly))
    assertEquals(sell.orderType, OrderType.Limit(50001.0 * 1.0001, TimeInForce.PostOnly))
    assertEquals(buy.quantity, 0.002)
    assertEquals(sell.quantity, 0.002)

  test("每边最多一张挂单，不重复挂出"):
    val (strategy, state) = (newStrategy(), newState())
    inject(state, EventData.AccountInfoUpdate(Exchange.Binance, AccountInfo(1000.0, 0.0)))
    inject(state, EventData.MarkPriceUpdate(MarkPrice(Exchange.Binance, symbol, 50000.0, t0)))

    val orders = placedOrders(feed(strategy, state, bboEvent(50000.0, 50001.0)))
    orders.zipWithIndex.foreach((o, i) => registerPending(state, o, s"c$i"))

    assertEquals(feed(strategy, state, bboEvent(50000.0, 50001.0)), Vector.empty)

  test("杠杆率约束: 成交后将突破上限的方向不挂单"):
    val (strategy, state) = (newStrategy(maxLeverage = 2.0), newState())
    // 净值 110, 现有多头 0.004 (标记价 50000): 买单成交后 0.006*50000/110≈2.7 超限; 卖单成交后 0.002*50000/110≈0.9 允许
    inject(state, EventData.AccountInfoUpdate(Exchange.Binance, AccountInfo(110.0, 0.0)))
    inject(state, EventData.MarkPriceUpdate(MarkPrice(Exchange.Binance, symbol, 50000.0, t0)))
    inject(state, EventData.PositionUpdate(Position(Exchange.Binance, symbol, 0.004, 50000.0, 0.0)))

    val orders = placedOrders(feed(strategy, state, bboEvent(50000.0, 50001.0)))
    assertEquals(orders.map(_.side), Vector(Side.Short)) // 只挂降杠杆方向

  test("净值过小时双边都不挂"):
    val (strategy, state) = (newStrategy(maxLeverage = 2.0), newState())
    // 0.002*50000/40 = 2.5 >= 2
    inject(state, EventData.AccountInfoUpdate(Exchange.Binance, AccountInfo(40.0, 0.0)))
    inject(state, EventData.MarkPriceUpdate(MarkPrice(Exchange.Binance, symbol, 50000.0, t0)))
    assertEquals(feed(strategy, state, bboEvent(50000.0, 50001.0)), Vector.empty)

  test("价格维护: 已确认挂单偏离目标价过远 -> 撤单一次，不重复撤"):
    val (strategy, state) = (newStrategy(), newState())
    inject(state, EventData.AccountInfoUpdate(Exchange.Binance, AccountInfo(1000.0, 0.0)))
    inject(state, EventData.MarkPriceUpdate(MarkPrice(Exchange.Binance, symbol, 50000.0, t0)))

    // 远离目标价的存量买单 (如启动接管的遗留单)，已获交易所确认
    val stale = Order("", Exchange.Binance, symbol, Side.Long, OrderType.Limit(49000.0, TimeInForce.PostOnly), 0.002, false, "b1")
    registerPending(state, stale, "b1")
    confirm(state, "b1", "ex-1", Side.Long)

    val first = feed(strategy, state, bboEvent(50000.0, 50001.0))
    assertEquals(
      first.collect { case c: OutcomeEvent.CancelOrder => c },
      Vector(OutcomeEvent.CancelOrder(Exchange.Binance, symbol, "ex-1")),
    )
    // 撤单确认到达前不重复撤
    val second = feed(strategy, state, bboEvent(50000.0, 50001.0))
    assert(second.collect { case c: OutcomeEvent.CancelOrder => c }.isEmpty)

  test("部分成交的挂单同样参与价格维护 (撤掉剩余部分)"):
    val (strategy, state) = (newStrategy(), newState())
    inject(state, EventData.AccountInfoUpdate(Exchange.Binance, AccountInfo(1000.0, 0.0)))
    inject(state, EventData.MarkPriceUpdate(MarkPrice(Exchange.Binance, symbol, 50000.0, t0)))

    val stale = Order("", Exchange.Binance, symbol, Side.Long, OrderType.Limit(49000.0, TimeInForce.PostOnly), 0.002, false, "b1")
    registerPending(state, stale, "b1")
    inject(
      state,
      EventData.OrderUpdated(
        OrderUpdate("ex-1", Some("b1"), Exchange.Binance, symbol, Side.Long, OrderStatus.PartiallyFilled(0.001), 49000.0, 0.002, 0.001, 0.001, t0)
      ),
    )

    val signals = feed(strategy, state, bboEvent(50000.0, 50001.0))
    assertEquals(
      signals.collect { case c: OutcomeEvent.CancelOrder => c },
      Vector(OutcomeEvent.CancelOrder(Exchange.Binance, symbol, "ex-1")),
    )

  test("未确认 (Created) 的挂单不撤 (尚无交易所 orderId)"):
    val (strategy, state) = (newStrategy(), newState())
    inject(state, EventData.AccountInfoUpdate(Exchange.Binance, AccountInfo(1000.0, 0.0)))
    inject(state, EventData.MarkPriceUpdate(MarkPrice(Exchange.Binance, symbol, 50000.0, t0)))

    val stale = Order("", Exchange.Binance, symbol, Side.Long, OrderType.Limit(49000.0, TimeInForce.PostOnly), 0.002, false, "b1")
    registerPending(state, stale, "b1") // 不 confirm

    val signals = feed(strategy, state, bboEvent(50000.0, 50001.0))
    assert(signals.collect { case c: OutcomeEvent.CancelOrder => c }.isEmpty)

  test("成交后继续挂: 买单成交 -> 仓位更新 -> 重新挂买单"):
    val (strategy, state) = (newStrategy(), newState())
    inject(state, EventData.AccountInfoUpdate(Exchange.Binance, AccountInfo(1000.0, 0.0)))
    inject(state, EventData.MarkPriceUpdate(MarkPrice(Exchange.Binance, symbol, 50000.0, t0)))

    val orders = placedOrders(feed(strategy, state, bboEvent(50000.0, 50001.0)))
    val buy = orders.find(_.side == Side.Long).get
    val sell = orders.find(_.side == Side.Short).get
    registerPending(state, buy, "b1")
    registerPending(state, sell, "s1")
    confirm(state, "b1", "ex-b", Side.Long)

    // 买单全部成交: 终态移除 pending + Fill 更新仓位
    inject(
      state,
      EventData.OrderUpdated(
        OrderUpdate("ex-b", Some("b1"), Exchange.Binance, symbol, Side.Long, OrderStatus.Filled, 49995.0, 0.002, 0.002, 0.002, t0)
      ),
    )
    inject(state, EventData.FillUpdate(Fill(Exchange.Binance, symbol, Side.Long, 49995.0, 0.002, t0)))

    val requote = placedOrders(feed(strategy, state, bboEvent(50000.0, 50001.0)))
    assertEquals(requote.map(_.side), Vector(Side.Long)) // 卖单仍挂着，只补买单
