package strategy.crashchase.logic

import hft.strategy.{OutcomeEvent, Strategy}

import hft.domain.*
import hft.messaging.{EventData, IncomeEvent, StateManager}

class CrashChaseStrategySpec extends munit.FunSuite:
  private val symbol = "SIRENUSDT"
  private val t0 = 1_700_000_000_000L

  private def newStrategy(
      crashThreshold: Double = 0.02,
      maxLeverage: Double = 3.0,
      takeProfitRatio: Double = 0.01,
      stopLossRatio: Double = 0.03,
      cooldownMs: Long = 60_000,
  ) =
    CrashChaseStrategy(
      targetExchange = Exchange.Binance,
      symbol = symbol,
      windowMs = 60_000,
      crashThreshold = crashThreshold,
      entryOffsetRatio = 0.0005,
      rungUsdt = 500.0,
      maxLeverage = maxLeverage,
      takeProfitRatio = takeProfitRatio,
      stopLossRatio = stopLossRatio,
      cooldownMs = cooldownMs,
      repriceToleranceRatio = 0.0003,
      sampleIntervalMs = 500,
    )

  private def newState() = StateManager(List(symbol), orderTimeoutMs = 5000)

  private def bboEvent(bid: Price, ask: Price, ts: Timestamp): IncomeEvent =
    IncomeEvent.at(ts, EventData.BboUpdate(BBO(Exchange.Binance, symbol, bid, 1.0, ask, 1.0, ts)))

  private def feed(strategy: Strategy, state: StateManager, event: IncomeEvent): Vector[OutcomeEvent] =
    state.apply(event)
    strategy.onEvent(event, state)

  private def inject(state: StateManager, data: EventData, ts: Timestamp = t0): Unit =
    state.apply(IncomeEvent.at(ts, data))

  /** 成交事件须同时喂给 StateManager 与策略 (策略自维护持仓均价)，复刻 Runner 流程 */
  private def feedFill(strategy: Strategy, state: StateManager, fill: Fill): Unit =
    feed(strategy, state, IncomeEvent.at(fill.timestamp, EventData.FillUpdate(fill)))

  private def placedOrders(signals: Vector[OutcomeEvent]): Vector[Order] =
    signals.collect { case OutcomeEvent.PlaceOrders(orders, _) => orders }.flatten

  private def registerPending(state: StateManager, order: Order, clientOrderId: String): Unit =
    state.addPendingOrder(order.copy(clientOrderId = clientOrderId), 0L)

  /** 把策略产出的下单信号登记为 pending (复刻 Runner 行为)，返回登记的 (order, clientId) */
  private def registerAll(state: StateManager, orders: Vector[Order]): Vector[(Order, String)] =
    orders.zipWithIndex.map { (o, i) =>
      val cid = s"c$i"
      registerPending(state, o, cid)
      (o, cid)
    }

  private def confirm(state: StateManager, clientId: String, orderId: OrderId, order: Order, ts: Timestamp = t0): Unit =
    inject(
      state,
      EventData.OrderUpdated(
        OrderUpdate(orderId, Some(clientId), Exchange.Binance, symbol, order.side, OrderStatus.Pending,
          order.orderType match { case OrderType.Limit(p, _) => p; case _ => 0.0 }, order.quantity, 0.0, 0.0, ts)
      ),
      ts,
    )

  private def withAccount(state: StateManager, equity: Double, notional: Double = 0.0): Unit =
    inject(state, EventData.AccountInfoUpdate(Exchange.Binance, AccountInfo(equity, notional)))

  test("净值未知时不交易 (安全侧)"):
    val (strategy, state) = (newStrategy(), newState())
    assertEquals(feed(strategy, state, bboEvent(1.0, 1.001, t0)), Vector.empty)

  test("平稳行情不挂任何单 (只监控)"):
    val (strategy, state) = (newStrategy(), newState())
    withAccount(state, 10_000.0)
    (0 to 10).foreach { i =>
      val px = 1.0 - i * 0.0001
      assertEquals(feed(strategy, state, bboEvent(px, px + 0.001, t0 + i * 1000L)), Vector.empty)
    }

  test("突然暴跌触发: 在 ask 上方挂 PostOnly 卖单加空"):
    val (strategy, state) = (newStrategy(crashThreshold = 0.02), newState())
    withAccount(state, 10_000.0)
    feed(strategy, state, bboEvent(1.0, 1.001, t0)) // 峰值 ~1.0005
    val orders = placedOrders(feed(strategy, state, bboEvent(0.97, 0.971, t0 + 30_000L)))
    assertEquals(orders.size, 1)
    val sell = orders.head
    assertEquals(sell.side, Side.Short)
    assertEquals(sell.reduceOnly, false)
    sell.orderType match
      case OrderType.Limit(px, tif) =>
        assertEquals(tif, TimeInForce.PostOnly)
        assertEqualsDouble(px, 0.971 * (1 + 0.0005), 1e-9)
      case other => fail(s"expected PostOnly limit, got $other")

  test("加空成交后维护平仓买单 (reduceOnly PostOnly 买, 价=均价*(1-tp))"):
    val (strategy, state) = (newStrategy(takeProfitRatio = 0.01), newState())
    withAccount(state, 10_000.0)
    feed(strategy, state, bboEvent(1.0, 1.001, t0))
    val sells = placedOrders(feed(strategy, state, bboEvent(0.97, 0.971, t0 + 30_000L)))
    val (sell, cid) = registerAll(state, sells).head
    confirm(state, cid, "ex-s", sell, t0 + 30_000L)
    // 卖单成交 (加空), 均价 0.98
    feedFill(strategy, state, Fill(Exchange.Binance, symbol, Side.Short, 0.98, 500.0, t0 + 31_000L))
    withAccount(state, 10_000.0, notional = 490.0)
    val signals = feed(strategy, state, bboEvent(0.965, 0.966, t0 + 31_500L))
    val cover = placedOrders(signals).find(_.side == Side.Long)
    assert(cover.isDefined, "应挂出平仓买单")
    assertEquals(cover.get.reduceOnly, true)
    cover.get.orderType match
      case OrderType.Limit(px, tif) =>
        assertEquals(tif, TimeInForce.PostOnly)
        assertEqualsDouble(px, 0.98 * (1 - 0.01), 1e-9)
      case other => fail(s"expected PostOnly limit, got $other")

  test("反弹破止损线: 平仓单切换为 Market (taker)"):
    val (strategy, state) = (newStrategy(stopLossRatio = 0.03), newState())
    withAccount(state, 10_000.0)
    feed(strategy, state, bboEvent(1.0, 1.001, t0))
    val sells = placedOrders(feed(strategy, state, bboEvent(0.97, 0.971, t0 + 30_000L)))
    val (sell, cid) = registerAll(state, sells).head
    confirm(state, cid, "ex-s", sell, t0 + 30_000L)
    feedFill(strategy, state, Fill(Exchange.Binance, symbol, Side.Short, 0.98, 500.0, t0 + 31_000L))
    withAccount(state, 9_000.0, notional = 510.0)
    // mid 反弹到 1.02 > 0.98*(1+0.03)=1.0094 -> 止损
    val signals = feed(strategy, state, bboEvent(1.02, 1.021, t0 + 32_000L))
    val cover = placedOrders(signals).find(_.side == Side.Long)
    assert(cover.isDefined, "应挂出止损单")
    assertEquals(cover.get.orderType, OrderType.Market)
    assertEquals(cover.get.reduceOnly, true)

  test("平仓后冷却: 立即再暴跌不开新仓"):
    val (strategy, state) = (newStrategy(cooldownMs = 60_000), newState())
    withAccount(state, 10_000.0)
    feed(strategy, state, bboEvent(1.0, 1.001, t0))
    val sells = placedOrders(feed(strategy, state, bboEvent(0.97, 0.971, t0 + 30_000L)))
    val (sell, cid) = registerAll(state, sells).head
    confirm(state, cid, "ex-s", sell, t0 + 30_000L)
    feedFill(strategy, state, Fill(Exchange.Binance, symbol, Side.Short, 0.98, 500.0, t0 + 31_000L))
    // 买回平仓 -> 进入冷却
    feedFill(strategy, state, Fill(Exchange.Binance, symbol, Side.Long, 0.97, 500.0, t0 + 32_000L))
    withAccount(state, 10_005.0, notional = 0.0)
    // 冷却期内继续暴跌 -> 不开新仓
    val signals = feed(strategy, state, bboEvent(0.90, 0.901, t0 + 33_000L))
    assertEquals(placedOrders(signals), Vector.empty)

  test("逐档追价: 价格下移使已确认卖单偏离目标 -> 撤单一次 (不重复撤)"):
    val (strategy, state) = (newStrategy(), newState())
    withAccount(state, 10_000.0)
    feed(strategy, state, bboEvent(1.0, 1.001, t0))
    val sells = placedOrders(feed(strategy, state, bboEvent(0.97, 0.971, t0 + 30_000L)))
    val (sell, cid) = registerAll(state, sells).head
    confirm(state, cid, "ex-s", sell, t0 + 30_000L)
    // 价格继续下跌到 0.95, 卖单目标价随之下移, 旧挂单偏离 -> 撤单
    val first = feed(strategy, state, bboEvent(0.95, 0.951, t0 + 31_000L))
    assertEquals(
      first.collect { case c: OutcomeEvent.CancelOrder => c },
      Vector(OutcomeEvent.CancelOrder(Exchange.Binance, symbol, "ex-s")),
    )
    // 撤单确认到达前不重复撤
    val second = feed(strategy, state, bboEvent(0.949, 0.95, t0 + 31_200L))
    assert(second.collect { case c: OutcomeEvent.CancelOrder => c }.isEmpty)

  test("杠杆触顶: 加空后名义价值超上限的方向不再挂单"):
    val (strategy, state) = (newStrategy(maxLeverage = 3.0), newState())
    // 净值 1000, 已有名义 2800: 加 500 -> 3300/1000=3.3 >= 3 -> 不挂
    withAccount(state, 1_000.0, notional = 2_800.0)
    feed(strategy, state, bboEvent(1.0, 1.001, t0))
    val orders = placedOrders(feed(strategy, state, bboEvent(0.97, 0.971, t0 + 30_000L)))
    assertEquals(orders.filter(_.side == Side.Short), Vector.empty)

  test("止盈单按持仓量重挂: 加空后持仓变大 -> 撤旧平仓单 (qtyDrift)"):
    val (strategy, state) = (newStrategy(), newState())
    withAccount(state, 10_000.0)
    feed(strategy, state, bboEvent(1.0, 1.001, t0))
    val sells = placedOrders(feed(strategy, state, bboEvent(0.97, 0.971, t0 + 30_000L)))
    val (sell, cid) = registerAll(state, sells).head
    confirm(state, cid, "ex-s", sell, t0 + 30_000L)
    feedFill(strategy, state, Fill(Exchange.Binance, symbol, Side.Short, 0.98, 500.0, t0 + 31_000L))
    // 第一次挂出止盈买单 (qty=500)
    val place1 = feed(strategy, state, bboEvent(0.965, 0.966, t0 + 31_200L))
    val cover = placedOrders(place1).find(_.side == Side.Long).get
    val (_, ccid) = registerAll(state, Vector(cover)).head
    confirm(state, ccid, "ex-c", cover, t0 + 31_200L)
    // 再加空一档 (持仓 500 -> 1000), 止盈单数量过期 -> 撤掉重挂
    feedFill(strategy, state, Fill(Exchange.Binance, symbol, Side.Short, 0.96, 500.0, t0 + 31_500L))
    val signals = feed(strategy, state, bboEvent(0.955, 0.956, t0 + 31_700L))
    assertEquals(
      signals.collect { case c: OutcomeEvent.CancelOrder => c },
      Vector(OutcomeEvent.CancelOrder(Exchange.Binance, symbol, "ex-c")),
    )
