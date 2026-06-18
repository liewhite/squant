package hft.strategy

import hft.domain.*
import hft.messaging.{EventData, IncomeEvent, StateManager}

/** 纯 gamma scalping 策略单测：净 delta = 期权 delta(含 cashBal 修正) + 永续仓位，
  * 越带的对冲方向/数量、带内不动作、cashBal 修正、greeks 未就绪不动作。
  */
class GammaScalpStrategySpec extends munit.FunSuite:
  private val ex = Exchange.Binance
  private val sym = "ETHUSDT"
  private val ccy = "ETH"
  private val band = 0.1
  private val offset = 0.0002

  private def strat = GammaScalpStrategy(ex, sym, ccy, deltaBand = band, offsetRatio = offset)

  /** 构造已就绪的状态：cashBal + greeks(原始 delta) + 可选永续仓位 + 当前 BBO */
  private def setup(rawDelta: Double, perpPos: Double, cashBal: Double, bid: Price, ask: Price): (StateManager, IncomeEvent) =
    val sm = StateManager(Iterable(sym), orderTimeoutMs = 5000)
    sm.apply(IncomeEvent(0, 0, EventData.BalanceUpdate(Balance(ex, ccy, cashBal, 0))))
    sm.apply(IncomeEvent(0, 0, EventData.GreeksUpdate(Greeks(ex, ccy, rawDelta, 0.01, -0.5, 1.0, 0))))
    if perpPos != 0.0 then sm.apply(IncomeEvent(0, 0, EventData.PositionUpdate(Position(ex, sym, perpPos, 100.0, 0.0))))
    val bboEv = IncomeEvent(1, 1, EventData.BboUpdate(BBO(ex, sym, bid, 1.0, ask, 1.0, 1)))
    sm.apply(bboEv)
    (sm, bboEv)

  private def placedOrder(out: Vector[OutcomeEvent]): Order = out match
    case Vector(OutcomeEvent.PlaceOrders(orders, _)) => orders.head
    case other => fail(s"expected single PlaceOrders, got $other")

  private def near(a: Double, b: Double): Unit = assert(math.abs(a - b) < 1e-9, s"expected $b got $a")

  test("偏多越带 -> 在 ask 上方挂 PostOnly 卖单, 数量=净 delta"):
    val (sm, ev) = setup(rawDelta = 0.5, perpPos = 0.0, cashBal = 0.0, bid = 99.9, ask = 100.1)
    val o = placedOrder(strat.onEvent(ev, sm))
    assertEquals(o.side, Side.Short)
    near(o.quantity, 0.5)
    o.orderType match
      case OrderType.Limit(px, tif) =>
        assertEquals(tif, TimeInForce.PostOnly)
        near(px, 100.1 * (1 + offset))
      case x => fail(s"expected Limit, got $x")

  test("偏空越带 -> 在 bid 下方挂 PostOnly 买单"):
    val (sm, ev) = setup(rawDelta = -0.5, perpPos = 0.0, cashBal = 0.0, bid = 99.9, ask = 100.1)
    val o = placedOrder(strat.onEvent(ev, sm))
    assertEquals(o.side, Side.Long)
    near(o.quantity, 0.5)
    o.orderType match
      case OrderType.Limit(px, _) => near(px, 99.9 * (1 - offset))
      case x                      => fail(s"expected Limit, got $x")

  test("带内 -> 不挂单"):
    val (sm, ev) = setup(rawDelta = 0.05, perpPos = 0.0, cashBal = 0.0, bid = 99.9, ask = 100.1)
    assertEquals(strat.onEvent(ev, sm), Vector.empty)

  test("永续仓位抵消期权 delta -> 净 delta 入带, 不挂单"):
    // 期权 delta 0.5, 永续 -0.45 -> 净 0.05 < band
    val (sm, ev) = setup(rawDelta = 0.5, perpPos = -0.45, cashBal = 0.0, bid = 99.9, ask = 100.1)
    assertEquals(strat.onEvent(ev, sm), Vector.empty)

  test("cashBal 修正计入净 delta (现货敞口)"):
    // 原始期权 delta 0, 现货 cashBal 0.2 -> 净 delta 0.2 > band -> 卖
    val (sm, ev) = setup(rawDelta = 0.0, perpPos = 0.0, cashBal = 0.2, bid = 99.9, ask = 100.1)
    val o = placedOrder(strat.onEvent(ev, sm))
    assertEquals(o.side, Side.Short)
    near(o.quantity, 0.2)

  test("greeks 未就绪 (无 cashBal) -> 不动作"):
    val sm = StateManager(Iterable(sym), orderTimeoutMs = 5000)
    sm.apply(IncomeEvent(0, 0, EventData.GreeksUpdate(Greeks(ex, ccy, 0.5, 0.01, -0.5, 1.0, 0))))
    val ev = IncomeEvent(1, 1, EventData.BboUpdate(BBO(ex, sym, 99.9, 1.0, 100.1, 1.0, 1)))
    sm.apply(ev)
    assertEquals(strat.onEvent(ev, sm), Vector.empty) // greeks() 返回 None (缺 cashBal)
