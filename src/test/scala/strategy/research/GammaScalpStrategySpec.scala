package strategy.research

import hft.strategy.{OutcomeEvent, Strategy}

import hft.domain.*
import hft.messaging.{EventData, IncomeEvent, StateManager}

/** gamma scalping 策略单测 (trade-native)：净 delta = 期权 delta(含 cashBal 修正) + 永续仓位，
  * 以最新成交价为基准的对冲方向/数量/间距、带内不动作、cashBal 修正、greeks 未就绪不动作。
  */
class GammaScalpStrategySpec extends munit.FunSuite:
  private val ex = Exchange.Binance
  private val sym = "ETHUSDT"
  private val ccy = "ETH"
  private val band = 0.1
  private val offset = 0.002 // base 对冲间距；MACD 未就绪 (仅 1 笔 trade) -> dir=0 -> 对称

  private def strat = GammaScalpStrategy(ex, sym, ccy, deltaBand = band, baseOffsetRatio = offset, dirSkewRatio = 0.0005)

  /** 构造已就绪状态：cashBal + greeks + 可选永续仓位 + 一笔触发用 trade (设最新成交价) */
  private def setup(rawDelta: Double, perpPos: Double, cashBal: Double, tradePrice: Price): (StateManager, IncomeEvent) =
    val sm = StateManager(Iterable(sym), orderTimeoutMs = 5000)
    sm.apply(IncomeEvent(0, 0, EventData.BalanceUpdate(Balance(ex, ccy, cashBal, 0))))
    sm.apply(IncomeEvent(0, 0, EventData.GreeksUpdate(Greeks(ex, ccy, rawDelta, 0.01, -0.5, 1.0, 0))))
    if perpPos != 0.0 then sm.apply(IncomeEvent(0, 0, EventData.PositionUpdate(Position(ex, sym, perpPos, 100.0, 0.0))))
    val tradeEv = IncomeEvent(1, 1, EventData.MarketTradeUpdate(MarketTrade(ex, sym, tradePrice, 1.0, isBuyerMaker = false, 1)))
    sm.apply(tradeEv)
    (sm, tradeEv)

  private def placedOrder(out: Vector[OutcomeEvent]): Order = out match
    case Vector(OutcomeEvent.PlaceOrders(orders, _)) => orders.head
    case other => fail(s"expected single PlaceOrders, got $other")

  private def near(a: Double, b: Double): Unit = assert(math.abs(a - b) < 1e-9, s"expected $b got $a")

  test("偏多越带 -> 最新价上方挂 PostOnly 卖单, 数量=净 delta, 间距=base"):
    val (sm, ev) = setup(rawDelta = 0.5, perpPos = 0.0, cashBal = 0.0, tradePrice = 100.0)
    val o = placedOrder(strat.onEvent(ev, sm))
    assertEquals(o.side, Side.Short)
    near(o.quantity, 0.5)
    o.orderType match
      case OrderType.Limit(px, tif) =>
        assertEquals(tif, TimeInForce.PostOnly)
        near(px, 100.0 * (1 + offset))
      case x => fail(s"expected Limit, got $x")

  test("偏空越带 -> 最新价下方挂 PostOnly 买单"):
    val (sm, ev) = setup(rawDelta = -0.5, perpPos = 0.0, cashBal = 0.0, tradePrice = 100.0)
    val o = placedOrder(strat.onEvent(ev, sm))
    assertEquals(o.side, Side.Long)
    near(o.quantity, 0.5)
    o.orderType match
      case OrderType.Limit(px, _) => near(px, 100.0 * (1 - offset))
      case x                      => fail(s"expected Limit, got $x")

  test("带内 -> 不挂单"):
    val (sm, ev) = setup(rawDelta = 0.05, perpPos = 0.0, cashBal = 0.0, tradePrice = 100.0)
    assertEquals(strat.onEvent(ev, sm), Vector.empty)

  test("永续仓位抵消期权 delta -> 净 delta 入带, 不挂单"):
    val (sm, ev) = setup(rawDelta = 0.5, perpPos = -0.45, cashBal = 0.0, tradePrice = 100.0)
    assertEquals(strat.onEvent(ev, sm), Vector.empty)

  test("cashBal 修正计入净 delta (现货敞口)"):
    val (sm, ev) = setup(rawDelta = 0.0, perpPos = 0.0, cashBal = 0.2, tradePrice = 100.0)
    val o = placedOrder(strat.onEvent(ev, sm))
    assertEquals(o.side, Side.Short)
    near(o.quantity, 0.2)

  test("greeks 未就绪 (无 cashBal) -> 不动作"):
    val sm = StateManager(Iterable(sym), orderTimeoutMs = 5000)
    sm.apply(IncomeEvent(0, 0, EventData.GreeksUpdate(Greeks(ex, ccy, 0.5, 0.01, -0.5, 1.0, 0))))
    val ev = IncomeEvent(1, 1, EventData.MarketTradeUpdate(MarketTrade(ex, sym, 100.0, 1.0, isBuyerMaker = false, 1)))
    sm.apply(ev)
    assertEquals(strat.onEvent(ev, sm), Vector.empty) // greeks() 返回 None (缺 cashBal)
