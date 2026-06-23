package strategy.strategies.atrtakehedge.logic

import hft.strategy.{OutcomeEvent, Strategy}

import hft.domain.*
import hft.messaging.{EventData, IncomeEvent, StateManager}

/** ATR 通道 take 式买方对冲单测：价格主导触发 (越 atrMult×ATR)、delta 定量 (take 当前净 delta)、
  * 市价成交、成交后中心重置、ATR/greeks 未就绪不动作、永续抵消则不动作。
  */
class AtrTakeHedgeStrategySpec extends munit.FunSuite:
  private val ex = Exchange.Binance
  private val sym = "ETHUSDT"
  private val ccy = "ETH"
  private val hour = 3_600_000L

  private def strat = AtrTakeHedgeStrategy(ex, sym, ccy, atrMult = 2.0, atrPeriodBars = 5, barIntervalMs = hour, minHedgeQty = 0.001)

  private def bboEv(px: Price, ts: Timestamp): IncomeEvent =
    IncomeEvent(ts, ts, EventData.BboUpdate(BBO(ex, sym, px, 1.0, px, 1.0, ts))) // 零价差: mid = px

  private def feed(sm: StateManager, s: AtrTakeHedgeStrategy, ev: IncomeEvent): Vector[OutcomeEvent] =
    sm.apply(ev); s.onEvent(ev, sm) // 复刻 StrategyRunner: 先更状态再跑策略

  /** 就绪状态: greeks+cashBal+可选永续仓 + ATR 预热 (8 根小时柱在 100/101 间, center=100, ATR≈1, 不越带)。 */
  private def warmReady(rawDelta: Double, perpPos: Double = 0.0): (StateManager, AtrTakeHedgeStrategy) =
    val sm = StateManager(Iterable(sym), orderTimeoutMs = 5000)
    sm.apply(IncomeEvent(0, 0, EventData.BalanceUpdate(Balance(ex, ccy, 0.0, 0))))
    sm.apply(IncomeEvent(0, 0, EventData.GreeksUpdate(Greeks(ex, ccy, rawDelta, 0.01, -0.5, 1.0, 0))))
    if perpPos != 0.0 then sm.apply(IncomeEvent(0, 0, EventData.PositionUpdate(Position(ex, sym, perpPos, 100.0, 0.0))))
    val s = strat
    (0 to 7).foreach(i => feed(sm, s, bboEv(if i % 2 == 0 then 100.0 else 101.0, i.toLong * hour)))
    (sm, s)

  private def placed(out: Vector[OutcomeEvent]): Order = out match
    case Vector(OutcomeEvent.PlaceOrders(orders, _)) => orders.head
    case other                                       => fail(s"expected single PlaceOrders, got $other")

  private def near(a: Double, b: Double): Unit = assert(math.abs(a - b) < 1e-9, s"expected $b got $a")

  test("上行越 2×ATR -> 市价卖 take 净 delta (净多)"):
    val (sm, s) = warmReady(rawDelta = 0.5)
    val o = placed(feed(sm, s, bboEv(104.0, 8 * hour))) // dev=4 > 2×ATR≈2
    assertEquals(o.side, Side.Short)
    assertEquals(o.orderType, OrderType.Market)
    near(o.quantity, 0.5)

  test("下行越 2×ATR -> 市价买 take 净 delta (净空)"):
    val (sm, s) = warmReady(rawDelta = -0.5)
    val o = placed(feed(sm, s, bboEv(96.0, 8 * hour))) // dev=4 向下
    assertEquals(o.side, Side.Long)
    near(o.quantity, 0.5)

  test("delta 定量: 净 delta 越大对冲量越大"):
    val (sm, s) = warmReady(rawDelta = 2.0)
    near(placed(feed(sm, s, bboEv(104.0, 8 * hour))).quantity, 2.0)

  test("未越带 (<2×ATR) -> 不动作"):
    val (sm, s) = warmReady(rawDelta = 0.5)
    assertEquals(feed(sm, s, bboEv(101.5, 8 * hour)), Vector.empty) // dev=1.5 < 2

  test("成交后中心重置到成交价 -> 同价不再触发"):
    val (sm, s) = warmReady(rawDelta = 0.5)
    assert(feed(sm, s, bboEv(104.0, 8 * hour)).nonEmpty) // 触发, center -> 104
    assertEquals(feed(sm, s, bboEv(104.0, 9 * hour)), Vector.empty) // dev=0

  test("永续仓位抵消期权 delta -> 净 delta≈0, 即便越带也不动作"):
    val (sm, s) = warmReady(rawDelta = 0.5, perpPos = -0.5)
    assertEquals(feed(sm, s, bboEv(104.0, 8 * hour)), Vector.empty)

  test("ATR 未预热 -> 不动作"):
    val sm = StateManager(Iterable(sym), orderTimeoutMs = 5000)
    sm.apply(IncomeEvent(0, 0, EventData.BalanceUpdate(Balance(ex, ccy, 0.0, 0))))
    sm.apply(IncomeEvent(0, 0, EventData.GreeksUpdate(Greeks(ex, ccy, 0.5, 0.01, -0.5, 1.0, 0))))
    val s = strat
    feed(sm, s, bboEv(100.0, 0)) // 仅 1 根, ATR=None
    assertEquals(feed(sm, s, bboEv(200.0, hour)), Vector.empty) // 大幅越价但 ATR 未就绪

  test("greeks 未就绪 (无 cashBal) -> 不动作"):
    val sm = StateManager(Iterable(sym), orderTimeoutMs = 5000)
    sm.apply(IncomeEvent(0, 0, EventData.GreeksUpdate(Greeks(ex, ccy, 0.5, 0.01, -0.5, 1.0, 0))))
    val s = strat
    (0 to 7).foreach(i => feed(sm, s, bboEv(if i % 2 == 0 then 100.0 else 101.0, i.toLong * hour)))
    assertEquals(feed(sm, s, bboEv(104.0, 8 * hour)), Vector.empty) // greeks() = None (缺 cashBal)
