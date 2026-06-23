package strategy.bandhedge.logic

import strategy.utils.hedge.{HedgeBand, HedgeCtx}

import hft.domain.*
import hft.messaging.{EventData, IncomeEvent, StateManager}
import hft.strategy.OutcomeEvent

/** BandHedgeStrategy 机制单测：用恒定带宽 stub 隔离指标预热，验证上/下越带触发 (含**不对称**)、
  * delta 定量 take、成交后中心重置、净 delta 抵消/未越带/指标未就绪不动作。带宽策略本身的数学
  * 见 [[HedgeBandSpec]]。 */
class BandHedgeStrategySpec extends munit.FunSuite:
  private val ex = Exchange.Binance
  private val sym = "ETHUSDT"
  private val ccy = "ETH"
  private val hour = 3_600_000L

  /** 恒定带宽 (忽略 ctx)：把策略的越带判定与指标信号解耦，专测机制。 */
  private final class ConstantBand(up: Double, down: Double) extends HedgeBand:
    def bands(ctx: HedgeCtx): (Double, Double) = (up, down)

  /** 记录最近收到的 ctx 且带宽极大永不触发：用于断言策略喂给带的信号 (如 maBias) 接线正确。 */
  private final class CapturingBand extends HedgeBand:
    var last: HedgeCtx = null
    def bands(ctx: HedgeCtx): (Double, Double) = { last = ctx; (1e9, 1e9) }

  private def strat(band: HedgeBand): BandHedgeStrategy =
    BandHedgeStrategy(ex, sym, ccy, band, atrPeriodBars = 5, rvShortWindowBars = 3, rvLongWindowBars = 6,
      barIntervalMs = hour, minHedgeQty = 0.001)

  private def bboEv(px: Price, ts: Timestamp): IncomeEvent =
    IncomeEvent(ts, ts, EventData.BboUpdate(BBO(ex, sym, px, 1.0, px, 1.0, ts))) // 零价差: mid=px

  private def feed(sm: StateManager, s: BandHedgeStrategy, ev: IncomeEvent): Vector[OutcomeEvent] =
    sm.apply(ev); s.onEvent(ev, sm)

  /** 就绪: greeks+cashBal(+可选永续仓) + ATR 预热 (8 根小时柱 100/101, center=100, ATR>0)。 */
  private def warmReady(band: HedgeBand, rawDelta: Double, perpPos: Double = 0.0): (StateManager, BandHedgeStrategy) =
    val sm = StateManager(Iterable(sym), orderTimeoutMs = 5000)
    sm.apply(IncomeEvent(0, 0, EventData.BalanceUpdate(Balance(ex, ccy, 0.0, 0))))
    sm.apply(IncomeEvent(0, 0, EventData.GreeksUpdate(Greeks(ex, ccy, rawDelta, 0.01, -0.5, 1.0, 0))))
    if perpPos != 0.0 then sm.apply(IncomeEvent(0, 0, EventData.PositionUpdate(Position(ex, sym, perpPos, 100.0, 0.0))))
    val s = strat(band)
    (0 to 7).foreach(i => feed(sm, s, bboEv(if i % 2 == 0 then 100.0 else 101.0, i.toLong * hour)))
    (sm, s)

  private def placed(out: Vector[OutcomeEvent]): Order = out match
    case Vector(OutcomeEvent.PlaceOrders(orders, _)) => orders.head
    case other                                       => fail(s"expected single PlaceOrders, got $other")

  private def near(a: Double, b: Double): Unit = assert(math.abs(a - b) < 1e-9, s"expected $b got $a")

  test("对称带 上行越带 -> 市价卖 take 净 delta (净多)"):
    val (sm, s) = warmReady(ConstantBand(2.0, 2.0), rawDelta = 0.5)
    val o = placed(feed(sm, s, bboEv(104.0, 8 * hour))) // dev +4 > 2
    assertEquals(o.side, Side.Short)
    assertEquals(o.orderType, OrderType.Market)
    near(o.quantity, 0.5)

  test("对称带 下行越带 -> 市价买 (净空)"):
    val (sm, s) = warmReady(ConstantBand(2.0, 2.0), rawDelta = -0.5)
    val o = placed(feed(sm, s, bboEv(96.0, 8 * hour))) // dev -4
    assertEquals(o.side, Side.Long)
    near(o.quantity, 0.5)

  test("不对称带 上带更宽 -> 上行需走更远才触发"):
    val band = ConstantBand(up = 3.0, down = 1.0)
    val (sm1, s1) = warmReady(band, rawDelta = 0.5)
    assertEquals(feed(sm1, s1, bboEv(102.0, 8 * hour)), Vector.empty) // dev +2 < up 3 -> 不触发
    val (sm2, s2) = warmReady(band, rawDelta = 0.5)
    assertEquals(placed(feed(sm2, s2, bboEv(104.0, 8 * hour))).side, Side.Short) // dev +4 > 3

  test("不对称带 下带更窄 -> 下行更易触发"):
    val band = ConstantBand(up = 3.0, down = 1.0)
    val (sm, s) = warmReady(band, rawDelta = -0.5)
    val o = placed(feed(sm, s, bboEv(98.5, 8 * hour))) // center-px=1.5 > down 1 (对称 2 则不会触发)
    assertEquals(o.side, Side.Long)
    near(o.quantity, 0.5)

  test("delta 定量: 净 delta 越大对冲量越大"):
    val (sm, s) = warmReady(ConstantBand(2.0, 2.0), rawDelta = 2.0)
    near(placed(feed(sm, s, bboEv(104.0, 8 * hour))).quantity, 2.0)

  test("未越带 -> 不动作"):
    val (sm, s) = warmReady(ConstantBand(2.0, 2.0), rawDelta = 0.5)
    assertEquals(feed(sm, s, bboEv(101.5, 8 * hour)), Vector.empty) // dev 1.5 < 2

  test("成交后中心重置到成交价 -> 同价不再触发"):
    val (sm, s) = warmReady(ConstantBand(2.0, 2.0), rawDelta = 0.5)
    assert(feed(sm, s, bboEv(104.0, 8 * hour)).nonEmpty) // center -> 104
    assertEquals(feed(sm, s, bboEv(104.0, 9 * hour)), Vector.empty)

  test("永续仓位抵消净 delta≈0 -> 即便越带也不动作"):
    val (sm, s) = warmReady(ConstantBand(2.0, 2.0), rawDelta = 0.5, perpPos = -0.5)
    assertEquals(feed(sm, s, bboEv(104.0, 8 * hour)), Vector.empty)

  test("ATR 未预热 -> 不动作"):
    val sm = StateManager(Iterable(sym), orderTimeoutMs = 5000)
    sm.apply(IncomeEvent(0, 0, EventData.BalanceUpdate(Balance(ex, ccy, 0.0, 0))))
    sm.apply(IncomeEvent(0, 0, EventData.GreeksUpdate(Greeks(ex, ccy, 0.5, 0.01, -0.5, 1.0, 0))))
    val s = strat(ConstantBand(2.0, 2.0))
    feed(sm, s, bboEv(100.0, 0)) // 仅 1 根
    assertEquals(feed(sm, s, bboEv(200.0, hour)), Vector.empty)

  test("maBias 接线: 价在均线上 -> +1, 均线下 -> -1"):
    val cap = CapturingBand()
    val sm = StateManager(Iterable(sym), orderTimeoutMs = 5000)
    sm.apply(IncomeEvent(0, 0, EventData.BalanceUpdate(Balance(ex, ccy, 0.0, 0))))
    sm.apply(IncomeEvent(0, 0, EventData.GreeksUpdate(Greeks(ex, ccy, 0.5, 0.01, -0.5, 1.0, 0))))
    val s = BandHedgeStrategy(ex, sym, ccy, cap, atrPeriodBars = 5, rvShortWindowBars = 3, rvLongWindowBars = 6,
      maSmaPeriod = 5, barIntervalMs = hour, minHedgeQty = 0.001)
    (0 to 7).foreach(i => feed(sm, s, bboEv(100.0 + i, i.toLong * hour))) // 递增收盘 -> SMA 落后于现价
    feed(sm, s, bboEv(200.0, 8 * hour)) // 远高于 SMA
    assertEquals(cap.last.maBias, 1)
    feed(sm, s, bboEv(50.0, 9 * hour)) // 远低于 SMA
    assertEquals(cap.last.maBias, -1)

  test("greeks 未就绪 (无 cashBal) -> 不动作"):
    val sm = StateManager(Iterable(sym), orderTimeoutMs = 5000)
    sm.apply(IncomeEvent(0, 0, EventData.GreeksUpdate(Greeks(ex, ccy, 0.5, 0.01, -0.5, 1.0, 0))))
    val s = strat(ConstantBand(2.0, 2.0))
    (0 to 7).foreach(i => feed(sm, s, bboEv(if i % 2 == 0 then 100.0 else 101.0, i.toLong * hour)))
    assertEquals(feed(sm, s, bboEv(104.0, 8 * hour)), Vector.empty)
