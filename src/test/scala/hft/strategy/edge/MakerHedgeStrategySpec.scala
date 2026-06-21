package hft.strategy.edge

import hft.domain.*
import hft.messaging.{EventData, IncomeEvent, StateManager}
import hft.strategy.OutcomeEvent

/** MakerHedgeStrategy 机制单测：越带挂被动 PostOnly 限价 (价/向正确)、下单到确认间不重复、5s 未成交撤单重挂、成交后中心重置。 */
class MakerHedgeStrategySpec extends munit.FunSuite:
  private val ex = Exchange.Binance
  private val sym = "ETHUSDT"
  private val ccy = "ETH"
  private val hour = 3_600_000L

  private final class ConstantBand(up: Double, down: Double) extends HedgeBand:
    def bands(ctx: HedgeCtx): (Double, Double) = (up, down)

  private def strat(band: HedgeBand, offset: Double = 0.01, requote: Long = 5000): MakerHedgeStrategy =
    MakerHedgeStrategy(ex, sym, ccy, band, offsetPct = offset, requoteMs = requote,
      atrPeriodBars = 5, rvShortWindowBars = 3, rvLongWindowBars = 6, maSmaPeriod = 5, barIntervalMs = hour, minHedgeQty = 0.001)

  private def bbo(px: Price, ts: Timestamp) = IncomeEvent(ts, ts, EventData.BboUpdate(BBO(ex, sym, px, 1.0, px, 1.0, ts)))
  private def ordUpd(status: OrderStatus, side: Side, px: Price, ts: Timestamp, oid: OrderId = "o1") =
    IncomeEvent(ts, ts, EventData.OrderUpdated(OrderUpdate(oid, Some("c1"), ex, sym, side, status, px, 0.5, 0.0, 0.0, ts)))
  private def feed(sm: StateManager, s: MakerHedgeStrategy, ev: IncomeEvent) = { sm.apply(ev); s.onEvent(ev, sm) }

  private def warm(band: HedgeBand, rawDelta: Double): (StateManager, MakerHedgeStrategy) =
    val sm = StateManager(Iterable(sym), orderTimeoutMs = 60000)
    sm.apply(IncomeEvent(0, 0, EventData.BalanceUpdate(Balance(ex, ccy, 0.0, 0))))
    sm.apply(IncomeEvent(0, 0, EventData.GreeksUpdate(Greeks(ex, ccy, rawDelta, 0.01, -0.5, 1.0, 0))))
    val s = strat(band)
    (0 to 7).foreach(i => feed(sm, s, bbo(if i % 2 == 0 then 100.0 else 101.0, i.toLong * hour)))
    (sm, s)

  private def placed(out: Vector[OutcomeEvent]): Order = out match
    case Vector(OutcomeEvent.PlaceOrders(os, _)) => os.head
    case other                                   => fail(s"expected PlaceOrders, got $other")

  test("越带 -> 挂 PostOnly 限价单, 净多则卖、被动价高 offset"):
    val (sm, s) = warm(ConstantBand(2.0, 2.0), rawDelta = 0.5)
    val o = placed(feed(sm, s, bbo(104.0, 8 * hour))) // dev +4 > 2, 净多 -> 卖
    assertEquals(o.side, Side.Short)
    o.orderType match
      case OrderType.Limit(px, tif) =>
        assertEquals(tif, TimeInForce.PostOnly)
        assert(math.abs(px - 104.0 * 1.01) < 1e-6, s"limit px=$px") // 卖挂高 1%
      case other => fail(s"expected Limit, got $other")

  test("下单到确认之间不重复下单 (awaitingAck)"):
    val (sm, s) = warm(ConstantBand(2.0, 2.0), rawDelta = 0.5)
    assert(feed(sm, s, bbo(104.0, 8 * hour)).nonEmpty)        // 下单
    assertEquals(feed(sm, s, bbo(105.0, 8 * hour + 1)), Vector.empty) // 未收确认 -> 不再下

  test("收 Pending 后 5s 内不重挂, 超 5s 撤单"):
    val (sm, s) = warm(ConstantBand(2.0, 2.0), rawDelta = 0.5)
    feed(sm, s, bbo(104.0, 8 * hour))                              // 下单
    feed(sm, s, ordUpd(OrderStatus.Pending, Side.Short, 105.04, 8 * hour)) // 确认, restingAt=8h
    assertEquals(feed(sm, s, bbo(104.0, 8 * hour + 2000)), Vector.empty)   // 2s < 5s
    feed(sm, s, bbo(104.0, 8 * hour + 6000)) match                          // 6s > 5s -> 撤
      case Vector(OutcomeEvent.CancelOrder(e, sy, id)) => assertEquals(id, "o1")
      case other                                       => fail(s"expected CancelOrder, got $other")

  test("成交回报 -> 中心重置到成交价, 同价不再下单"):
    val (sm, s) = warm(ConstantBand(2.0, 2.0), rawDelta = 0.5)
    feed(sm, s, bbo(104.0, 8 * hour))
    feed(sm, s, ordUpd(OrderStatus.Pending, Side.Short, 105.04, 8 * hour))
    feed(sm, s, ordUpd(OrderStatus.Filled, Side.Short, 105.04, 8 * hour + 100)) // 成交 -> center=105.04
    assertEquals(feed(sm, s, bbo(105.04, 9 * hour)), Vector.empty) // dev=0 -> 不下单
