package strategy.strategies.makerhedge.logic
import strategy.utils.hedge.{HedgeBand, HedgeCtx}

import hft.domain.*
import hft.event.{AnyEvent, Event, Topics}
import hft.engine.StrategyRunner
import hft.event.Commands.{OrderIntent, OutcomeEvent}
import hft.state.{StateManager}
import hft.TestUnits.given

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

  private def bbo(px: Price, ts: Timestamp) = Event.stamped(Topics.Bbo, BBO(ex, sym, px, 1.0, px, 1.0, ts), ts, ts)
  private def ordUpd(status: OrderStatus, side: Side, px: Price, ts: Timestamp, oid: OrderId = "o1") =
    Event.stamped(Topics.OrderUpdate, OrderUpdate(AccountId.Live, oid, Some("c1"), ex, sym, side, status, px, 0.5, 0.0, 0.0, ts), ts, ts)
  /** 经 StrategyRunner 驱动策略 —— 它是策略的唯一入口 (负责绑定账户、生成 id、登记 pending)。
    * 产出的是事件流, 从中取回下单意图。 */
  private def feed(runner: StrategyRunner, ev: AnyEvent): Vector[OutcomeEvent] =
    runner.onEvent(ev, ev.localTs).flatMap(_.as(OrderIntent)).map(_.outcome)

  private val metas = Map((ex, sym) -> SymbolMeta(ex, sym, 0.01, 0.0001, 0.0001, 1.0))

  private def warm(band: HedgeBand, rawDelta: Double): StrategyRunner =
    val runner = StrategyRunner(strat(band), AccountId.Live)
    feed(runner, Event.stamped(Topics.Balance, Balance(AccountId.Live, ex, ccy, 0.0, 0), 0, 0))
    feed(runner, Event.stamped(Topics.Greeks, Greeks(AccountId.Live, ex, ccy, rawDelta, 0.01, -0.5, 1.0, 0), 0, 0))
    (0 to 7).foreach(i => feed(runner, bbo(if i % 2 == 0 then 100.0 else 101.0, i.toLong * hour)))
    runner

  private def placed(out: Vector[OutcomeEvent]): Order = out match
    case Vector(OutcomeEvent.PlaceOrders(os, _)) => os.head
    case other                                   => fail(s"expected PlaceOrders, got $other")

  test("越带 -> 挂 PostOnly 限价单, 净多则卖、被动价高 offset"):
    val runner = warm(ConstantBand(2.0, 2.0), rawDelta = 0.5)
    val o = placed(feed(runner, bbo(104.0, 8 * hour))) // dev +4 > 2, 净多 -> 卖
    assertEquals(o.side, Side.Short)
    o.orderType match
      case OrderType.Limit(px, tif) =>
        assertEquals(tif, TimeInForce.PostOnly)
        assert(math.abs(px.value - 104.0 * 1.01) < 1e-6, s"limit px=$px") // 卖挂高 1%
      case other => fail(s"expected Limit, got $other")

  test("下单到确认之间不重复下单 (awaitingAck)"):
    val runner = warm(ConstantBand(2.0, 2.0), rawDelta = 0.5)
    assert(feed(runner, bbo(104.0, 8 * hour)).nonEmpty)        // 下单
    assertEquals(feed(runner, bbo(105.0, 8 * hour + 1)), Vector.empty) // 未收确认 -> 不再下

  test("收 Pending 后 5s 内不重挂, 超 5s 撤单"):
    val runner = warm(ConstantBand(2.0, 2.0), rawDelta = 0.5)
    feed(runner, bbo(104.0, 8 * hour))                              // 下单
    feed(runner, ordUpd(OrderStatus.Pending, Side.Short, 105.04, 8 * hour)) // 确认, restingAt=8h
    assertEquals(feed(runner, bbo(104.0, 8 * hour + 2000)), Vector.empty)   // 2s < 5s
    feed(runner, bbo(104.0, 8 * hour + 6000)) match                          // 6s > 5s -> 撤
      case Vector(OutcomeEvent.CancelOrder(e, sy, ref)) => assertEquals(ref, OrderRef.ByExchangeId("o1"))
      case other                                       => fail(s"expected CancelOrder, got $other")

  test("成交回报 -> 中心重置到成交价, 同价不再下单"):
    val runner = warm(ConstantBand(2.0, 2.0), rawDelta = 0.5)
    feed(runner, bbo(104.0, 8 * hour))
    feed(runner, ordUpd(OrderStatus.Pending, Side.Short, 105.04, 8 * hour))
    feed(runner, ordUpd(OrderStatus.Filled, Side.Short, 105.04, 8 * hour + 100)) // 成交 -> center=105.04
    assertEquals(feed(runner, bbo(105.04, 9 * hour)), Vector.empty) // dev=0 -> 不下单

  test("对冲量超 maxHedgeQty 硬上限 -> 不下单 (防 delta/gamma bug)"):
    val runner = StrategyRunner(MakerHedgeStrategy(ex, sym, ccy, ConstantBand(2.0, 2.0), offsetPct = 0.01, requoteMs = 5000,
      atrPeriodBars = 5, rvShortWindowBars = 3, rvLongWindowBars = 6, maSmaPeriod = 5, maxHedgeQty = 0.3, barIntervalMs = hour, minHedgeQty = 0.001), AccountId.Live)
    feed(runner, Event.stamped(Topics.Balance, Balance(AccountId.Live, ex, ccy, 0.0, 0), 0, 0))
    feed(runner, Event.stamped(Topics.Greeks, Greeks(AccountId.Live, ex, ccy, 0.5, 0.01, -0.5, 1.0, 0), 0, 0))
    (0 to 7).foreach(i => feed(runner, bbo(if i % 2 == 0 then 100.0 else 101.0, i.toLong * hour)))
    assertEquals(feed(runner, bbo(104.0, 8 * hour)), Vector.empty) // netDelta 0.5 > maxHedge 0.3 -> 不下

  test("无 ccy 余额 -> greeks()=None -> 不对冲 (实盘由 OptionGreeksStream 同步兜底余额)"):
    val runner = StrategyRunner(strat(ConstantBand(2.0, 2.0)), AccountId.Live)
    // 只 greeks, 无 Balance
    feed(runner, Event.stamped(Topics.Greeks, Greeks(AccountId.Live, ex, ccy, 0.5, 0.01, -0.5, 1.0, 0), 0, 0))
    (0 to 7).foreach(i => feed(runner, bbo(if i % 2 == 0 then 100.0 else 101.0, i.toLong * hour)))
    assertEquals(feed(runner, bbo(104.0, 8 * hour)), Vector.empty) // 越带但 greeks()=None -> 不挂

  test("greeks 陈旧超 maxGreeksStaleMs -> 暂停对冲"):
    val runner = StrategyRunner(MakerHedgeStrategy(ex, sym, ccy, ConstantBand(2.0, 2.0), offsetPct = 0.01, requoteMs = 5000,
      atrPeriodBars = 5, rvShortWindowBars = 3, rvLongWindowBars = 6, maSmaPeriod = 5, maxGreeksStaleMs = 1000, barIntervalMs = hour, minHedgeQty = 0.001), AccountId.Live)
    feed(runner, Event.stamped(Topics.Balance, Balance(AccountId.Live, ex, ccy, 0.0, 0), 0, 0))
    feed(runner, Event.stamped(Topics.Greeks, Greeks(AccountId.Live, ex, ccy, 0.5, 0.01, -0.5, 1.0, 0), 0, 0)) // ts=0 (旧)
    (0 to 7).foreach(i => feed(runner, bbo(if i % 2 == 0 then 100.0 else 101.0, i.toLong * hour)))
    assertEquals(feed(runner, bbo(104.0, 8 * hour)), Vector.empty) // now=8h, greeks ts=0 -> 陈旧 -> 暂停

  test("gammaAdjust: 两次 greeks 间用 gamma×价差修正净 delta"):
    // greeks: delta=0.0, gamma=0.1; greeksRefMid=100 (greeks 更新时); 现价 104 -> 修正 delta = 0.1×(104-100)=0.4
    val runner = StrategyRunner(MakerHedgeStrategy(ex, sym, ccy, ConstantBand(2.0, 2.0), offsetPct = 0.01, requoteMs = 5000,
        atrPeriodBars = 5, rvShortWindowBars = 3, rvLongWindowBars = 6, maSmaPeriod = 5, gammaAdjust = true,
        barIntervalMs = hour, minHedgeQty = 0.001),
      AccountId.Live,
    )
    feed(runner, Event.stamped(Topics.Balance, Balance(AccountId.Live, ex, ccy, 0.0, 0), 0, 0))
    feed(runner, Event.stamped(Topics.Greeks, Greeks(AccountId.Live, ex, ccy, 0.0, 0.1, -0.5, 1.0, 0), 0, 0)) // delta0=0, gamma=0.1
    (0 to 7).foreach(i => feed(runner, bbo(if i % 2 == 0 then 100.0 else 101.0, i.toLong * hour)))
    feed(runner, Event.stamped(Topics.Greeks, Greeks(AccountId.Live, ex, ccy, 0.0, 0.1, -0.5, 1.0, 8 * hour), 8 * hour, 8 * hour)) // 设 greeksRefMid≈101
    // 现价 104: gammaAdj=0.1×(104-101)=0.3, 净delta≈0.3 -> 卖 0.3
    val q = placed(feed(runner, bbo(104.0, 8 * hour + 1))).quantity
    assert(math.abs(q.value - 0.1 * (104.0 - 101.0)) < 1e-9, s"qty=${q.value}")

  test("requote 判据用交易所时钟, 不受投递延迟影响"):
    // restingAt 取自订单回报的**交易所**时间戳; 若 requote 拿本地处理时刻去比,
    // 两个时钟域一混, 撤单时机就随投递延迟与时钟偏斜漂移。
    // 这里让 localTs 比 exchangeTs 晚 6 秒 (模拟延迟), 而交易所时钟只过了 2 秒:
    // 正确行为是**不撤**。此前的测试构造 localTs == exchangeTs, 恰好掩盖了这个区别。
    val runner = warm(ConstantBand(2.0, 2.0), rawDelta = 0.5)
    feed(runner, bbo(104.0, 8 * hour))                                      // 下单
    feed(runner, ordUpd(OrderStatus.Pending, Side.Short, 105.04, 8 * hour))  // 确认, restingAt = 8h
    val exTs = 8 * hour + 2000  // 交易所时钟只过了 2s (< requoteMs 5s)
    val lateDelivery = Event.stamped(Topics.Bbo, BBO(ex, sym, 104.0, Coin(1.0), 104.0, Coin(1.0), exTs), exTs, exTs + 6000)
    assertEquals(
      runner.onEvent(lateDelivery, exTs + 6000).flatMap(_.as(OrderIntent)).map(_.outcome),
      Vector.empty,
      "交易所时钟只过了 2s, 不该 requote —— 拿本地时刻 (已过 6s) 比就会误撤",
    )
