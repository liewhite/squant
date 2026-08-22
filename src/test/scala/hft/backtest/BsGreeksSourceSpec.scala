package hft.backtest

import hft.domain.*
import hft.event.{AnyEvent, Event, Topics}
import hft.option.{BlackScholes, OptionRight}
import hft.state.{StateManager}

/** BS 合成 Greeks 源 (单只 ATM 跨式，持有到期，不滚动) 单测：聚合、发射间隔门控、cashBal 一次、
  * StateManager delta 修正、期权腿 P&L、临近到期 gamma 钳制。 */
class BsGreeksSourceSpec extends munit.FunSuite:
  private val ex = Exchange.Okx
  private val sym = "BTCUSDT"
  private val ccy = "BTC"
  private val day = 86_400_000L

  private class FixedSource(evs: Vector[AnyEvent]) extends MarketDataSource:
    def events(): Iterator[AnyEvent] = evs.iterator

  private def trade(price: Price, ts: Timestamp): AnyEvent =
    Event.stamped(Topics.Trade, MarketTrade(ex, sym, price, 1.0, isBuyerMaker = false, ts), ts, ts)

  // 到期设在 30 天后 (单只持有, 不滚动)
  private def config(straddles: Double, spot: Double, intervalMs: Long = 1000) =
    BsGreeksConfig(ex, ccy, sym, straddles = straddles, impliedVol = 0.2, expiry = 30 * day,
      riskFreeRate = 0.0, spotHolding = spot, emitIntervalMs = intervalMs)

  private def near(a: Double, b: Double, eps: Double = 1e-6): Unit = assert(math.abs(a - b) < eps, s"$a vs $b")

  test("聚合: 账户级 delta/gamma = 份数 × (call+put) 单份 BS, ATM=首笔成交价"):
    val src = BsGreeksSource(FixedSource(Vector(trade(100.0, 0))), config(straddles = 10.0, spot = 0.0))
    val g = src.events().flatMap(_.as(Topics.Greeks)).toVector
    assertEquals(g.size, 1)
    val tY = 30.0 / 365.0
    val call = BlackScholes.greeks(OptionRight.Call, 100.0, 100.0, tY, 0.2, 0.0)
    val put = BlackScholes.greeks(OptionRight.Put, 100.0, 100.0, tY, 0.2, 0.0)
    near(g.head.delta, 10.0 * (call.delta + put.delta))
    near(g.head.gamma, 10.0 * (call.gamma + put.gamma))
    near(g.head.theta, 10.0 * (call.theta + put.theta) / 365.0) // 每年->每日
    near(g.head.vega, 10.0 * (call.vega + put.vega) / 100.0)     // 对 1.0->对 1%

  test("发射间隔门控: ts 0/500/1000, interval=1000 -> 仅 0 与 1000"):
    val src = BsGreeksSource(
      FixedSource(Vector(trade(100.0, 0), trade(100.0, 500), trade(100.0, 1000))),
      config(straddles = 1.0, spot = 0.0),
    )
    val ts = src.events().filter(_.is(Topics.Greeks)).map(_.exchangeTs).toVector
    assertEquals(ts, Vector(0L, 1000L))

  test("cashBal (Balance) 仅首次发布一次"):
    val src = BsGreeksSource(
      FixedSource(Vector(trade(100.0, 0), trade(100.0, 1000), trade(100.0, 2000))),
      config(straddles = 1.0, spot = 3.0),
    )
    val bals = src.events().flatMap(_.as(Topics.Balance)).toVector
    assertEquals(bals.size, 1)
    assertEquals(bals.head.asset, ccy)
    assertEquals(bals.head.available, 3.0)

  test("经 StateManager: greeks().delta = 原始 delta + cashBal"):
    val src = BsGreeksSource(FixedSource(Vector(trade(100.0, 0))), config(straddles = 10.0, spot = 2.0))
    val sm = StateManager(Iterable(sym), orderTimeoutMs = 0L)
    src.events().foreach(sm.apply)
    val corrected = sm.greeks(ex, ccy)
    assert(corrected.isDefined)
    val tY = 30.0 / 365.0
    val raw = 10.0 * (BlackScholes.greeks(OptionRight.Call, 100.0, 100.0, tY, 0.2, 0.0).delta
      + BlackScholes.greeks(OptionRight.Put, 100.0, 100.0, tY, 0.2, 0.0).delta)
    near(corrected.get.delta, raw + 2.0)

  test("期权腿: 刚开仓现价=ATM -> P&L≈0; ATM 为首笔成交价; 不滚动"):
    val src = BsGreeksSource(FixedSource(Vector(trade(100.0, 0))), config(straddles = 1.0, spot = 0.0))
    src.events().toVector // 驱动初始化
    near(src.optionPnl(100.0, 0), 0.0) // 现价值 - 进场权利金
    assertEquals(src.strikePrice, 100.0)

  test("期权腿: 价格大涨 -> 长跨式 P&L 转正且量级合理 (接近内在价值增量)"):
    val src = BsGreeksSource(FixedSource(Vector(trade(100.0, 0))), config(straddles = 1.0, spot = 0.0))
    src.events().toVector
    val pnl = src.optionPnl(130.0, day)
    assert(pnl > 0, s"pnl=$pnl")
    // 涨到 130: call 至少值内在价值 30, 跨式现值 >= 30; 减进场权利金(双 ATM, IV 0.2, 30d 约 ~6.5) -> P&L 量级 ~20+
    assert(pnl > 15.0 && pnl < 35.0, s"pnl=$pnl 量级异常")

  test("临近到期 gamma 钳制到 minTenorDays 下限 (不发散)"):
    // expiry=30d, minTenorDays 默认 1 天；首笔在 expiry 前 1 分钟 -> 剩余 << 1 天, 应被钳制到 1 天
    val src = BsGreeksSource(FixedSource(Vector(trade(100.0, 30 * day - 60_000L))), config(straddles = 1.0, spot = 0.0))
    val g = src.events().flatMap(_.as(Topics.Greeks)).toVector.head
    val tFloor = 1.0 / 365.0 // 钳制下限 = 1 天
    val expected = BlackScholes.greeks(OptionRight.Call, 100.0, 100.0, tFloor, 0.2, 0.0).gamma +
      BlackScholes.greeks(OptionRight.Put, 100.0, 100.0, tFloor, 0.2, 0.0).gamma
    assert(g.gamma.isFinite, "gamma 不应发散")
    near(g.gamma, expected) // 按 1 天 (而非分钟级) 计算 -> 有界