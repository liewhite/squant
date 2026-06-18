package hft.backtest

import hft.domain.*
import hft.messaging.{EventData, IncomeEvent, StateManager}
import hft.option.{BlackScholes, OptionRight}

/** BS 合成 Greeks 源 (滚动 ATM 跨式) 单测：聚合、发射间隔门控、cashBal 一次、StateManager delta 修正、
  * 期权腿 P&L 与滚动。 */
class BsGreeksSourceSpec extends munit.FunSuite:
  private val ex = Exchange.Okx
  private val sym = "BTCUSDT"
  private val ccy = "BTC"
  private val day = 86_400_000L

  private class FixedSource(evs: Vector[IncomeEvent]) extends MarketDataSource:
    def events(): Iterator[IncomeEvent] = evs.iterator

  private def trade(price: Price, ts: Timestamp): IncomeEvent =
    IncomeEvent(ts, ts, EventData.MarketTradeUpdate(MarketTrade(ex, sym, price, 1.0, isBuyerMaker = false, ts)))

  // 到期设在 30 天后 (单只持有, 不滚动)
  private def config(straddles: Double, spot: Double, intervalMs: Long = 1000) =
    BsGreeksConfig(ex, ccy, sym, straddles = straddles, impliedVol = 0.2, expiry = 30 * day,
      riskFreeRate = 0.0, spotHolding = spot, emitIntervalMs = intervalMs)

  private def near(a: Double, b: Double, eps: Double = 1e-6): Unit = assert(math.abs(a - b) < eps, s"$a vs $b")

  test("聚合: 账户级 delta/gamma = 份数 × (call+put) 单份 BS, ATM=首笔成交价"):
    val src = BsGreeksSource(FixedSource(Vector(trade(100.0, 0))), config(straddles = 10.0, spot = 0.0))
    val g = src.events().collect { case IncomeEvent(_, _, EventData.GreeksUpdate(gg)) => gg }.toVector
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
    val ts = src.events().collect { case IncomeEvent(t, _, _: EventData.GreeksUpdate) => t }.toVector
    assertEquals(ts, Vector(0L, 1000L))

  test("cashBal (Balance) 仅首次发布一次"):
    val src = BsGreeksSource(
      FixedSource(Vector(trade(100.0, 0), trade(100.0, 1000), trade(100.0, 2000))),
      config(straddles = 1.0, spot = 3.0),
    )
    val bals = src.events().collect { case IncomeEvent(_, _, EventData.BalanceUpdate(b)) => b }.toVector
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

  test("期权腿: 价格大涨 -> 长跨式 P&L 转正 (call 增值盖过 put)"):
    val src = BsGreeksSource(FixedSource(Vector(trade(100.0, 0))), config(straddles = 1.0, spot = 0.0))
    src.events().toVector
    assert(src.optionPnl(130.0, day) > 0, s"pnl=${src.optionPnl(130.0, day)}") // 涨 30%, 跨式获利