package hft.backtest

import hft.domain.*
import hft.messaging.{EventData, IncomeEvent, StateManager}
import hft.option.{BlackScholes, OptionPosition, OptionRight, OptionSpec}

/** BS 合成 Greeks 源单测：聚合、发射间隔门控、cashBal 仅一次、以及经 StateManager 的 delta 修正。 */
class BsGreeksSourceSpec extends munit.FunSuite:
  private val ex = Exchange.Okx
  private val sym = "BTCUSDT"
  private val ccy = "BTC"

  private class FixedSource(evs: Vector[IncomeEvent]) extends MarketDataSource:
    def events(): Iterator[IncomeEvent] = evs.iterator

  private def trade(price: Price, ts: Timestamp): IncomeEvent =
    IncomeEvent(ts, ts, EventData.MarketTradeUpdate(MarketTrade(ex, sym, price, 1.0, isBuyerMaker = false, ts)))

  // 1 年期 ATM call, 标的中间价=100
  private val expiry: Timestamp = BlackScholes.MillisPerYear.toLong
  private val spec = OptionSpec(OptionRight.Call, strike = 100.0, expiry = expiry)

  private def config(qty: Double, spot: Double, intervalMs: Long = 1000) = BsGreeksConfig(
    exchange = ex,
    ccy = ccy,
    underlyingSymbol = sym,
    positions = Vector(OptionPosition(spec, qty)),
    impliedVol = 0.2,
    riskFreeRate = 0.0,
    spotHolding = spot,
    emitIntervalMs = intervalMs,
  )

  test("聚合: 账户级 delta = 持仓量 × 单份 BS delta"):
    val src = BsGreeksSource(FixedSource(Vector(trade(100.0, 0))), config(qty = 10.0, spot = 0.0))
    val greeks = src.events().collect { case IncomeEvent(_, _, EventData.GreeksUpdate(g)) => g }.toVector
    assertEquals(greeks.size, 1)
    val single = BlackScholes.greeks(OptionRight.Call, 100.0, 100.0, 1.0, 0.2, 0.0)
    assert(math.abs(greeks.head.delta - 10.0 * single.delta) < 1e-6, s"delta=${greeks.head.delta}")
    assert(math.abs(greeks.head.gamma - 10.0 * single.gamma) < 1e-9)
    // 通道规范单位换算: theta 每年->每日 (/365), vega 对 1.0->对 1% (/100)
    assert(math.abs(greeks.head.theta - 10.0 * single.theta / 365.0) < 1e-9, s"theta=${greeks.head.theta}")
    assert(math.abs(greeks.head.vega - 10.0 * single.vega / 100.0) < 1e-9, s"vega=${greeks.head.vega}")

  test("greeks 事件与触发 trade 同 exchangeTs, 排在该 trade 之后"):
    val src = BsGreeksSource(FixedSource(Vector(trade(100.0, 1234))), config(qty = 1.0, spot = 0.0))
    val evs = src.events().toVector
    val gIdx = evs.indexWhere(_.data.isInstanceOf[EventData.GreeksUpdate])
    assert(gIdx > 0, "greeks 应在 trade 之后")
    assertEquals(evs(gIdx).exchangeTs, 1234L)
    // 同一 exchangeTs 的触发 trade 排在 greeks 之前 (中间可能夹首次 cashBal)
    assert(evs.take(gIdx).exists(e => e.data.isInstanceOf[EventData.MarketTradeUpdate] && e.exchangeTs == 1234L))

  test("发射间隔门控: ts 0/500/1000, interval=1000 -> 仅 0 与 1000 发射"):
    val src = BsGreeksSource(
      FixedSource(Vector(trade(100.0, 0), trade(100.0, 500), trade(100.0, 1000))),
      config(qty = 1.0, spot = 0.0),
    )
    val greeksTs = src.events().collect { case IncomeEvent(ts, _, _: EventData.GreeksUpdate) => ts }.toVector
    assertEquals(greeksTs, Vector(0L, 1000L))

  test("cashBal (Balance) 仅首次发布一次"):
    val src = BsGreeksSource(
      FixedSource(Vector(trade(100.0, 0), trade(100.0, 1000), trade(100.0, 2000))),
      config(qty = 1.0, spot = 3.0),
    )
    val balances = src.events().collect { case IncomeEvent(_, _, EventData.BalanceUpdate(b)) => b }.toVector
    assertEquals(balances.size, 1)
    assertEquals(balances.head.asset, ccy)
    assertEquals(balances.head.available, 3.0)

  test("经 StateManager: greeks().delta = 原始 delta + cashBal"):
    val src = BsGreeksSource(FixedSource(Vector(trade(100.0, 0))), config(qty = 10.0, spot = 2.0))
    val sm = StateManager(Iterable(sym), orderTimeoutMs = 0L)
    src.events().foreach(sm.apply)
    val corrected = sm.greeks(ex, ccy)
    assert(corrected.isDefined, "greeks 与 cashBal 均到达后应返回 Some")
    val rawDelta = 10.0 * BlackScholes.greeks(OptionRight.Call, 100.0, 100.0, 1.0, 0.2, 0.0).delta
    assert(math.abs(corrected.get.delta - (rawDelta + 2.0)) < 1e-6, s"corrected=${corrected.get.delta}")

  test("缺 cashBal 时 greeks() 返回 None (需两者齐备)"):
    val sm = StateManager(Iterable(sym), orderTimeoutMs = 0L)
    sm.apply(IncomeEvent(0, 0, EventData.GreeksUpdate(Greeks(ex, ccy, 1.0, 0.1, -0.5, 2.0, 0))))
    assertEquals(sm.greeks(ex, ccy), None)
