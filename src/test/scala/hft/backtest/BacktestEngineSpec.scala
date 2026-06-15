package hft.backtest

import hft.domain.*
import hft.engine.StrategyRunner
import hft.exchange.SubscriptionKind
import hft.messaging.{EventData, IncomeEvent, StateManager}
import hft.sim.SimConfig
import hft.strategy.{OutcomeEvent, Strategy}

/** 回测引擎单测：用内存假数据源驱动，验证 下单->挂单->越价成交 全链路 + 确定性。 */
class BacktestEngineSpec extends munit.FunSuite:
  private val ex = Exchange.Binance
  private val sym = "BTCUSDT"
  private val metas = Map((ex, sym) -> SymbolMeta(ex, sym, tickSize = 0.01, sizeStep = 0.001, minOrderSize = 0.001, contractSize = 1.0))

  private def bboEv(bid: Price, ask: Price, ts: Timestamp): IncomeEvent =
    IncomeEvent.at(ts, EventData.BboUpdate(BBO(ex, sym, bid, 1.0, ask, 1.0, ts)))

  /** 假数据源：手造 BBO 序列。 */
  private class FixedSource(evs: Vector[IncomeEvent]) extends MarketDataSource:
    def events(): Iterator[IncomeEvent] = evs.iterator

  /** 首个 BBO 时挂一张 PostOnly 买单 (挂在买一价, 不可成交故 resting)。 */
  private class OneShotBuy extends Strategy:
    private var placed = false
    def publicStreams = Map(ex -> Set(SubscriptionKind.BBO(sym)))
    def orderTimeoutMs = 0L
    def onEvent(event: IncomeEvent, state: StateManager): Vector[OutcomeEvent] = event.data match
      case EventData.BboUpdate(b) if !placed =>
        placed = true
        Vector(OutcomeEvent.PlaceOrders(
          Vector(Order("", ex, sym, Side.Long, OrderType.Limit(b.bidPrice, TimeInForce.PostOnly), 1.0, reduceOnly = false, clientOrderId = "")),
          "buy",
        ))
      case _ => Vector.empty

  private val series = Vector(
    bboEv(100.0, 100.1, 1000), // 挂买单 @100
    bboEv(99.8, 99.9, 2000),   // ask 99.9 <= 100 -> 越价, resting 买单成交 @100
    bboEv(99.7, 99.8, 3000),
  )

  private def runOnce(): BacktestResult =
    val runner = StrategyRunner(OneShotBuy(), metas)
    BacktestEngine(ex, FixedSource(series), Seq(runner), SimConfig(initialBalanceUsdt = 10_000.0)).run()

  test("挂单越价成交: 1 笔成交, 持仓 +1, 已实现盈亏 0"):
    val r = runOnce()
    assertEquals(r.fills, 1)
    assertEquals(r.marketEvents, 3L)
    assertEquals(r.realizedPnl, 0.0) // 只开仓未平仓
    val pos = r.positions.find(_.symbol == sym).get
    assertEqualsDouble(pos.size, 1.0, 1e-9)
    // 未实现 = (mark 99.75 - entry 100) * 1 = -0.25; equity = 10000 - 0.25
    assertEqualsDouble(r.finalEquity, 10_000.0 - 0.25, 1e-6)

  test("确定性: 相同输入两次运行结果完全一致"):
    assertEquals(runOnce(), runOnce())
