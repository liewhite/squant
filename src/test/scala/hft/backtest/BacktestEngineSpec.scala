package hft.backtest

import hft.domain.*
import hft.engine.StrategyRunner
import hft.exchange.SubscriptionKind
import hft.event.{AnyEvent, Event, Interest, Topics}
import hft.sim.SimConfig
import hft.strategy.{OutcomeEvent, Strategy, StrategyHandlers}
import hft.state.{StateManager}

/** 回测引擎单测：用内存假数据源驱动，验证 下单->挂单->越价成交 全链路 + 确定性。 */
class BacktestEngineSpec extends munit.FunSuite:
  private val ex = Exchange.Binance
  private val sym = "BTCUSDT"
  private val metas = Map((ex, sym) -> SymbolMeta(ex, sym, tickSize = 0.01, sizeStep = 0.001, minOrderSize = 0.001, contractSize = 1.0))

  private def bboEv(bid: Price, ask: Price, ts: Timestamp): AnyEvent =
    Event.at(Topics.Bbo, BBO(ex, sym, bid, 1.0, ask, 1.0, ts), ts)

  /** 假数据源：手造 BBO 序列。 */
  private class FixedSource(evs: Vector[AnyEvent]) extends MarketDataSource:
    def events(): Iterator[AnyEvent] = evs.iterator

  /** 首个 BBO 时挂一张 PostOnly 买单 (挂在买一价, 不可成交故 resting)。 */
  private class OneShotBuy extends Strategy:
    private var placed = false
    def orderTimeoutMs = 0L
    def handlers = StrategyHandlers.empty.market(Topics.Bbo, Instrument(ex, sym)) { (b, ctx, _) =>
      if placed then Vector.empty
      else
        placed = true
        Vector(ctx.place(
          Order("", ex, sym, Side.Long, OrderType.Limit(b.bidPrice, TimeInForce.PostOnly), 1.0, reduceOnly = false, clientOrderId = ""),
          "buy",
        ))
    }

  private val series = Vector(
    bboEv(100.0, 100.1, 1000), // 挂买单 @100
    bboEv(99.8, 99.9, 2000),   // ask 99.9 <= 100 -> 越价, resting 买单成交 @100
    bboEv(99.7, 99.8, 3000),
  )

  private def runOnce(): BacktestResult =
    val runner = StrategyRunner.backtest(OneShotBuy(), metas)
    BacktestEngine(ex, FixedSource(series), Seq(runner), SimConfig(initialBalanceUsdt = 10_000.0), metas).run()

  /** 跑一次并收集投递给观察者的全部事件 (含逐笔回报的 client_order_id 与时间戳)。 */
  private def runCollect(): Vector[AnyEvent] =
    val collected = Vector.newBuilder[AnyEvent]
    val runner = StrategyRunner.backtest(OneShotBuy(), metas)
    BacktestEngine(ex, FixedSource(series), Seq(runner), SimConfig(initialBalanceUsdt = 10_000.0), metas, observers = Seq(collected += _)).run()
    collected.result()

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

  test("确定性: 逐笔回报流 (client_order_id + 时间戳) 跨运行完全一致"):
    // 直接比对全部投递事件 —— 含 OrderUpdate/Fill 的 client_order_id 与 exchangeTs/localTs。
    // 旧实现里 UUID 随机 + 墙钟时间戳会让两次运行不等，能捕获 #1/#2 回归。
    assertEquals(runCollect(), runCollect())

  test("确定性: client_order_id 为自增计数, 回报时间戳取虚拟时间 (非墙钟)"):
    val evs = runCollect()
    val orderUpdates = evs.flatMap(_.as(Topics.OrderUpdate))
    assert(orderUpdates.nonEmpty, "应有订单回报")
    assert(orderUpdates.forall(_.clientOrderId.contains("bt0")), s"client_order_id 应为确定性 bt0: ${orderUpdates.map(_.clientOrderId)}")
    // 数据时间在 1000..3000，回报时间戳必落在虚拟时间量级 (远小于墙钟 ~1.7e12)
    val fillEvs = evs.filter(_.is(Topics.Fill))
    assertEquals(fillEvs.size, 1)
    fillEvs.foreach { e =>
      assert(e.exchangeTs < 1_000_000L && e.localTs < 1_000_000L, s"回报时间戳应为虚拟时间, got $e")
    }
