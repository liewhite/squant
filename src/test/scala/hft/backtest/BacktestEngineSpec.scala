package hft.backtest

import hft.domain.*
import hft.engine.StrategyRunner
import hft.event.{AnyEvent, Event, Interest, Topics}
import hft.sim.SimConfig
import hft.strategy.{OutcomeEvent, Strategy, StrategyHandlers}
import hft.state.{StateManager}
import hft.TestUnits.given

/** 回测引擎单测：用内存假数据源驱动，验证 下单->挂单->越价成交 全链路 + 确定性。 */
class BacktestEngineSpec extends munit.FunSuite:
  private val ex = Exchange.Binance
  private val sym = "BTCUSDT"
  private val metas = Map((ex, sym) -> SymbolMeta(ex, sym, tickSize = 0.01, sizeStep = 0.001, minOrderSize = 0.001, contractSize = 1.0))

  private def bboEv(bid: Price, ask: Price, ts: Timestamp): AnyEvent =
    Event.at(Topics.Bbo, BBO(ex, sym, bid, Coin(1.0), ask, Coin(1.0), ts), ts)

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
    assertEqualsDouble(pos.size.value, 1.0, 1e-9)
    // 未实现 = (mark 99.75 - entry 100) * 1 = -0.25; equity = 10000 - 0.25
    assertEqualsDouble(r.finalEquity, 10_000.0 - 0.25, 1e-6)

  test("行情先于它引发的成交送达策略 (撮合不回显, 由引擎按网关职责转发)"):
    val evs = runCollect()
    val bboIdx = evs.indexWhere(e => e.is(Topics.Bbo) && e.exchangeTs == 2000L)
    val fillIdx = evs.indexWhere(_.is(Topics.Fill))
    assert(bboIdx >= 0, "引发成交的那条行情应被转发给策略")
    assert(fillIdx >= 0, "应有成交")
    assert(bboIdx < fillIdx, s"行情($bboIdx) 必须排在它引发的成交($fillIdx) 之前")

  test("每条行情都被转发, 一条不少"):
    val bbos = runCollect().count(_.is(Topics.Bbo))
    assertEquals(bbos, series.size, "撮合不再回显行情后, 转发的条数仍应等于源事件数")

  test("账户不一致的 runner 在装配期即被拒 (私有回报按账户路由, 不一致会静默饿死策略)"):
    val runner = StrategyRunner.backtest(OneShotBuy(), metas, AccountId.Paper(1))
    val e = intercept[IllegalArgumentException](
      BacktestEngine(ex, FixedSource(series), Seq(runner), SimConfig(), metas)
    )
    assert(e.getMessage.contains("paper1"), e.getMessage)

  test("数据源乱序: 虚拟时间不倒流, 乱序条数被计数上报"):
    // 第 2 条 BBO 的时间戳倒退回 1500 (< 已推进到的 2000), 引擎应钳制而非让时间回退
    val disordered = Vector(bboEv(100.0, 100.1, 1000), bboEv(99.8, 99.9, 2000), bboEv(99.7, 99.8, 1500))
    val runner = StrategyRunner.backtest(OneShotBuy(), metas)
    val r = BacktestEngine(ex, FixedSource(disordered), Seq(runner), SimConfig(initialBalanceUsdt = 10_000.0), metas).run()
    assertEquals(r.outOfOrderEvents, 1L)
    assertEquals(r.marketEvents, 3L)
    assert(r.lastTs >= 2000L, s"虚拟时间不得回退到乱序事件的时间戳, got ${r.lastTs}")

  test("数据源有序时乱序计数为 0"):
    assertEquals(runOnce().outOfOrderEvents, 0L)

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
