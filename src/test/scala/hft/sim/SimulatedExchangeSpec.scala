package hft.sim

import hft.domain.*
import hft.engine.{Engine, ExchangeGateway}
import hft.exchange.{ExchangeClient, MarketDataStream}
import hft.event.{AnyEvent, Event, EventBus, Interest, Topics}
import hft.strategy.{OutcomeEvent, Strategy, StrategyHandlers}
import ox.{Ox, fork, supervised}

import java.util.concurrent.ConcurrentLinkedQueue
import scala.jdk.CollectionConverters.*
import hft.state.StateManager
import hft.TestUnits.given

/** 虚拟柜台撮合与延迟的测试，以及"策略无感知"的端到端集成。 */
class SimulatedExchangeSpec extends munit.FunSuite:
  private val sym = "BTCUSDT"

  private val meta = SymbolMeta(Exchange.Binance, sym, tickSize = 0.1, sizeStep = 0.001, minOrderSize = 0.001, contractSize = 1.0)

  /** 测试用极简策略: 收到首个 BBO 即在买一下方 offset 处挂一张 PostOnly 限价买单 (之后不再下单)。
    * 用于端到端验证"策略无感知地下单/成交", 不依赖任何业务策略实现。 */
  private class OneShotMakerStrategy(ex: Exchange, symbol: Symbol, offsetRatio: Double, orderSize: Coin) extends Strategy:
    private var placed = false
    override def orderTimeoutMs: Long = 60_000
    override def handlers = StrategyHandlers.empty.market(Topics.Bbo, Instrument(ex, symbol)) { (b, ctx, _) =>
      if placed then Vector.empty
      else
        placed = true
        val buyPx = b.bidPrice * (1.0 - offsetRatio) // 买一下方, PostOnly 静止挂单
        Vector(ctx.place(
          Order("", ex, symbol, Side.Long, OrderType.Limit(buyPx, TimeInForce.PostOnly), orderSize, reduceOnly = false, clientOrderId = ""),
          "oneshot maker buy",
        ))
    }

  /** 可手动喂行情的假上游公共流 */
  private class FakeMarketStream extends MarketDataStream:
    @volatile private var bus: EventBus = scala.compiletime.uninitialized
    override def exchange: Exchange = Exchange.Binance
    override def start(eventBus: EventBus)(using Ox): Unit = bus = eventBus
    override def subscribe(kinds: Set[SubscriptionKind]): Unit = ()
    def emitBbo(bid: Price, ask: Price, ts: Timestamp): Unit =
      bus.publish(Event.at(Topics.Bbo, BBO(Exchange.Binance, sym, bid, Coin(1.0), ask, Coin(1.0), ts), ts))

  /** 只提供 symbol 元数据的桩 REST 客户端。
    * 拆出 TradingClient 之后它只需实现这一个方法 —— 从前还要把六个私有端点各桩一个
    * "不该被调用"的返回值出来，那本身就是"公共客户端被迫假装自己能交易"的症状。 */
  private class StubPublicClient extends ExchangeClient:
    override def exchange: Exchange = Exchange.Binance
    override def fetchAllSymbolMetas(): Either[ExchangeError, Vector[SymbolMeta]] = Right(Vector(meta))

  /** 订阅总线并把事件收集到队列；返回收集器 (须在产生事件前调用) */
  private def collect(bus: EventBus)(using Ox): ConcurrentLinkedQueue[AnyEvent] =
    val q = ConcurrentLinkedQueue[AnyEvent]()
    val src = bus.subscribe(Topics.market.map(Interest.All.apply) ++ Topics.instrumentPrivate.map(Interest.All.apply))
    fork { while true do q.add(src.events.receive()) }
    q

  private def fills(q: ConcurrentLinkedQueue[AnyEvent]): Vector[Fill] =
    q.asScala.flatMap(_.as(Topics.Fill)).toVector

  private def orderStatuses(q: ConcurrentLinkedQueue[AnyEvent]): Vector[OrderStatus] =
    q.asScala.flatMap(_.as(Topics.OrderUpdate)).map(_.status).toVector

  /** 柜台扮演交易所，收的是**已换算成交易所格式**的订单 */
  private def limitOrder(side: Side, price: Price, tif: TimeInForce, cid: String): ExchangeOrder =
    ExchangeOrder(Exchange.Binance, sym, side, OrderType.Limit(price, tif), Contracts(0.002), reduceOnly = false, clientOrderId = cid)

  test("挂单成交判定: BBO 越过买单价 -> 成交于挂单价, 仓位增加"):
    supervised:
      val market = FakeMarketStream()
      val sim = SimulatedExchange(market, StubPublicClient(), SimConfig(0, 0, 10_000), AccountId.Live)
      val bus = EventBus()
      val q = collect(bus)
      sim.start(bus)

      market.emitBbo(50000, 50001, 1) // 现价
      val oid = sim.placeOrder(limitOrder(Side.Long, 49995.0, TimeInForce.PostOnly, "buy-1")).toOption.get
      Thread.sleep(80)
      assert(orderStatuses(q).contains(OrderStatus.Pending), "买单应先挂出 (Pending)")
      assert(fills(q).isEmpty, "未越价前不应成交")

      market.emitBbo(49990, 49994, 2) // 卖价 49994 <= 49995 -> 越过买单价
      Thread.sleep(80)

      val f = fills(q)
      assertEquals(f.map(_.side), Vector(Side.Long))
      assertEquals(f.head.price.value, 49995.0) // maker 成交价 = 挂单价
      assertEquals(f.head.size.value, 0.002)
      assertEquals(sim.fetchPositions().toOption.get.map(_.size.value), Vector(0.002))
      sim.shutdown()

  test("挂单成交判定: BBO 越过卖单价 -> 成交, 仓位转空"):
    supervised:
      val market = FakeMarketStream()
      val sim = SimulatedExchange(market, StubPublicClient(), SimConfig(0, 0, 10_000), AccountId.Live)
      val bus = EventBus()
      val q = collect(bus)
      sim.start(bus)

      market.emitBbo(50000, 50001, 1)
      sim.placeOrder(limitOrder(Side.Short, 50010.0, TimeInForce.PostOnly, "sell-1"))
      Thread.sleep(60)
      market.emitBbo(50012, 50013, 2) // 买价 50012 >= 50010 -> 越过卖单价
      Thread.sleep(80)

      val f = fills(q)
      assertEquals(f.map(_.side), Vector(Side.Short))
      assertEquals(sim.fetchPositions().toOption.get.map(_.size.value), Vector(-0.002))
      sim.shutdown()

  test("PostOnly 到达时已可成交 -> 拒单 (不吃单, 不成交)"):
    supervised:
      val market = FakeMarketStream()
      val sim = SimulatedExchange(market, StubPublicClient(), SimConfig(0, 0, 10_000), AccountId.Live)
      val bus = EventBus()
      val q = collect(bus)
      sim.start(bus)

      market.emitBbo(50000, 50001, 1)
      Thread.sleep(50) // 等行情进入柜台 (撮合需先知道现价才能判定可成交性)
      // 买单价 50001 >= 卖价 50001 -> 立即可成交, PostOnly 应被拒
      sim.placeOrder(limitOrder(Side.Long, 50001.0, TimeInForce.PostOnly, "buy-1"))
      Thread.sleep(80)

      assert(orderStatuses(q).exists(_.isInstanceOf[OrderStatus.Rejected]), "PostOnly 可成交应被拒")
      assert(fills(q).isEmpty)
      assert(sim.fetchPositions().toOption.get.isEmpty)
      sim.shutdown()

  test("撤单: resting 订单撤销后回报 Cancelled 并移出挂单簿"):
    supervised:
      val market = FakeMarketStream()
      val sim = SimulatedExchange(market, StubPublicClient(), SimConfig(0, 0, 10_000), AccountId.Live)
      val bus = EventBus()
      val q = collect(bus)
      sim.start(bus)

      market.emitBbo(50000, 50001, 1)
      val oid = sim.placeOrder(limitOrder(Side.Long, 49995.0, TimeInForce.PostOnly, "buy-1")).toOption.get
      Thread.sleep(60)
      assertEquals(sim.cancelOrder(sym, OrderRef.ByExchangeId(oid)), Right(()))
      Thread.sleep(60)

      assert(orderStatuses(q).contains(OrderStatus.Cancelled))
      assert(sim.fetchPendingOrders(sym).toOption.get.isEmpty)
      // 已撤订单再次撤单 -> OrderNotFound
      assert(sim.cancelOrder(sym, OrderRef.ByExchangeId(oid)) match { case Left(_: ExchangeError.OrderNotFound) => true; case _ => false })
      sim.shutdown()

  test("交易所->策略延迟: 成交回报延迟到达策略侧"):
    supervised:
      val market = FakeMarketStream()
      val sim = SimulatedExchange(market, StubPublicClient(), SimConfig(exchangeToStrategyDelayMs = 250, orderToExchangeDelayMs = 0, initialBalanceUsdt = 10_000), AccountId.Live)
      val bus = EventBus()
      val q = collect(bus)
      sim.start(bus)

      market.emitBbo(50000, 50001, 1)
      sim.placeOrder(limitOrder(Side.Long, 49995.0, TimeInForce.PostOnly, "buy-1"))
      market.emitBbo(49990, 49994, 2) // 触发成交 (柜台内部即时撮合)

      // 仓位在柜台内部已即时更新 (撮合不延迟)
      Thread.sleep(80)
      assertEquals(sim.fetchPositions().toOption.get.map(_.size.value), Vector(0.002), "柜台内部应即时成交")
      // 但成交回报尚未送达策略侧 (延迟 250ms)
      assert(fills(q).isEmpty, "延迟窗口内策略不应看到成交回报")

      Thread.sleep(300)
      assertEquals(fills(q).map(_.side), Vector(Side.Long), "延迟后成交回报应到达")
      sim.shutdown()

  test("端到端: 模拟盘提供与实盘一致的接口, 策略无感知地下单成交 (Engine + 极简 maker 策略)"):
    supervised:
      val market = FakeMarketStream()
      val sim = SimulatedExchange(market, StubPublicClient(), SimConfig(exchangeToStrategyDelayMs = 10, orderToExchangeDelayMs = 10, initialBalanceUsdt = 10_000), AccountId.Live)
      // clock/account 刷新间隔调大, 避免测试期周期任务干扰
      val engine = Engine.start(
        gateways = Vector(ExchangeGateway.trading(client = sim, marketData = sim, accountStream = Some(sim))),
        clockIntervalMs = 100_000,
        accountRefreshMs = 100_000,
      )
      engine.addStrategy(OneShotMakerStrategy(Exchange.Binance, sym, offsetRatio = 0.0001, orderSize = 0.002), AccountId.Live)

      // 初始行情 -> 策略在买一下方挂 PostOnly 买单 (~49995), resting
      market.emitBbo(50000, 50001, 1)
      Thread.sleep(250)
      assert(sim.fetchPendingOrders(sym).toOption.get.nonEmpty, "策略应已挂出订单到柜台")

      // 行情下跌, 卖价越过买单价 -> 买单成交
      market.emitBbo(49980, 49984, 2)
      Thread.sleep(300)

      val pos = sim.fetchPositions().toOption.get
      assert(pos.exists(_.size > 0), s"买单应成交形成多头, 实际仓位=$pos")
      sim.shutdown()

  test("contractSize != 1: 柜台扮演交易所, 收张数、撮合与回报用币本位"):
    // 真实网关在回报侧把张数还原成币本位, 柜台作为它的替身必须对称 ——
    // 否则 contractSize != 1 的交易所上, 模拟盘的成交量与实盘差一个倍数。
    supervised:
      val ctVal = 0.01
      val ctMeta = SymbolMeta(Exchange.Binance, sym, tickSize = 0.1, sizeStep = 1.0, minOrderSize = 1.0, contractSize = ctVal)
      val market = FakeMarketStream()
      class CtClient extends StubPublicClient:
        override def fetchAllSymbolMetas(): Either[ExchangeError, Vector[SymbolMeta]] = Right(Vector(ctMeta))
      val sim = SimulatedExchange(market, CtClient(), SimConfig(0, 0, 10_000), AccountId.Live)
      val bus = EventBus()
      val q = collect(bus)
      sim.start(bus)
      market.emitBbo(50000, 50001, 1)

      // 下 3 张 = 3 × 0.01 = 0.03 币
      sim.placeOrder(ExchangeOrder(Exchange.Binance, sym, Side.Long, OrderType.Limit(49995.0, TimeInForce.PostOnly), Contracts(3.0), reduceOnly = false, "c1"))
      Thread.sleep(80)
      market.emitBbo(49990, 49994, 2) // 越价成交
      Thread.sleep(80)

      assertEqualsDouble(fills(q).head.size.value, 3.0 * ctVal, 1e-12, "回报必须是币本位")
