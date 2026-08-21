package hft.sim

import hft.domain.*
import hft.engine.{Engine, ExchangeGateway}
import hft.exchange.{ExchangeClient, MarketDataStream, SubscriptionKind}
import hft.messaging.{EventBus, EventData, IncomeEvent, StateManager}
import hft.strategy.{OutcomeEvent, Strategy}
import ox.{Ox, fork, supervised}

import java.util.concurrent.ConcurrentLinkedQueue
import scala.jdk.CollectionConverters.*

/** 虚拟柜台撮合与延迟的测试，以及"策略无感知"的端到端集成。 */
class SimulatedExchangeSpec extends munit.FunSuite:
  private val sym = "BTCUSDT"

  private val meta = SymbolMeta(Exchange.Binance, sym, tickSize = 0.1, sizeStep = 0.001, minOrderSize = 0.001, contractSize = 1.0)

  /** 测试用极简策略: 收到首个 BBO 即在买一下方 offset 处挂一张 PostOnly 限价买单 (之后不再下单)。
    * 用于端到端验证"策略无感知地下单/成交", 不依赖任何业务策略实现。 */
  private class OneShotMakerStrategy(ex: Exchange, symbol: Symbol, offsetRatio: Double, orderSize: Quantity) extends Strategy:
    private var placed = false
    override def orderTimeoutMs: Long = 60_000
    override def publicStreams: Map[Exchange, Set[SubscriptionKind]] = Map(ex -> Set(SubscriptionKind.BBO(symbol)))
    override def onEvent(event: IncomeEvent, state: StateManager): Vector[OutcomeEvent] =
      event.data match
        case EventData.BboUpdate(b) if !placed && b.exchange == ex && b.symbol == symbol =>
          placed = true
          val buyPx = b.bidPrice * (1.0 - offsetRatio) // 买一下方, PostOnly 静止挂单
          Vector(OutcomeEvent.PlaceOrders(
            Vector(Order("", ex, symbol, Side.Long, OrderType.Limit(buyPx, TimeInForce.PostOnly), orderSize, reduceOnly = false, clientOrderId = "")),
            "oneshot maker buy",
          ))
        case _ => Vector.empty

  /** 可手动喂行情的假上游公共流 */
  private class FakeMarketStream extends MarketDataStream:
    @volatile private var bus: EventBus[IncomeEvent] = scala.compiletime.uninitialized
    override def exchange: Exchange = Exchange.Binance
    override def start(incomeBus: EventBus[IncomeEvent])(using Ox): Unit = bus = incomeBus
    override def subscribe(kinds: Set[SubscriptionKind]): Unit = ()
    def emitBbo(bid: Price, ask: Price, ts: Timestamp): Unit =
      bus.publish(IncomeEvent.at(ts, EventData.BboUpdate(BBO(Exchange.Binance, sym, bid, 1.0, ask, 1.0, ts))))

  /** 只提供 symbol 元数据的桩 REST 客户端 (其余账户接口由柜台覆盖, 不应被调用) */
  private class StubPublicClient extends ExchangeClient:
    override def exchange: Exchange = Exchange.Binance
    override def fetchAllSymbolMetas(): Either[ExchangeError, Vector[SymbolMeta]] = Right(Vector(meta))
    private def unused = Left(ExchangeError.Other("stub: not used"))
    override def placeOrder(order: Order) = unused
    override def cancelOrder(symbol: Symbol, orderId: OrderId) = unused
    override def fetchPendingOrders(symbol: Symbol) = unused
    override def setLeverage(symbol: Symbol, leverage: Int) = unused
    override def fetchAccountInfo() = unused
    override def fetchPositions() = unused

  /** 订阅总线并把事件收集到队列；返回收集器 (须在产生事件前调用) */
  private def collect(bus: EventBus[IncomeEvent])(using Ox): ConcurrentLinkedQueue[IncomeEvent] =
    val q = ConcurrentLinkedQueue[IncomeEvent]()
    val src = bus.subscribe()
    fork { while true do q.add(src.receive()) }
    q

  private def fills(q: ConcurrentLinkedQueue[IncomeEvent]): Vector[Fill] =
    q.asScala.collect { case IncomeEvent(_, _, EventData.FillUpdate(f)) => f }.toVector

  private def orderStatuses(q: ConcurrentLinkedQueue[IncomeEvent]): Vector[OrderStatus] =
    q.asScala.collect { case IncomeEvent(_, _, EventData.OrderUpdated(u)) => u.status }.toVector

  private def limitOrder(side: Side, price: Price, tif: TimeInForce, cid: String): Order =
    Order("", Exchange.Binance, sym, side, OrderType.Limit(price, tif), 0.002, reduceOnly = false, clientOrderId = cid)

  test("挂单成交判定: BBO 越过买单价 -> 成交于挂单价, 仓位增加"):
    supervised:
      val market = FakeMarketStream()
      val sim = SimulatedExchange(market, StubPublicClient(), SimConfig(0, 0, 10_000))
      val bus = EventBus[IncomeEvent]()
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
      assertEquals(f.head.price, 49995.0) // maker 成交价 = 挂单价
      assertEquals(f.head.size, 0.002)
      assertEquals(sim.fetchPositions().toOption.get.map(_.size), Vector(0.002))
      sim.shutdown()

  test("挂单成交判定: BBO 越过卖单价 -> 成交, 仓位转空"):
    supervised:
      val market = FakeMarketStream()
      val sim = SimulatedExchange(market, StubPublicClient(), SimConfig(0, 0, 10_000))
      val bus = EventBus[IncomeEvent]()
      val q = collect(bus)
      sim.start(bus)

      market.emitBbo(50000, 50001, 1)
      sim.placeOrder(limitOrder(Side.Short, 50010.0, TimeInForce.PostOnly, "sell-1"))
      Thread.sleep(60)
      market.emitBbo(50012, 50013, 2) // 买价 50012 >= 50010 -> 越过卖单价
      Thread.sleep(80)

      val f = fills(q)
      assertEquals(f.map(_.side), Vector(Side.Short))
      assertEquals(sim.fetchPositions().toOption.get.map(_.size), Vector(-0.002))
      sim.shutdown()

  test("PostOnly 到达时已可成交 -> 拒单 (不吃单, 不成交)"):
    supervised:
      val market = FakeMarketStream()
      val sim = SimulatedExchange(market, StubPublicClient(), SimConfig(0, 0, 10_000))
      val bus = EventBus[IncomeEvent]()
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
      val sim = SimulatedExchange(market, StubPublicClient(), SimConfig(0, 0, 10_000))
      val bus = EventBus[IncomeEvent]()
      val q = collect(bus)
      sim.start(bus)

      market.emitBbo(50000, 50001, 1)
      val oid = sim.placeOrder(limitOrder(Side.Long, 49995.0, TimeInForce.PostOnly, "buy-1")).toOption.get
      Thread.sleep(60)
      assertEquals(sim.cancelOrder(sym, oid), Right(()))
      Thread.sleep(60)

      assert(orderStatuses(q).contains(OrderStatus.Cancelled))
      assert(sim.fetchPendingOrders(sym).toOption.get.isEmpty)
      // 已撤订单再次撤单 -> OrderNotFound
      assert(sim.cancelOrder(sym, oid) match { case Left(_: ExchangeError.OrderNotFound) => true; case _ => false })
      sim.shutdown()

  test("交易所->策略延迟: 成交回报延迟到达策略侧"):
    supervised:
      val market = FakeMarketStream()
      val sim = SimulatedExchange(market, StubPublicClient(), SimConfig(exchangeToStrategyDelayMs = 250, orderToExchangeDelayMs = 0, initialBalanceUsdt = 10_000))
      val bus = EventBus[IncomeEvent]()
      val q = collect(bus)
      sim.start(bus)

      market.emitBbo(50000, 50001, 1)
      sim.placeOrder(limitOrder(Side.Long, 49995.0, TimeInForce.PostOnly, "buy-1"))
      market.emitBbo(49990, 49994, 2) // 触发成交 (柜台内部即时撮合)

      // 仓位在柜台内部已即时更新 (撮合不延迟)
      Thread.sleep(80)
      assertEquals(sim.fetchPositions().toOption.get.map(_.size), Vector(0.002), "柜台内部应即时成交")
      // 但成交回报尚未送达策略侧 (延迟 250ms)
      assert(fills(q).isEmpty, "延迟窗口内策略不应看到成交回报")

      Thread.sleep(300)
      assertEquals(fills(q).map(_.side), Vector(Side.Long), "延迟后成交回报应到达")
      sim.shutdown()

  test("端到端: 模拟盘提供与实盘一致的接口, 策略无感知地下单成交 (Engine + 极简 maker 策略)"):
    supervised:
      val market = FakeMarketStream()
      val sim = SimulatedExchange(market, StubPublicClient(), SimConfig(exchangeToStrategyDelayMs = 10, orderToExchangeDelayMs = 10, initialBalanceUsdt = 10_000))
      // clock/account 刷新间隔调大, 避免测试期周期任务干扰
      val engine = Engine.start(
        gateways = Vector(ExchangeGateway(client = sim, marketData = sim, accountStream = Some(sim))),
        dryRun = false,
        clockIntervalMs = 100_000,
        accountRefreshMs = 100_000,
      )
      engine.addStrategy(OneShotMakerStrategy(Exchange.Binance, sym, offsetRatio = 0.0001, orderSize = 0.002))

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
