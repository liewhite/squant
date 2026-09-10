package hft.sim

import hft.TestSim

import hft.actor.ActorSystem
import hft.domain.*
import hft.engine.Engine
import hft.event.Commands.{AccountOutcome, OrderIntent, OutcomeEvent}
import hft.event.{AnyEvent, Event, EventBus, Interest, Topics}
import hft.exchange.MarketFeed
import hft.event.MarketTopic
import hft.strategy.{Strategy, StrategyHandlers}
import ox.{Ox, fork, supervised}

import java.util.concurrent.ConcurrentLinkedQueue
import scala.jdk.CollectionConverters.*
import hft.TestUnits.given

/** 虚拟柜台 (替身) 的撮合与延迟，以及"策略无感知"的端到端集成。
  *
  * 柜台是插件：下单经总线的下单指令进来、回报经总线出去。测试因此完全走总线，
  * 不再有任何"直接调柜台的 REST 方法"的旁路 —— 那条旁路在实盘里根本不存在。
  */
object CustomFeedSpec:
  /** 用户自定义的行情族 —— 框架不知道它的存在, 但它是一等公民 */
  final case class DepthSnapshot(instrument: Instrument, value: Double)

  object Depth extends MarketTopic[DepthSnapshot]("testDepth"):
    def keyOf(p: DepthSnapshot): Instrument = p.instrument
    def streamKind(symbol: Symbol): SubscriptionKind = SubscriptionKind.BBO(symbol)

class SimulatedExchangeSpec extends munit.FunSuite:
  private val ex = Exchange.Binance
  private val sym = "BTCUSDT"
  private val meta = SymbolMeta(ex, sym, tickSize = 0.1, sizeStep = 0.001, minOrderSize = 0.001, contractSize = 1.0)
  private val metas = Map(Instrument.perp(ex, sym) -> meta)

  /** 可手动喂行情的假上游行情源。柜台把它装在自己的私有总线上 */
  private class FakeMarketFeed(publishOnConnect: Boolean = false) extends MarketFeed:
    override def exchange: Exchange = ex
    override protected def connect(): Unit =
      if publishOnConnect then emitBbo(50000, 50001, 1)
    override protected def subscribeToExchange(kinds: Set[SubscriptionKind]): Unit = ()
    /** 外部喂一条盘口 —— 走的正是真实行情源发布事件的那条路径 */
    def emitBbo(bid: Price, ask: Price, ts: Timestamp): Unit =
      publish(Event.at(Topics.Bbo, BBO(ex, sym, bid, Coin(1.0), ask, Coin(1.0), ts), ts))
    /** 外部喂一条**用户自定义**的行情 —— 框架把这类源当一等公民, 替身也必须转发 */
    def emitCustom(value: Double, ts: Timestamp): Unit =
      publish(Event.at(CustomFeedSpec.Depth, CustomFeedSpec.DepthSnapshot(Instrument.perp(ex, sym), value), ts))

  /** 测试用极简策略: 收到首个 BBO 即在买一下方 offset 处挂一张 PostOnly 限价买单 */
  private class OneShotMakerStrategy(offsetRatio: Double, orderSize: Coin) extends Strategy:
    private var placed = false
    override def handlers = StrategyHandlers.empty.market(Topics.Bbo, Instrument.perp(ex, sym)) { (b, ctx, _) =>
      if placed then Vector.empty
      else
        placed = true
        val buyPx = b.bidPrice * (1.0 - offsetRatio) // 买一下方, PostOnly 静止挂单
        ctx.place(
          Order("", ex, sym, Side.Long, OrderType.Limit(buyPx, TimeInForce.PostOnly), orderSize, reduceOnly = false, clientOrderId = ""),
          "oneshot maker buy",
        )
    }

  /** 订阅总线并把事件收集到队列 (须在产生事件前调用) */
  private def collect(bus: EventBus)(using Ox): ConcurrentLinkedQueue[AnyEvent] =
    val q = ConcurrentLinkedQueue[AnyEvent]()
    val src = bus.subscribe(Topics.market.map(Interest.All.apply) ++ Topics.instrumentPrivate.map(Interest.All.apply))
    fork { while true do q.add(src.events.receive()) }
    q

  private def fills(q: ConcurrentLinkedQueue[AnyEvent]): Vector[Fill] =
    q.asScala.flatMap(_.as(Topics.Fill)).toVector

  private def orderStatuses(q: ConcurrentLinkedQueue[AnyEvent]): Vector[OrderStatus] =
    q.asScala.flatMap(_.as(Topics.OrderUpdate)).map(_.status).toVector

  private def limitIntent(side: Side, price: Price, tif: TimeInForce, cid: String, qty: Coin = Coin(0.002)): AnyEvent =
    Event.local(OrderIntent, AccountOutcome(AccountId.Live, OutcomeEvent.PlaceOrders(
      Vector(Order("", ex, sym, side, OrderType.Limit(price, tif), qty, reduceOnly = false, clientOrderId = cid)),
      "test",
    )))

  private def cancelIntent(ref: OrderRef): AnyEvent =
    Event.local(OrderIntent, AccountOutcome(AccountId.Live, OutcomeEvent.CancelOrder(Instrument.perp(ex, sym), ref)))

  private def eventually(what: String)(cond: => Boolean): Unit =
    val deadline = System.nanoTime() + 3_000_000_000L
    while System.nanoTime() < deadline && !cond do Thread.sleep(5)
    assert(cond, s"等待超时: $what")

  /** 装一台虚拟柜台在总线上，交给测试体驱动 */
  private def withCounter(config: SimConfig, symbolMetas: Map[Instrument, SymbolMeta] = metas)(
      body: (EventBus, FakeMarketFeed, SimulatedExchange, ConcurrentLinkedQueue[AnyEvent]) => Unit
  ): Unit =
    supervised:
      val bus = EventBus()
      val q = collect(bus)
      val upstream = FakeMarketFeed()
      val sim = SimulatedExchange(upstream, symbolMetas, config, AccountId.Live)
      ActorSystem(bus).spawn(sim)
      body(bus, upstream, sim, q)

  test("挂单成交判定: BBO 越过买单价 -> 成交于挂单价, 仓位增加"):
    withCounter(TestSim.noFees.copy(exchangeToStrategyDelayMs = 0, orderToExchangeDelayMs = 0, initialBalanceUsdt = 10_000)) { (bus, upstream, sim, q) =>
      upstream.emitBbo(50000, 50001, 1) // 现价
      bus.publish(limitIntent(Side.Long, 49995.0, TimeInForce.PostOnly, "buy-1"))
      eventually("买单应先挂出 (Pending)")(orderStatuses(q).contains(OrderStatus.Pending))
      assert(fills(q).isEmpty, "未越价前不应成交")

      upstream.emitBbo(49990, 49994, 2) // 卖价 49994 <= 49995 -> 越过买单价
      eventually("应成交")(fills(q).nonEmpty)

      val f = fills(q)
      assertEquals(f.map(_.side), Vector(Side.Long))
      assertEquals(f.head.price.value, 49995.0) // maker 成交价 = 挂单价
      assertEquals(f.head.size.value, 0.002)
      eventually("仓位应到账")(sim.positions.map(_.size.value) == Vector(0.002))
    }

  test("挂单成交判定: BBO 越过卖单价 -> 成交, 仓位转空"):
    withCounter(TestSim.noFees.copy(exchangeToStrategyDelayMs = 0, orderToExchangeDelayMs = 0, initialBalanceUsdt = 10_000)) { (bus, upstream, sim, q) =>
      upstream.emitBbo(50000, 50001, 1)
      bus.publish(limitIntent(Side.Short, 50010.0, TimeInForce.PostOnly, "sell-1"))
      eventually("卖单应先挂出")(orderStatuses(q).contains(OrderStatus.Pending))
      upstream.emitBbo(50012, 50013, 2) // 买价 50012 >= 50010 -> 越过卖单价
      eventually("应成交")(fills(q).nonEmpty)

      assertEquals(fills(q).map(_.side), Vector(Side.Short))
      eventually("仓位应转空")(sim.positions.map(_.size.value) == Vector(-0.002))
    }

  test("PostOnly 到达时已可成交 -> 拒单 (不吃单, 不成交)"):
    withCounter(TestSim.noFees.copy(exchangeToStrategyDelayMs = 0, orderToExchangeDelayMs = 0, initialBalanceUsdt = 10_000)) { (bus, upstream, sim, q) =>
      upstream.emitBbo(50000, 50001, 1)
      // 等行情进入柜台 (撮合需先知道现价才能判定可成交性)
      eventually("行情应已转发")(q.asScala.exists(_.is(Topics.Bbo)))
      // 买单价 50001 >= 卖价 50001 -> 立即可成交, PostOnly 应被拒
      bus.publish(limitIntent(Side.Long, 50001.0, TimeInForce.PostOnly, "buy-1"))
      eventually("PostOnly 可成交应被拒")(orderStatuses(q).exists(_.isInstanceOf[OrderStatus.Rejected]))
      assert(fills(q).isEmpty)
      assert(sim.positions.isEmpty)
    }

  test("撤单: resting 订单撤销后回报 Cancelled 并移出挂单簿"):
    withCounter(TestSim.noFees.copy(exchangeToStrategyDelayMs = 0, orderToExchangeDelayMs = 0, initialBalanceUsdt = 10_000)) { (bus, upstream, sim, q) =>
      upstream.emitBbo(50000, 50001, 1)
      bus.publish(limitIntent(Side.Long, 49995.0, TimeInForce.PostOnly, "buy-1"))
      eventually("买单应先挂出")(orderStatuses(q).contains(OrderStatus.Pending))

      // 在途单也撤得掉 —— 按 clientOrderId 指名 (见 OrderRef)
      bus.publish(cancelIntent(OrderRef.ByClientId("buy-1")))
      eventually("应回报 Cancelled")(orderStatuses(q).contains(OrderStatus.Cancelled))
    }

  test("交易所->策略延迟: 成交回报延迟到达策略侧"):
    withCounter(TestSim.noFees.copy(exchangeToStrategyDelayMs = 250, orderToExchangeDelayMs = 0, initialBalanceUsdt = 10_000)) {
      (bus, upstream, sim, q) =>
        upstream.emitBbo(50000, 50001, 1)
        bus.publish(limitIntent(Side.Long, 49995.0, TimeInForce.PostOnly, "buy-1"))
        eventually("买单应先挂出")(orderStatuses(q).contains(OrderStatus.Pending))
        upstream.emitBbo(49990, 49994, 2) // 触发成交 (柜台内部即时撮合)

        // 仓位在柜台内部已即时更新 (撮合不延迟)
        eventually("柜台内部应即时成交")(sim.positions.map(_.size.value) == Vector(0.002))
        assert(fills(q).isEmpty, "延迟窗口内策略不应看到成交回报")

        eventually("延迟后成交回报应到达")(fills(q).map(_.side) == Vector(Side.Long))
    }

  test("端到端: 柜台与行情面都由替身扮演, 策略无感知地下单成交"):
    supervised:
      val upstream = FakeMarketFeed()
      val sim = SimulatedExchange(upstream, metas, TestSim.noFees.copy(exchangeToStrategyDelayMs = 10, orderToExchangeDelayMs = 10, initialBalanceUsdt = 10_000), AccountId.Live)
      // 时钟间隔调大, 避免测试期周期任务干扰
      Engine.run(plugins = Vector(sim), clockIntervalMs = 100_000) { engine =>
        // 替身同时接下单指令、对齐指令与行情订阅指令 —— 契约校验因此通过, 策略起得来
        engine.addStrategy(OneShotMakerStrategy(offsetRatio = 0.0001, orderSize = 0.002), AccountId.Live)

        // 初始行情 -> 策略在买一下方挂 PostOnly 买单 (~49995), resting
        upstream.emitBbo(50000, 50001, 1)
        // 行情下跌, 卖价越过买单价 -> 买单成交
        eventually("策略应已挂出订单到柜台")(sim.restingCount > 0)
        upstream.emitBbo(49980, 49984, 2)
        eventually("买单应成交形成多头")(sim.positions.exists(_.size > Coin.Zero))
      }

  test("用户自定义的行情 topic 也被替身转发出去 —— 否则它在私有总线上静默消失"):
    // 替身若按内置行情 topic 枚举着收上游, 用户自定义的行情源就会在这里断掉:
    // 契约校验通过 (替身接了订阅指令)、指令也转下去了, 数据却没人接着往外送 ——
    // 策略"很安静", 没有任何症状。这正是本架构最忌讳的失效形态。
    supervised:
      val bus = EventBus()
      val seen = ConcurrentLinkedQueue[Double]()
      val mailbox = bus.subscribe(Set(Interest.All(CustomFeedSpec.Depth)))
      fork { mailbox.events.foreach(ev => ev.as(CustomFeedSpec.Depth).foreach(d => seen.add(d.value))) }
      val upstream = FakeMarketFeed()
      ActorSystem(bus).spawn(SimulatedExchange(upstream, metas, TestSim.noFees.copy(exchangeToStrategyDelayMs = 0, orderToExchangeDelayMs = 0, initialBalanceUsdt = 10_000), AccountId.Live))

      upstream.emitCustom(42.0, 1)
      eventually("自定义行情应被转发到主总线")(seen.asScala.toVector == Vector(42.0))

  test("上游在 onStart 立即发布的首条行情不会落入中继订阅窗口之前"):
    supervised:
      val bus = EventBus()
      val seen = ConcurrentLinkedQueue[Timestamp]()
      val mailbox = bus.subscribe(Set(Interest.All(Topics.Bbo)))
      fork { mailbox.events.foreach(event => if event.is(Topics.Bbo) then seen.add(event.exchangeTs)) }
      val upstream = FakeMarketFeed(publishOnConnect = true)
      ActorSystem(bus).spawn(SimulatedExchange(upstream, metas, TestSim.noFees.copy(exchangeToStrategyDelayMs = 0, orderToExchangeDelayMs = 0, initialBalanceUsdt = 10_000), AccountId.Live))

      eventually("首条行情应穿过已先建立的私有总线中继")(seen.asScala.toVector == Vector(1L))

  test("行情订阅指令不会在两条总线之间弹跳"):
    // 指令是从主总线流进替身、再转给私有总线上游的。若中继把它原样转回主总线,
    // 替身就会再收到一次、再转一次 —— 无限循环, 症状是 CPU 打满而不是报错。
    supervised:
      val bus = EventBus()
      val upstream = FakeMarketFeed()
      ActorSystem(bus).spawn(SimulatedExchange(upstream, metas, TestSim.noFees.copy(exchangeToStrategyDelayMs = 0, orderToExchangeDelayMs = 0, initialBalanceUsdt = 10_000), AccountId.Live))

      val relayed = ConcurrentLinkedQueue[AnyEvent]()
      val mailbox = bus.subscribe(Set(Interest.Keyed(hft.event.Commands.MarketSubscription, Set(ex))))
      fork { mailbox.events.foreach(relayed.add) }

      bus.publish(Event.local(
        hft.event.Commands.MarketSubscription,
        hft.event.Commands.MarketSubscriptionRequest(ex, Set(SubscriptionKind.BBO(sym))),
      ))
      Thread.sleep(200) // 真弹跳的话这 200ms 足够攒出成千上万条
      assertEquals(relayed.size, 1, "订阅指令只该在主总线上出现一次")

  test("精度对齐在柜台里发生: 收不下的量不进撮合, 以拒单回流"):
    // 精度是交易所的事实, 影子盘也照此对齐 —— 否则它的成交量与实盘系统性地差一个取整,
    // 而它存在的全部理由就是预测实盘。
    val coarse = Map(Instrument.perp(ex, sym) -> SymbolMeta(ex, sym, tickSize = 0.1, sizeStep = 1.0, minOrderSize = 1.0, contractSize = 0.01))
    withCounter(TestSim.noFees.copy(exchangeToStrategyDelayMs = 0, orderToExchangeDelayMs = 0, initialBalanceUsdt = 10_000), coarse) { (bus, upstream, sim, q) =>
      upstream.emitBbo(50000, 50001, 1)
      bus.publish(limitIntent(Side.Long, 49995.0, TimeInForce.PostOnly, "dust", qty = Coin(0.004))) // 0.4 张 < 1 张
      eventually("应以拒单回流")(orderStatuses(q).exists {
        case OrderStatus.Error(reason) => reason.contains("最小下单量")
        case _                         => false
      })
      assert(sim.restingCount == 0, "收不下的单不该进挂单簿")
    }
