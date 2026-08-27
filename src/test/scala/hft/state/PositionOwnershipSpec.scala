package hft.state

import hft.actor.ActorSystem
import hft.domain.*
import hft.engine.Executor
import hft.event.{AnyEvent, Event, EventBus, Interest, Topics}
import hft.event.Commands.{AccountSync, AccountSyncRequest, AccountSynced, OrderIntent, OutcomeEvent}
import hft.exchange.{AccountFeed, AccountReport, RestTradingGateway, TradingClient}
import hft.sim.{PaperCounter, SimConfig}
import hft.strategy.{Strategy, StrategyHandlers}
import ox.supervised

import java.util.concurrent.ConcurrentLinkedQueue
import scala.jdk.CollectionConverters.*
import hft.TestUnits.given

/** 仓位归柜台之后的两条契约。
  *
  * 这两条都没有外在症状 —— 错了不会抛异常、不会打错误日志，只是策略一直按错的敞口决策。
  */
class PositionOwnershipSpec extends munit.FunSuite:
  private val ex = Exchange.Binance
  private val sym = "BTCUSDT"
  private val inst = Instrument(ex, sym)
  private val paper: AccountId.Paper = AccountId.Paper(1)
  private val metas = Map[Symbol, SymbolMeta](sym -> SymbolMeta(ex, sym, 0.1, 0.001, 0.001, 1.0))
  private val instant = SimConfig(exchangeToStrategyDelayMs = 0, orderToExchangeDelayMs = 0, initialBalanceUsdt = 10_000.0)

  /** 挂一张买单；在成交回调与仓位回调里各记一次"此刻读到的仓位" */
  private class Recorder(atFill: ConcurrentLinkedQueue[Double], atPosition: ConcurrentLinkedQueue[Double]) extends Strategy:
    private var placed = false
    def orderTimeoutMs: Long = 0L
    def handlers = StrategyHandlers.empty
      .market(Topics.Bbo, inst) { (b, ctx, _) =>
        if placed then Vector.empty
        else
          placed = true
          ctx.place(
            Order("", ex, sym, Side.Long, OrderType.Limit(b.bidPrice, TimeInForce.GTC), 0.5, reduceOnly = false, ""),
            "maker",
          )
      }
      .own(Topics.Fill) { (_, ctx, _) => atFill.add(ctx.state.symbolState(sym).get.positionSize(ex).value); Vector.empty }
      .own(Topics.Position) { (_, ctx, _) => atPosition.add(ctx.state.symbolState(sym).get.positionSize(ex).value); Vector.empty }

  private def eventually(what: => String)(cond: => Boolean): Unit =
    val deadline = System.nanoTime() + 3_000_000_000L
    while System.nanoTime() < deadline && !cond do Thread.sleep(5)
    assert(cond, s"等待超时: $what")

  test("虚拟柜台上, 仓位紧邻在成交之前"):
    // **这不是契约, 是虚拟柜台能构造出来的性质** —— 撮合内核一次产出三条回报, 顺序在我们
    // 手里。真实柜台上成交明细走的是另一条渠道 (Bybit 干脆是另一条频道), 位置不承诺。
    // 所以策略**不该**在成交回调里读仓位; 这条用例只是钉住虚拟柜台不要无谓地偏离。
    // 真正的契约由"挂单消失的那一刻仓位已经更新"那条盯着。
    supervised:
      val bus = EventBus()
      val system = ActorSystem(bus)
      val atFill = ConcurrentLinkedQueue[Double]()
      val atPosition = ConcurrentLinkedQueue[Double]()
      system.spawn(PaperCounter(paper, ex, instant, metas))
      system.spawn(Executor.readyToTrade(Recorder(atFill, atPosition), paper))

      bus.publish(Event.at(Topics.Bbo, BBO(ex, sym, 100.0, Coin(1.0), 100.1, Coin(1.0), 1L), 1L))
      bus.publish(Event.at(Topics.Bbo, BBO(ex, sym, 98.0, Coin(1.0), 98.1, Coin(1.0), 2L), 2L)) // 越价成交

      eventually(s"应有成交回调, 实际 ${atFill.asScala.toVector}")(atFill.size == 1)
      assertEquals(atFill.asScala.toVector, Vector(0.5), "成交回调里读到的必须是成交后的仓位")

  test("不订阅成交的策略, 仓位照样是对的"):
    // 仓位归柜台算之后, Fill 对策略不再是必修课。这里的策略只声明行情,
    // 框架补齐持仓与订单回报 —— 敞口仍然准确。
    supervised:
      val bus = EventBus()
      val system = ActorSystem(bus)
      val seen = ConcurrentLinkedQueue[Double]()

      class NoFill extends Strategy:
        private var placed = false
        def orderTimeoutMs: Long = 0L
        def handlers = StrategyHandlers.empty.market(Topics.Bbo, inst) { (b, ctx, _) =>
          seen.add(ctx.state.symbolState(sym).get.positionSize(ex).value)
          if placed then Vector.empty
          else
            placed = true
            ctx.place(
              Order("", ex, sym, Side.Long, OrderType.Limit(b.bidPrice, TimeInForce.GTC), 0.5, reduceOnly = false, ""),
              "maker",
            )
        }

      val strategy = NoFill()
      system.spawn(PaperCounter(paper, ex, instant, metas))
      val executor = Executor.readyToTrade(strategy, paper)
      assert(
        !executor.subscription.accepts(
          Event.local(Topics.Fill, Fill(paper, ex, sym, Side.Long, 100.0, Coin(0.5), 0L))
        ),
        "没声明就不该补齐成交明细",
      )
      system.spawn(executor)

      bus.publish(Event.at(Topics.Bbo, BBO(ex, sym, 100.0, Coin(1.0), 100.1, Coin(1.0), 1L), 1L))
      bus.publish(Event.at(Topics.Bbo, BBO(ex, sym, 98.0, Coin(1.0), 98.1, Coin(1.0), 2L), 2L)) // 越价成交
      // 再来一条行情, 让策略在成交之后又读一次仓位
      eventually("应已成交")(seen.asScala.exists(_ == 0.5) || {
        bus.publish(Event.at(Topics.Bbo, BBO(ex, sym, 97.0, Coin(1.0), 97.1, Coin(1.0), 3L), 3L)); false
      })
      assert(seen.asScala.exists(_ == 0.5), s"没订阅成交也该看到仓位变化, 实际 ${seen.asScala.toVector}")

  // ==================== 真实柜台路径 ====================

  private class ManualFeed extends AccountFeed:
    @volatile private var sink: AccountReport => Unit = scala.compiletime.uninitialized
    override def exchange: Exchange = ex
    override def connect(s: AccountReport => Unit, fork: (=> Unit) => Unit): Unit = sink = s
    def emit(report: AccountReport): Unit = sink(report)

  private class AcceptingClient extends TradingClient:
    override def exchange: Exchange = ex
    override def placeOrder(order: ExchangeOrder) = Right("ex-1")
    override def fetchAllSymbolMetas() = Right(Vector(metas(sym)))
    override def cancelOrder(symbol: Symbol, ref: OrderRef) = Right(())
    override def fetchPendingOrders(symbol: Symbol) = Right(Vector.empty)
    override def setLeverage(symbol: Symbol, leverage: Int) = Right(())
    override def fetchAccountInfo() = Right(AccountInfo(AccountId.Live, ex, 10_000.0, 0.0))
    override def fetchPositions() = Right(Vector.empty)

  test("挂单消失的那一刻, 仓位已经更新 —— 否则策略会重复下单"):
    // 这是仓位归柜台、并且仓位排在订单状态之前的**全部理由**:
    //   OrderUpdate(Filled) 到达 -> 本地挂单消失
    //   若仓位还没更新 -> 策略看到"既没有单、仓位也不够" -> 再下一单
    // 中间状态没有任何症状, 只有重复开仓这个后果。
    supervised:
      val bus = EventBus()
      val system = ActorSystem(bus)
      val violations = ConcurrentLinkedQueue[String]()
      val intents = bus.subscribe(Set(Interest.All(OrderIntent)))

      val terminalSeen = ConcurrentLinkedQueue[String]()

      class Checker extends Strategy:
        private var placed = false
        def orderTimeoutMs: Long = 0L
        def handlers = StrategyHandlers.empty
          .market(Topics.Bbo, inst) { (b, ctx, _) =>
            if placed then Vector.empty
            else
              placed = true
              ctx.place(Order("", ex, sym, Side.Long, OrderType.Limit(b.bidPrice, TimeInForce.GTC), 0.5, reduceOnly = false, ""), "maker")
          }
          .own(Topics.OrderUpdate) { (u, ctx, _) =>
            if u.status.isTerminal then
              val view = ctx.state.symbolState(sym).get
              if view.pendingOrders.isEmpty && view.positionSize(ex).isZero then
                violations.add(s"挂单已消失但仓位仍为零 (status=${u.status})")
              terminalSeen.add(u.orderId)
            Vector.empty
          }

      val feed = ManualFeed()
      system.spawn(RestTradingGateway(AcceptingClient(), feed, AccountId.Live, metas))
      // 柜台在对齐之前忽略一切报告 —— 那时它还不知道自己管哪些标的
      val synced = bus.subscribe(Set(Interest.All(AccountSynced)))
      bus.publish(Event.local(AccountSync, AccountSyncRequest(AccountId.Live, ex, 1L, Set(sym))))
      synced.events.receive(): Unit
      synced.close()

      system.spawn(Executor.readyToTrade(Checker(), AccountId.Live))
      bus.publish(Event.at(Topics.Bbo, BBO(ex, sym, 100.0, Coin(1.0), 100.1, Coin(1.0), 1L), 1L))

      val clientOrderId = intents.events.receive().as(OrderIntent).get.outcome match
        case OutcomeEvent.PlaceOrders(orders, _) => orders.head.clientOrderId
        case other                               => fail(s"expected PlaceOrders, got $other")

      feed.emit(AccountReport.OrderStatusChanged(
        "ex-1", Some(clientOrderId), sym, Side.Long, OrderStatus.Filled,
        Price(100.0), Price(100.0), Coin(0.5), Coin(0.5), 2L,
      ))

      // 先等终态确实到了策略手里, 否则"没有违规"可能只是还没发生
      eventually("策略应已收到终态回报")(!terminalSeen.isEmpty)
      assertEquals(violations.asScala.toVector, Vector.empty[String])

  test("行情已在流动时装载策略, 它在对齐落地前不动作"):
    // 撤下一个策略再装回来、实盘与影子先后装载、扫描器先把标的订上了 —— 这些场景下
    // 行情早就在流。新装的策略 spawn 即收行情, 而初始仓位还在几次 REST 往返之外。
    // 那一刻它看到"仓位为零、没有挂单", 据此做的第一个决策就是重复开仓。
    supervised:
      val bus = EventBus()
      val system = ActorSystem(bus)
      val decisions = ConcurrentLinkedQueue[Double]()

      /** 每收到一条行情就记一次"此刻看到的仓位" */
      class Peeker extends Strategy:
        def orderTimeoutMs: Long = 0L
        def handlers = StrategyHandlers.empty.market(Topics.Bbo, inst) { (_, ctx, _) =>
          decisions.add(ctx.state.symbolState(sym).get.positionSize(ex).value)
          Vector.empty
        }

      // 柜台已持仓 0.7, 但对齐要等它把快照推过来
      class HoldingClient extends AcceptingClient:
        override def fetchPositions() =
          Right(Vector(Position(AccountId.Live, ex, sym, Coin(0.7), Price(100.0), 0.0)))
      system.spawn(RestTradingGateway(HoldingClient(), ManualFeed(), AccountId.Live, metas))

      // 行情先流起来 —— 模拟"这个标的早就有别的组件在看"
      bus.publish(Event.at(Topics.Bbo, BBO(ex, sym, 100.0, Coin(1.0), 100.1, Coin(1.0), 1L), 1L))

      val gated = Executor(Peeker(), AccountId.Live) // 闸门自带, 无需装配方设置
      system.spawn(gated)

      // 此刻策略已在总线上, 行情继续流 —— 但对齐还没发
      bus.publish(Event.at(Topics.Bbo, BBO(ex, sym, 101.0, Coin(1.0), 101.1, Coin(1.0), 2L), 2L))
      Thread.sleep(100)
      assert(decisions.isEmpty, s"对齐落地前不该动作, 却已决策 ${decisions.asScala.toVector}")

      // 对齐落地
      val synced = bus.subscribe(Set(Interest.All(AccountSynced)))
      bus.publish(Event.local(AccountSync, AccountSyncRequest(AccountId.Live, ex, 1L, Set(sym))))
      synced.events.receive(): Unit
      synced.close()

      // "引擎收到应答"不等于"策略已看到" —— 两个订阅者, 同一次 publish 谁先谁后没有承诺。
      // 所以这里等的是闸门本身, 而不是应答到了测试手里。
      eventually("闸门应已放行")(!gated.isGated)
      bus.publish(Event.at(Topics.Bbo, BBO(ex, sym, 102.0, Coin(1.0), 102.1, Coin(1.0), 3L), 3L))
      eventually("对齐之后应开始动作")(!decisions.isEmpty)
      assertEquals(decisions.asScala.toVector, Vector(0.7), "第一次决策就该看到真实仓位")
