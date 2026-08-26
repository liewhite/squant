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

  test("策略在成交回调里读到的是成交**后**的仓位"):
    // 仓位快照必须排在成交之前发布。顺序反了策略读到的就是成交前的数,
    // 对冲量从此一直差一笔 —— 而这不会有任何症状。
    supervised:
      val bus = EventBus()
      val system = ActorSystem(bus)
      val atFill = ConcurrentLinkedQueue[Double]()
      val atPosition = ConcurrentLinkedQueue[Double]()
      system.spawn(PaperCounter(paper, ex, instant, metas))
      system.spawn(Executor(Recorder(atFill, atPosition), paper))

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
      val executor = Executor(strategy, paper)
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

      system.spawn(Executor(Checker(), AccountId.Live))
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
