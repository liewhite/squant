package hft.engine

import hft.actor.ActorSystem
import hft.domain.*
import hft.event.{AnyEvent, Event, EventBus, Interest, Topics}
import hft.exchange.ExchangeClient
import hft.sim.{PaperCounter, SimConfig}
import hft.state.StateManager
import hft.strategy.{OrderIntent, OutcomeEvent, Strategy}
import ox.supervised

import java.util.concurrent.ConcurrentLinkedQueue
import scala.jdk.CollectionConverters.*

/** 实盘与影子盘**并行**：同一份策略逻辑、同一份行情，两条出口互不知情。
  *
  * 这是"按影子盘表现起停实盘策略"的完整地基 —— 两个实例只有账户不同，
  * 实盘的单走 REST 出口、影子的单走虚拟柜台，谁也看不见谁。
  */
class LiveAndShadowSpec extends munit.FunSuite:
  private val ex = Exchange.Binance
  private val sym = "BTCUSDT"
  private val inst = Instrument(ex, sym)
  private val meta = SymbolMeta(ex, sym, tickSize = 0.1, sizeStep = 0.001, minOrderSize = 0.001, contractSize = 1.0)
  private val metas = Map((ex, sym) -> meta)
  private val paper = AccountId.Paper(1)
  private val instant = SimConfig(exchangeToStrategyDelayMs = 0, orderToExchangeDelayMs = 0, initialBalanceUsdt = 10_000.0)

  /** 记录下单的假交易所 —— 代表实盘出口的那一端 */
  private class RecordingClient(placed: ConcurrentLinkedQueue[Order]) extends ExchangeClient:
    override def exchange: Exchange = ex
    override def placeOrder(order: Order): Either[ExchangeError, OrderId] =
      placed.add(order); Right(s"live-${placed.size}")
    override def cancelOrder(symbol: Symbol, ref: OrderRef) = Right(())
    override def fetchAllSymbolMetas() = Right(Vector(meta))
    override def fetchPendingOrders(symbol: Symbol) = Right(Vector.empty)
    override def setLeverage(symbol: Symbol, leverage: Int) = Right(())
    override def fetchAccountInfo() = Right(AccountInfo(AccountId.Live, ex, 10_000.0, 0.0))
    override def fetchPositions() = Right(Vector.empty)

  /** 收到首个 BBO 就挂一张买单 —— 同一份逻辑给两个账户各跑一份 */
  private class OneShotMaker extends Strategy:
    private var placed = false
    def interests: Set[Interest] = Set(Interest.Keyed(Topics.Bbo, Set(inst)))
    def orderTimeoutMs: Long = 0L
    def onEvent(event: AnyEvent, state: StateManager): Vector[OutcomeEvent] =
      event.as(Topics.Bbo).filter(_ => !placed).map { b =>
        placed = true
        OutcomeEvent.PlaceOrders(
          Vector(Order("", ex, sym, Side.Long, OrderType.Limit(b.bidPrice - 1.0, TimeInForce.GTC), 0.5, reduceOnly = false, clientOrderId = "")),
          "maker",
        )
      }.toVector

  private def eventually(cond: => Boolean, what: String): Unit =
    val deadline = System.nanoTime() + 3_000_000_000L
    while System.nanoTime() < deadline && !cond do Thread.sleep(5)
    assert(cond, s"等待超时: $what")

  test("同一策略逻辑在实盘与影子账户并行, 各自下单、各自成交、互不串味"):
    supervised:
      val bus = EventBus()
      val system = ActorSystem(bus)
      val livePlaced = ConcurrentLinkedQueue[Order]()
      val fills = ConcurrentLinkedQueue[Fill]()
      val fillMailbox = bus.subscribe(Set(Interest.All(Topics.Fill)))
      ox.forkDiscard { while true do fillMailbox.events.receive().as(Topics.Fill).foreach(fills.add) }

      // 两条出口：真实交易所 (Live) 与虚拟柜台 (Paper(1))
      system.spawn(OutcomeProcessor(Map(ex -> RecordingClient(livePlaced)), dryRun = false, AccountId.Live))
      system.spawn(PaperCounter(paper, ex, instant))

      // 同一份策略逻辑, 两个账户各一个实例
      system.spawn(Executor(OneShotMaker(), metas, AccountId.Live))
      system.spawn(Executor(OneShotMaker(), metas, paper))

      // 一份行情喂给所有人 (行情无账户归属)
      bus.publish(Event.at(Topics.Bbo, BBO(ex, sym, 100.0, 1.0, 100.1, 1.0, 1L), 1L))

      // 实盘那张单进了真实出口
      eventually(livePlaced.size == 1, s"实盘应下一张单, 实际 ${livePlaced.asScala.toVector}")
      assertEqualsDouble(livePlaced.peek().orderType.asInstanceOf[OrderType.Limit].price, 99.0, 1e-9)

      // 影子那张单进了虚拟柜台：行情下穿后成交
      bus.publish(Event.at(Topics.Bbo, BBO(ex, sym, 98.0, 1.0, 98.1, 1.0, 2L), 2L))
      eventually(fills.asScala.exists(_.account == paper), "影子盘应成交")

      val got = fills.asScala.toVector
      assertEquals(got.count(_.account == paper), 1, "影子盘一笔成交")
      assertEquals(got.count(_.account == AccountId.Live), 0, "实盘的成交只能来自真实交易所推送, 不该由柜台合成")
      assertEquals(livePlaced.size, 1, "影子盘的单绝不能流到真实出口")
