package hft.engine

import hft.TestSim

import hft.actor.ActorSystem
import hft.domain.*
import hft.event.{AnyEvent, Event, EventBus, Interest, Topics}
import hft.exchange.{AccountFeed, AccountReport, RestTradingGateway, TradingClient}
import hft.sim.{PaperCounter, SimConfig}
import hft.state.StateManager
import hft.event.Commands.{AccountSync, AccountSyncRequest, AccountSynced, OrderIntent, OutcomeEvent}
import hft.strategy.{Strategy, StrategyHandlers}
import ox.supervised

import java.util.concurrent.ConcurrentLinkedQueue
import scala.jdk.CollectionConverters.*
import hft.TestUnits.given
import hft.exchange.MetaTable

/** 实盘与影子盘**并行**：同一份策略逻辑、同一份行情，两条出口互不知情。
  *
  * 这是"按影子盘表现起停实盘策略"的完整地基 —— 两个实例只有账户不同，
  * 实盘的单走 REST 出口、影子的单走虚拟柜台，谁也看不见谁。
  */
class LiveAndShadowSpec extends munit.FunSuite:
  private val ex = Exchange.Binance
  private val sym = "BTCUSDT"
  private val inst = Instrument.perp(ex, sym)
  private val meta = SymbolMeta(ex, sym, tickSize = 0.1, sizeStep = 0.001, minOrderSize = 0.001, contractSize = 1.0)
  private val metas = Map(Instrument.perp(ex, sym) -> meta)
  private val paper: AccountId.Paper = AccountId.Paper(1)
  private val instant = TestSim.noFees.copy(exchangeToStrategyDelayMs = 0, orderToExchangeDelayMs = 0, initialBalanceUsdt = 10_000.0)

  /** 不推送任何东西的汇报面 —— 实盘成交只能来自真实推送, 本测试不造 */
  private object SilentFeed extends AccountFeed:
    override def exchange: Exchange = ex
    override def connect(sink: AccountReport => Unit, fork: (=> Unit) => Unit, sleepUnlessStopped: Long => Boolean): Unit = ()

  /** 记录下单的假交易所 —— 代表实盘柜台的那一端 */
  private class RecordingClient(placed: ConcurrentLinkedQueue[ExchangeOrder]) extends TradingClient:
    override val metaTable: MetaTable = MetaTable()
    override val supportedKinds: Set[InstrumentKind] = Set(InstrumentKind.LinearPerp)
    override def exchange: Exchange = ex
    override def placeOrder(order: ExchangeOrder): Either[ExchangeError, OrderId] =
      placed.add(order); Right(s"live-${placed.size}")
    override def cancelOrder(instrument: Instrument, ref: OrderRef) = Right(())
    override protected def fetchSupportedMetas(kind: InstrumentKind) = Right(Vector(meta))
    override def fetchPendingOrders(instrument: Instrument) = Right(Vector.empty)
    override def fetchAccountInfo() = Right(AccountInfo(AccountId.Live, ex, 10_000.0))
    override def fetchWallet() = Right(Map("USDT" -> 10_000.0))
    override def fetchPositions() = Right(Vector.empty)

  /** 收到首个 BBO 就挂一张买单 —— 同一份逻辑给两个账户各跑一份 */
  private class OneShotMaker extends Strategy:
    private var placed = false
    def handlers = StrategyHandlers.empty.market(Topics.Bbo, inst) { (b, ctx, _) =>
      if placed then Vector.empty
      else
        placed = true
        ctx.place(
          Order("", ex, sym, Side.Long, OrderType.Limit(b.bidPrice - 1.0, TimeInForce.GTC), 0.5, reduceOnly = false, clientOrderId = ""),
          "maker",
        )
    }

  private def eventually(cond: => Boolean, what: String): Unit =
    val deadline = System.nanoTime() + 3_000_000_000L
    while System.nanoTime() < deadline && !cond do Thread.sleep(5)
    assert(cond, s"等待超时: $what")

  test("同一策略逻辑在实盘与影子账户并行, 各自下单、各自成交、互不串味"):
    supervised:
      val bus = EventBus()
      val system = ActorSystem(bus)
      val livePlaced = ConcurrentLinkedQueue[ExchangeOrder]()
      val fills = ConcurrentLinkedQueue[Fill]()
      val fillMailbox = bus.subscribe(Set(Interest.All(Topics.Fill)))
      ox.forkDiscard { while true do fillMailbox.events.receive().as(Topics.Fill).foreach(fills.add) }

      // 两个柜台：真实交易所 (Live) 与虚拟柜台 (Paper(1))
      system.spawn(RestTradingGateway(RecordingClient(livePlaced), SilentFeed, AccountId.Live))
      system.spawn(PaperCounter(paper, ex, instant, metas))

      // 实盘柜台的合约规格在启动对齐入口加载 (见 RestTradingGateway.syncSnapshot) ——
      // 本用例用 Executor.readyToTrade 绕过了策略侧的对齐闸门, 柜台这一侧仍要走真实链路。
      val synced = bus.subscribe(Set(Interest.All(AccountSynced)))
      bus.publish(Event.local(AccountSync, AccountSyncRequest(AccountId.Live, ex, 1L, Set(inst))))
      synced.events.receive(): Unit
      synced.close()

      // 同一份策略逻辑, 两个账户各一个实例
      system.spawn(Executor.readyToTrade(OneShotMaker(), AccountId.Live))
      system.spawn(Executor.readyToTrade(OneShotMaker(), paper))

      // 一份行情喂给所有人 (行情无账户归属)
      bus.publish(Event.at(Topics.Bbo, BBO(ex, sym, 100.0, Coin(1.0), 100.1, Coin(1.0), 1L), 1L))

      // 实盘那张单进了真实出口
      eventually(livePlaced.size == 1, s"实盘应下一张单, 实际 ${livePlaced.asScala.toVector}")
      assertEqualsDouble(livePlaced.peek().orderType.asInstanceOf[OrderType.Limit].price.value, 99.0, 1e-9)

      // 影子那张单进了虚拟柜台：行情下穿后成交
      bus.publish(Event.at(Topics.Bbo, BBO(ex, sym, 98.0, Coin(1.0), 98.1, Coin(1.0), 2L), 2L))
      eventually(fills.asScala.exists(_.account == paper), "影子盘应成交")

      val got = fills.asScala.toVector
      assertEquals(got.count(_.account == paper), 1, "影子盘一笔成交")
      assertEquals(got.count(_.account == AccountId.Live), 0, "实盘的成交只能来自真实交易所推送, 不该由柜台合成")
      assertEquals(livePlaced.size, 1, "影子盘的单绝不能流到真实出口")
