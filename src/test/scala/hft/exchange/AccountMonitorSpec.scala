package hft.exchange

import hft.domain.*
import hft.engine.Engine
import hft.event.{AnyEvent, Interest, Topics}
import hft.TestUnits.given
import ox.supervised

import java.util.concurrent.ConcurrentLinkedQueue
import scala.jdk.CollectionConverters.*

/** 只读账户监控的契约: 账上有什么就报什么 (**不要求合约规格**)、点名而空仓的显式推零仓、
  * 启动后新开的仓位下一拍就出现、拉不到就抛。 */
class AccountMonitorSpec extends munit.FunSuite:
  private val ex = Exchange.Binance

  /** 只读客户端替身。`symbolMetas` 里**故意只有 BTCUSDT** —— 用来钉住"账上持有一个没有规格的
    * 标的 (如币安的股票永续 AAPLUSDT) 时, 监控照样报得出来"。柜台走不通的正是这条路径。 */
  private class ReadOnlyClient(@volatile var positions: Vector[Position]) extends TradingClient:
    override val metaTable: MetaTable = MetaTable()
    private val meta = SymbolMeta(ex, "BTCUSDT", tickSize = 0.1, sizeStep = 0.001, minOrderSize = 0.001, contractSize = 1.0)
    val placed = ConcurrentLinkedQueue[String]()
    override def exchange: Exchange = ex
    override def fetchMetas(kind: InstrumentKind) = Right(Vector(meta))
    override def placeOrder(order: ExchangeOrder) = { placed.add("placed"): Unit; Right("should-never-happen") }
    override def cancelOrder(instrument: Instrument, ref: OrderRef) = Right(())
    override def fetchPendingOrders(instrument: Instrument) = Right(Vector.empty)
    override def fetchAccountInfo() = Right(AccountInfo(AccountId.Live, ex, 12_345.0))
    override def fetchWallet() = Right(Map("USDT" -> 12_000.0))
    override def fetchPositions() = Right(positions)

  private def pos(symbol: Symbol, size: Double) = Position(AccountId.Live, ex, symbol, Coin(size))

  private def eventually(what: String)(cond: => Boolean): Unit =
    val deadline = System.currentTimeMillis() + 3000
    while !cond && System.currentTimeMillis() < deadline do Thread.sleep(10)
    assert(cond, s"等待超时: $what")

  private def withMonitor(client: ReadOnlyClient, symbols: Set[Symbol] = Set.empty)(
      body: ConcurrentLinkedQueue[AnyEvent] => Unit
  ): Unit =
    supervised:
      Engine.run(plugins = Vector.empty) { engine =>
        val seen = ConcurrentLinkedQueue[AnyEvent]()
        val mailbox = engine.subscribe(Set(
          Interest.All(Topics.Position),
          Interest.All(Topics.AccountInfo),
          Interest.All(Topics.Wallet),
        ))
        ox.forkUnsupervised(mailbox.events.foreach(seen.add(_): Unit)): Unit
        engine.install(AccountMonitor(client, AccountId.Live, symbols, pollMs = AccountMonitor.MinPollMs))
        body(seen)
        engine.requestShutdown("测试结束")
      }

  private def positionsOf(seen: ConcurrentLinkedQueue[AnyEvent]): Map[Symbol, Double] =
    seen.asScala.toVector.flatMap(_.as(Topics.Position)).map(p => p.symbol -> p.size.value).toMap

  test("账上有什么就报什么 —— **不要求合约规格**"):
    // 柜台走不通的正是这条: 它在对齐入口 `symbols.foreach(metaOf)` 要求每个标的都有规格,
    // 而币安的 symbolMetas 只收 TRADING+PERPETUAL。账上一张股票永续 (AAPLUSDT) 或季度交割
    // 合约就能让整个只读进程启动失败, 报的还是"无法发单"。监控不发单, 所以不问规格。
    val client = ReadOnlyClient(Vector(pos("BTCUSDT", 0.5), pos("AAPLUSDT", 3.0)))
    withMonitor(client) { seen =>
      eventually("两个仓位都报出来")(positionsOf(seen).keySet == Set("BTCUSDT", "AAPLUSDT"))
      assertEquals(positionsOf(seen)("AAPLUSDT"), 3.0, "没有规格的标的照样如实报出")
    }

  test("点名而空仓的显式推零仓 —— '空仓'与'这条读数还没来'必须分得开"):
    val client = ReadOnlyClient(Vector(pos("BTCUSDT", 0.5)))
    withMonitor(client, symbols = Set("BTCUSDT", "ETHUSDT")) { seen =>
      eventually("点名的两个都有读数")(positionsOf(seen).keySet == Set("BTCUSDT", "ETHUSDT"))
      assertEquals(positionsOf(seen)("ETHUSDT"), 0.0, "点名而空仓 -> 显式零仓")
    }

  test("启动后新开的仓位下一拍就出现 —— 柜台那条路它永远不会出现"):
    // 柜台只管对齐过的标的 (PositionBook.manages): 请求里没列的标的其回报一律在入口被丢掉,
    // 于是启动之后在手机上新开的仓位永远不显示, 而页面看起来是完整的 —— 假账比没有账更坏。
    val client = ReadOnlyClient(Vector(pos("BTCUSDT", 0.5)))
    withMonitor(client) { seen =>
      eventually("第一拍")(positionsOf(seen).contains("BTCUSDT"))
      client.positions = Vector(pos("BTCUSDT", 0.5), pos("SOLUSDT", 10.0))
      eventually("新开的仓位出现")(positionsOf(seen).contains("SOLUSDT"))
      assertEquals(positionsOf(seen)("SOLUSDT"), 10.0)
    }

  test("净值与全量钱包每拍都发 —— 页面上的年龄因此在一个间隔内摆动, 而不是一直涨"):
    val client = ReadOnlyClient(Vector.empty)
    withMonitor(client) { seen =>
      eventually("净值与钱包都到过") {
        val evs = seen.asScala.toVector
        evs.exists(_.as(Topics.AccountInfo).isDefined) && evs.exists(_.as(Topics.Wallet).isDefined)
      }
      val wallet = seen.asScala.toVector.flatMap(_.as(Topics.Wallet)).head
      assertEquals(wallet.balances, Map("USDT" -> 12_000.0))
      assertEquals(seen.asScala.toVector.flatMap(_.as(Topics.AccountInfo)).head.equity, 12_345.0)
    }

  test("一次下单都不会发 —— 没有命令处理能力是结构保证"):
    // commandHandlers 为空: 下单指令在这个进程里连路由都没有, 不依赖"这里没装策略所以没人会发"。
    val client = ReadOnlyClient(Vector(pos("BTCUSDT", 0.5)))
    withMonitor(client) { seen =>
      eventually("跑过至少一拍")(positionsOf(seen).nonEmpty)
      assert(client.placed.isEmpty, "只读监控不该调用 placeOrder")
    }
    assertEquals(AccountMonitor(client, AccountId.Live, Set.empty, 1000).commandHandlers, Set.empty)

  test("轮询太快即拒 —— 账户接口有权重限制"):
    val client = ReadOnlyClient(Vector.empty)
    intercept[IllegalArgumentException](AccountMonitor(client, AccountId.Live, Set.empty, pollMs = 10))
