package hft.exchange

import hft.domain.*
import hft.TestUnits.given

import java.util.concurrent.atomic.AtomicInteger

/** 合约规格表：按标的索引、可增量补充。
  *
  * 从前它是 `Map[Symbol, SymbolMeta]` 且进程内只拉一次。两处都不够用：
  *   - 键丢了品种：OKX 的 `ETH-USD-SWAP` 是 10 USD/张、`ETH-USDT-SWAP` 是 0.1 ETH/张，
  *     按 symbol 索引时后写入的那条静默覆盖前一条，之后所有张↔币换算都按错的乘数走；
  *   - 一次性快照装不下"下周才上市的合约"，而期权链每周滚动。
  */
class MetaCacheSpec extends munit.FunSuite:

  private def metaOf(instrument: Instrument, contractSize: Double = 1.0) =
    SymbolMeta(instrument.exchange, instrument.symbol, 0.1, 0.001, 0.001, contractSize, kind = instrument.kind)

  /** 按品种给不同的规格，并数一数每个品种被拉了几次。 */
  private class FakeClient(byKind: Map[InstrumentKind, Vector[SymbolMeta]]) extends ExchangeClient:
    override val metaTable: MetaTable = MetaTable()
    val calls: AtomicInteger = AtomicInteger(0)
    override def exchange: Exchange = Exchange.Okx
    override protected val metaFetchers = byKind.map((kind, metas) =>
      kind -> { () =>
        calls.incrementAndGet()
        Right(metas)
      }
    )

  private val perp = Instrument.perp(Exchange.Okx, "ETH")
  private val inverse = Instrument(Exchange.Okx, "ETH", InstrumentKind.InversePerp)
  private val option = Instrument.option(Exchange.Okx, "ETH-USD-250101-3000-C")

  test("同一 symbol 的不同品种各有各的规格 —— 按 symbol 索引时它们会互相覆盖"):
    val client = FakeClient(
      Map(
        InstrumentKind.LinearPerp -> Vector(metaOf(perp, contractSize = 0.1)),
        InstrumentKind.InversePerp -> Vector(metaOf(inverse, contractSize = 10.0)),
      )
    )
    client.ensureMetas(InstrumentKind.LinearPerp, "test")
    client.ensureMetas(InstrumentKind.InversePerp, "test")

    assertEqualsDouble(client.metaOf(perp).contractSize, 0.1, 1e-12)
    assertEqualsDouble(client.metaOf(inverse).contractSize, 10.0, 1e-12)

  test("补充的品种不会挤掉已加载的 —— 合并, 不是整表替换"):
    // 整表替换会在到期日当天把一个仍持有仓位的合约的规格抹掉, 于是记账路径上的 metaOf
    // 抛错、进程终止。规格是合约的静态事实, 到期不会改变它。
    val client = FakeClient(
      Map(
        InstrumentKind.LinearPerp -> Vector(metaOf(perp)),
        InstrumentKind.Option -> Vector(metaOf(option)),
      )
    )
    client.ensureMetas(InstrumentKind.LinearPerp, "test")
    client.ensureMetas(InstrumentKind.Option, "test")

    assertEquals(client.metaTable.known.keySet, Set(perp, option), "先加载的永续必须还在")

  test("没加载过的标的: 纯查表, 缺失即抛 —— 不在热路径上偷偷发一次网络请求"):
    val client = FakeClient(Map(InstrumentKind.LinearPerp -> Vector(metaOf(perp))))
    client.ensureMetas(InstrumentKind.LinearPerp, "test")

    val e = intercept[RuntimeException](client.metaOf(option))
    assert(e.getMessage.contains("尚未加载"), e.getMessage)
    assertEquals(client.calls.get(), 1, "metaOf 不该触发拉取 —— 它在每条订单回报的路径上")

  test("本所不支持的品种: Left 而不是空集"):
    // "这个所没有期权"与"我没接期权"是两件事, 空集把它们混成同一个读数。
    val client = FakeClient(Map(InstrumentKind.LinearPerp -> Vector(metaOf(perp))))
    assert(client.fetchMetas(InstrumentKind.Option).isLeft)

  test("本所不支持的品种: ensureMetas 给 Left, ensureMetasOrThrow 抛"):
    val client = FakeClient(Map(InstrumentKind.LinearPerp -> Vector(metaOf(perp))))
    assert(client.ensureMetas(InstrumentKind.Option, "某某流").isLeft, "Either 形态: 与调用方自己的失败同一条通道")
    val e = intercept[IllegalStateException](client.ensureMetasOrThrow(InstrumentKind.Option, "某某流"))
    assert(e.getMessage.contains("某某流"), "错误里要说得出是谁要的这份规格")

  test("已经拉过的品种不再打 REST —— 一个进程启动一度打三次同一个端点"):
    // 两条 OKX 流 + 柜台对齐各自无条件加载, 而"已有的不再拉"只写在柜台那一处。
    val client = FakeClient(Map(InstrumentKind.LinearPerp -> Vector(metaOf(perp))))
    client.ensureMetas(InstrumentKind.LinearPerp, "行情源")
    client.ensureMetas(InstrumentKind.LinearPerp, "私有流")
    client.ensureMetas(InstrumentKind.LinearPerp, "柜台")
    assertEquals(client.calls.get(), 1, "只该拉一次")
    assertEquals(client.metaTable.known.size, 1)

  test("拉到空集也算拉过 —— 否则一张合约都没上市的品种会被反复拉"):
    // "拉过"记的是端点访问过了, 不是"表里有条目"。按后者判断的话, 每个调用点都会为
    // 一个确实没有合约的品种再打一次 REST, 永远打下去。
    val client = FakeClient(Map(InstrumentKind.Option -> Vector.empty))
    client.ensureMetas(InstrumentKind.Option, "第一次")
    client.ensureMetas(InstrumentKind.Option, "第二次")
    assertEquals(client.calls.get(), 1)

  test("装饰器与被装饰者是同一张表 —— 不是各自一张"):
    // DryRunClient 从前必然自带一张接不上去的空表: 柜台对齐时装进 dry-run 那张,
    // 而 delegate 的 REST 响应侧读自己那张。
    val delegate = new FakeClient(Map(InstrumentKind.LinearPerp -> Vector(metaOf(perp)))) with TradingClient:
      override def placeOrder(order: ExchangeOrder) = Right("x")
      override def cancelOrder(instrument: Instrument, ref: OrderRef) = Right(())
      override def fetchPendingOrders(instrument: Instrument) = Right(Vector.empty)
      override def fetchAccountInfo() = Right(AccountInfo(AccountId.Live, Exchange.Okx, 0.0))
      override def fetchWallet() = Right(Map.empty)
      override def fetchPositions() = Right(Vector.empty)
    val dryRun = DryRunClient(delegate)

    dryRun.ensureMetas(InstrumentKind.LinearPerp, "柜台对齐")
    assertEqualsDouble(delegate.metaOf(perp).contractSize, 1.0, 1e-12, "delegate 也该看得到")
    assertEquals(delegate.calls.get(), 1, "两张表的话这里会是 0 —— 装进了 dry-run 自己那张")
