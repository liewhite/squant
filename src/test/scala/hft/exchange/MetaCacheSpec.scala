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
    val calls: AtomicInteger = AtomicInteger(0)
    override def exchange: Exchange = Exchange.Okx
    override def fetchMetas(kind: InstrumentKind): Either[ExchangeError, Vector[SymbolMeta]] =
      calls.incrementAndGet()
      byKind.get(kind).toRight(ExchangeError.Rejected("unsupported", s"没接 $kind"))

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
    client.loadMetas(InstrumentKind.LinearPerp)
    client.loadMetas(InstrumentKind.InversePerp)

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
    client.loadMetas(InstrumentKind.LinearPerp)
    client.loadMetas(InstrumentKind.Option)

    assertEquals(client.knownMetas.keySet, Set(perp, option), "先加载的永续必须还在")

  test("没加载过的标的: 纯查表, 缺失即抛 —— 不在热路径上偷偷发一次网络请求"):
    val client = FakeClient(Map(InstrumentKind.LinearPerp -> Vector(metaOf(perp))))
    client.loadMetas(InstrumentKind.LinearPerp)

    val e = intercept[RuntimeException](client.metaOf(option))
    assert(e.getMessage.contains("尚未加载"), e.getMessage)
    assertEquals(client.calls.get(), 1, "metaOf 不该触发拉取 —— 它在每条订单回报的路径上")

  test("本所不支持的品种: Left 而不是空集"):
    // "这个所没有期权"与"我没接期权"是两件事, 空集把它们混成同一个读数。
    val client = FakeClient(Map(InstrumentKind.LinearPerp -> Vector(metaOf(perp))))
    assert(client.loadMetas(InstrumentKind.Option).isLeft)

  test("重复加载是幂等的"):
    val client = FakeClient(Map(InstrumentKind.LinearPerp -> Vector(metaOf(perp))))
    client.loadMetas(InstrumentKind.LinearPerp)
    client.loadMetas(InstrumentKind.LinearPerp)
    assertEquals(client.knownMetas.size, 1)
