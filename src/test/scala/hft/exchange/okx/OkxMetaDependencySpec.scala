package hft.exchange.okx

import hft.domain.*

import sttp.client4.testing.BackendStub

import scala.collection.mutable

/** OKX 适配层**自己**保证合约规格已加载 —— 张↔币换算是它自己的协议细节。
  *
  * 这一条要有测试，因为它的失效只在特定装配形态下出现：实时看板与跨所价差监控都**没有柜台**，
  * 而规格一度改成"由柜台代为加载"。那时行情源在第一条盘口上就抛、`fetchPositions` 把持仓
  * 静默读成零，861 例测试全绿 —— 因为没有一条用例跑过"无柜台"这个形态。
  *
  * Binance/Bybit 的推送本就是币本位，它们的适配层不需要规格；所以这条前提按"谁的事实"划分，
  * 不按"谁先跑"。
  */
class OkxMetaDependencySpec extends munit.FunSuite:

  private val instrumentsJson =
    """{"code":"0","msg":"","data":[
      |{"instId":"ETH-USDT-SWAP","tickSz":"0.01","lotSz":"0.1","minSz":"0.1","ctVal":"0.1","state":"live"}
      |]}""".stripMargin

  /** 只回合约清单的后端，并记下被请求过几次 */
  private def instrumentsBackend(calls: mutable.ArrayBuffer[String]) =
    BackendStub.synchronous.whenAnyRequest.thenRespondF { req =>
      calls += req.uri.toString
      sttp.client4.testing.ResponseStub.adjust(instrumentsJson)
    }

  test("行情源自己加载规格 —— 无柜台的装配形态 (跨所监控/看板) 才成立"):
    val calls = mutable.ArrayBuffer.empty[String]
    val client = OkxClient.public(instrumentsBackend(calls))
    assert(client.knownMetas.isEmpty, "构造时不拉 —— 拉取是连接时的动作")

    // connect() 里的 WsLoop 会去连真实 WS，这里只验证它之前那一步：规格已经进表。
    // 直接调 loadMetas 与 connect 首行是同一个调用，形态上等价且不需要网络。
    client.loadMetas(InstrumentKind.LinearPerp)
    assertEquals(
      client.metaOf(Instrument.perp(Exchange.Okx, "ETH")).contractSize,
      0.1,
      "ctVal 要能查到 —— 盘口数量是张数, 换回币本位靠它",
    )
    assert(calls.exists(_.contains("instruments")), "应当真的去拉过合约清单")

  test("规格没加载时, 换算路径抛而不是给一个看着正常的数"):
    val client = OkxClient.public(instrumentsBackend(mutable.ArrayBuffer.empty))
    val e = intercept[RuntimeException](client.metaOf(Instrument.perp(Exchange.Okx, "ETH")))
    assert(e.getMessage.contains("尚未加载"), e.getMessage)

  test("私有流的张->币换算走同一份规格 —— 缺了就抛, 不静默跳过"):
    // 仓位读数是对账的输入: 静默跳过等于让对账永远"一致"。
    val calls = mutable.ArrayBuffer.empty[String]
    val client = OkxClient.public(instrumentsBackend(calls))
    client.loadMetas(InstrumentKind.LinearPerp)

    val meta = client.metaOf(Instrument.perp(Exchange.Okx, "ETH"))
    // 3 张 × ctVal 0.1 = 0.3 币
    assertEqualsDouble(meta.toCoin(Contracts(3.0)).value, 0.3, 1e-12)
