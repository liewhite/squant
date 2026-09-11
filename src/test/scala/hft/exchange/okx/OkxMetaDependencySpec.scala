package hft.exchange.okx

import hft.domain.*

import sttp.client4.testing.BackendStub
import hft.exchange.MetaTable

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
    // 直接调 ensureMetas 与 connect 首行是同一个调用，形态上等价且不需要网络。
    client.ensureMetas(InstrumentKind.LinearPerp, "OKX 行情源")
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
    client.ensureMetas(InstrumentKind.LinearPerp, "OKX 私有流")

    val meta = client.metaOf(Instrument.perp(Exchange.Okx, "ETH"))
    // 3 张 × ctVal 0.1 = 0.3 币
    assertEqualsDouble(meta.toCoin(Contracts(3.0)).value, 0.3, 1e-12)

  // ==================== 客户端自己的 REST 响应侧 ====================

  /** 合约清单 + 持仓, 按 path 分流; 记下每个 path 被打了几次。 */
  private def accountBackend(calls: mutable.ArrayBuffer[String]) =
    val positionsJson =
      """{"code":"0","msg":"","data":[
        |{"instId":"ETH-USDT-SWAP","pos":"3","avgPx":"2000","upl":"1"}
        |]}""".stripMargin
    BackendStub.synchronous.whenAnyRequest.thenRespondF { req =>
      val path = req.uri.path.mkString("/")
      calls += path
      val body = if path.contains("instruments") then instrumentsJson else positionsJson
      sttp.client4.testing.ResponseStub.adjust(body)
    }

  private val credentials = OkxCredentials("k", "s", "p")

  test("fetchPositions 自己保证规格已加载 —— 无柜台看板给行情源和账户轮询建的是两个实例"):
    // 这是 LiveDashboardLauncher 的形态: OkxClient.trading 给 AccountMonitor,
    // OkxClient.public 给 OkxMarketFeed。表在具体客户端上, 所以行情源加载的是**另一份**。
    // 少了自保证这一步, 账户上只要有一张持仓, 第一轮轮询就抛"尚未加载", 看板进程终止。
    val calls = mutable.ArrayBuffer.empty[String]
    val trading = OkxClient.trading(accountBackend(calls), credentials)
    assert(trading.knownMetas.isEmpty, "没有任何人替它加载过")

    val positions = trading.fetchPositions().fold(e => fail(s"应当报得出持仓: ${e.message}"), identity)
    assertEquals(positions.map(_.symbol), Vector("ETH"))
    // 3 张 x ctVal 0.1 = 0.3 币 —— 换算真的做了, 不是原样把张数当币报出来
    assertEqualsDouble(positions.head.size.value, 0.3, 1e-12)
    assert(calls.exists(_.contains("instruments")), "它该自己去拉规格, 而不是指望别人先加载")

  test("同一实例的第二次查询不再重复拉规格"):
    val calls = mutable.ArrayBuffer.empty[String]
    val trading = OkxClient.trading(accountBackend(calls), credentials)
    trading.fetchPositions()
    trading.fetchPositions()
    assertEquals(calls.count(_.contains("instruments")), 1, "规格是静态事实, 拉一次就够")

  test("规格表可以被两个客户端实例共享 —— 装配方想省掉那次 REST 时"):
    // 自保证解决的是正确性; 想连那一次 REST 都省掉, 把同一个实例传给两边即可
    // (OkxMarketFeed 收的是 OkxPublicClient, 交易客户端是它的子类)。
    val calls = mutable.ArrayBuffer.empty[String]
    val shared = OkxClient.trading(accountBackend(calls), credentials)
    shared.ensureMetas(InstrumentKind.LinearPerp, "行情源")
    shared.fetchPositions()
    assertEquals(calls.count(_.contains("instruments")), 1)
