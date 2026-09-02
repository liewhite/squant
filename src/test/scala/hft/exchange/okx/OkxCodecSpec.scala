package hft.exchange.okx

import com.github.plokhotnyuk.jsoniter_scala.core.*
import hft.domain.*

import OkxCodec.*
import OkxCodec.given

/** OKX 报文解析单测：symbol 转换、状态映射、字段名/单位/可空字段——最易回归的纯数据层。
  * (实盘 WS/REST 链路无网络测试，此处用真实报文样本断言保真度。)
  */
class OkxCodecSpec extends munit.FunSuite:

  test("symbol <-> instId 转换"):
    assertEquals(toOkx("BTC", "USDT"), "BTC-USDT-SWAP")
    assertEquals(toOkxIndex("BTC", "USDT"), "BTC-USDT")
    assertEquals(fromOkx("BTC-USDT-SWAP", "USDT"), Some("BTC"))
    // 计价币不符一律不认。框架的 Symbol 只有基础币, 认了的话币本位的 ETH-USD-SWAP
    // 会和 ETH-USDT-SWAP 撞成同一个 "ETH" —— 而私有流与 /account/positions 都是全量的。
    assertEquals(fromOkx("ETH-USD-SWAP", "USDT"), None, "币本位永续不归本柜台管")
    assertEquals(fromOkx("ETH-USDC-SWAP", "USDT"), None, "USDC 永续同理")
    assertEquals(fromOkx("ETH-USD-SWAP", "USD"), Some("ETH"), "配了币本位就该认")
    assertEquals(fromOkxIndex("BTC-USDT"), Some("BTC"))
    // 非永续 / 非指数格式返回 None
    assertEquals(fromOkx("BTC-USDT", "USDT"), None)
    assertEquals(fromOkx("BTC-USDT-FUTURES", "USDT"), None)
    assertEquals(fromOkxIndex("BTC-USDT-SWAP"), None)

  test("订单状态映射"):
    assertEquals(mapOrderState("live", Coin(0.0)), OrderStatus.Pending)
    assertEquals(mapOrderState("partially_filled", Coin(3.0)), OrderStatus.PartiallyFilled(Coin(3.0)))
    assertEquals(mapOrderState("filled", Coin(10.0)), OrderStatus.Filled)
    assertEquals(mapOrderState("canceled", Coin(0.0)), OrderStatus.Cancelled)
    assertEquals(mapOrderState("cancelled", Coin(0.0)), OrderStatus.Cancelled)

  test("mmp_canceled 是文档内的终态撤单 (做市商保护), 不是未知状态"):
    // 启动对齐会拉到历史单, 把文档里有的状态当"未知"抛出等于见到它就崩
    assertEquals(mapOrderState("mmp_canceled", Coin(0.0)), OrderStatus.Cancelled)

  test("文档之外的订单状态 -> 抛错, 不归成终态"):
    val e = intercept[IllegalStateException](mapOrderState("something_new", Coin(0.0)))
    assert(e.getMessage.contains("文档之外的 OKX 订单状态"), e.getMessage)

  test("asDouble / asDoubleOrZero"):
    assertEquals("42000.5".asDouble, 42000.5)
    assertEquals("".asDoubleOrZero, 0.0)
    assertEquals("1.5".asDoubleOrZero, 1.5)
    intercept[IllegalStateException]("abc".asDouble)

  test("解析 bbo-tbt 推送 (取首档, 数量为张)"):
    val json =
      """{"arg":{"channel":"bbo-tbt","instId":"BTC-USDT-SWAP"},"data":[{"asks":[["42001.5","3","0","1"]],"bids":[["42000.5","5","0","2"]],"ts":"1700000000123","seqId":123}]}"""
    val push = readFromString[WsPush[BboData]](json)
    assertEquals(push.arg.instId, "BTC-USDT-SWAP")
    val d = push.data.head
    assertEquals(d.asks.head.head.asDouble, 42001.5)
    assertEquals(d.asks.head(1).asDouble, 3.0)
    assertEquals(d.bids.head.head.asDouble, 42000.5)
    assertEquals(d.ts.toLong, 1700000000123L)

  test("解析 account/greeks REST 响应 (deltaBS 等字段名 + 多币种)"):
    val json =
      """{"code":"0","msg":"","data":[{"ccy":"BTC","deltaBS":"0.5","gammaBS":"0.001","thetaBS":"-12.3","vegaBS":"4.5","ts":"1700000000000"},{"ccy":"ETH","deltaBS":"-2.0","gammaBS":"0.02","thetaBS":"1.1","vegaBS":"0.3","ts":"1700000000001"}]}"""
    val resp = readFromString[GreeksResp](json)
    assertEquals(resp.code, "0")
    assertEquals(resp.data.size, 2)
    val btc = resp.data.head
    assertEquals(btc.ccy, "BTC")
    assertEquals(btc.deltaBS.asDouble, 0.5)
    assertEquals(btc.gammaBS.asDouble, 0.001)
    assertEquals(btc.thetaBS.asDouble, -12.3)
    assertEquals(btc.vegaBS.asDouble, 4.5)
    assertEquals(btc.ts.toLong, 1700000000000L)

  test("解析 orders 推送 (市价单 px 为空, 部分成交)"):
    val json =
      """{"arg":{"channel":"orders","instType":"SWAP"},"data":[{"instId":"BTC-USDT-SWAP","ordId":"o1","clOrdId":"c1","side":"buy","state":"partially_filled","px":"","sz":"10","fillSz":"3","fillPx":"42000","accFillSz":"3"}]}"""
    val d = readFromString[WsPush[OrderPushData]](json).data.head
    assertEquals(d.ordId, "o1")
    assertEquals(d.clOrdId, "c1")
    assertEquals(d.side, "buy")
    assertEquals(d.px.asDoubleOrZero, 0.0) // 市价单空 px
    assertEquals(d.sz.asDouble, 10.0)
    assertEquals(d.fillSz.asDouble, 3.0)
    assertEquals(d.accFillSz.asDouble, 3.0)
    assertEquals(mapOrderState(d.state, Coin(d.accFillSz.asDouble)), OrderStatus.PartiallyFilled(Coin(3.0)))

  test("解析 positions 推送 (空仓 avgPx/upl 为空字符串)"):
    val json =
      """{"arg":{"channel":"positions","instType":"SWAP"},"data":[{"instId":"BTC-USDT-SWAP","pos":"5","avgPx":"41000","upl":"123.4"},{"instId":"ETH-USDT-SWAP","pos":"0","avgPx":"","upl":""}]}"""
    val data = readFromString[WsPush[PositionData]](json).data
    assertEquals(data.head.pos.asDouble, 5.0)
    assertEquals(data.head.avgPx.asDoubleOrZero, 41000.0)
    assertEquals(data(1).pos.asDouble, 0.0)
    assertEquals(data(1).avgPx.asDoubleOrZero, 0.0) // 空仓不报错
    assertEquals(data(1).upl.asDoubleOrZero, 0.0)

  test("解析 account 推送 (totalEq/notionalUsd + 各币种 cashBal)"):
    val json =
      """{"arg":{"channel":"account"},"data":[{"uTime":"1700000000000","totalEq":"50000.5","notionalUsd":"12345.6","details":[{"ccy":"BTC","cashBal":"2.5"},{"ccy":"USDT","cashBal":"10000"}]}]}"""
    val d = readFromString[WsPush[AccountData]](json).data.head
    assertEquals(d.totalEq.asDouble, 50000.5)
    assertEquals(d.notionalUsd.asDouble, 12345.6)
    assertEquals(d.details.map(x => x.ccy -> x.cashBal.asDouble).toMap, Map("BTC" -> 2.5, "USDT" -> 10000.0))

  test("控制消息 / 错误事件经 OkxEnvelope 探测"):
    val sub = readFromString[OkxEnvelope]("""{"event":"subscribe","arg":{"channel":"bbo-tbt","instId":"BTC-USDT-SWAP"}}""")
    assertEquals(sub.event, "subscribe")
    assertEquals(sub.arg.channel, "bbo-tbt")
    val err = readFromString[OkxEnvelope]("""{"event":"error","code":"60012","msg":"Invalid request"}""")
    assertEquals(err.event, "error")
    assertEquals(err.code, "60012")
    // 数据推送无 event 字段 -> 空串, 据 arg.channel 分派
    val push = readFromString[OkxEnvelope]("""{"arg":{"channel":"bbo-tbt","instId":"BTC-USDT-SWAP"},"data":[]}""")
    assertEquals(push.event, "")
    assertEquals(push.arg.channel, "bbo-tbt")

  test("reduceOnly 缺失与 false 分得开 —— OKX 用字符串, 空串即缺失"):
    // 把缺失当成 false 就是给缺失字段填默认值; 而策略拿它给 resting 单分槽。
    assertEquals(OkxCodec.booleanFrom("true", "reduceOnly"), true)
    assertEquals(OkxCodec.booleanFrom("false", "reduceOnly"), false)
    val missing = intercept[IllegalStateException](OkxCodec.booleanFrom("", "reduceOnly"))
    assert(missing.getMessage.contains("缺字段"), missing.getMessage)
    val bad = intercept[IllegalStateException](OkxCodec.booleanFrom("1", "reduceOnly"))
    assert(bad.getMessage.contains("不是布尔字符串"), bad.getMessage)
