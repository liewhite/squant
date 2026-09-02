package hft.exchange.bybit

import com.github.plokhotnyuk.jsoniter_scala.core.*
import hft.domain.*

import BybitCodec.*
import BybitCodec.given

/** Bybit 报文解析单测：symbol/状态映射、字段名/单位/可空字段、snapshot vs delta——最易回归的纯数据层。
  * (实盘 WS/REST 链路无网络测试，此处用真实报文样本断言保真度。)
  */
class BybitCodecSpec extends munit.FunSuite:

  test("symbol 恒等转换 (非空校验)"):
    assertEquals(fromBybit("BTCUSDT"), Some("BTCUSDT"))
    assertEquals(fromBybit(""), None)

  test("订单状态映射"):
    assertEquals(mapOrderStatus("New", Coin(0.0)), OrderStatus.Pending)
    assertEquals(mapOrderStatus("PartiallyFilled", Coin(0.003)), OrderStatus.PartiallyFilled(Coin(0.003)))
    assertEquals(mapOrderStatus("Filled", Coin(0.01)), OrderStatus.Filled)
    assertEquals(mapOrderStatus("Cancelled", Coin(0.0)), OrderStatus.Cancelled)
    assertEquals(mapOrderStatus("PartiallyFilledCanceled", Coin(0.0)), OrderStatus.Cancelled)
    assertEquals(mapOrderStatus("Deactivated", Coin(0.0)), OrderStatus.Cancelled)
    assert(mapOrderStatus("Rejected", Coin(0.0)).isInstanceOf[OrderStatus.Rejected])

  test("条件单状态 Untriggered/Triggered 是文档内的合法状态, 且不是终态"):
    // 本框架只下普通限价/市价单, 所以它们只出现在别人的单上 (手工止损、交易所 TP/SL)。
    // 私有 order 频道推整个账户的订单, /v5/order/realtime 在启动对齐时也会返回它们 ——
    // 把它们当"未知"抛出的结果是: 账户里只要有一张条件单, 进程立刻死、甚至启动即崩。
    assertEquals(mapOrderStatus("Untriggered", Coin(0.0)), OrderStatus.Pending)
    // Triggered = "conditional order from Untriggered to New", 是刚活过来, 不是结束
    assertEquals(mapOrderStatus("Triggered", Coin(0.0)), OrderStatus.Pending)

  test("文档之外的订单状态 -> 抛错, 不归成终态"):
    // 归成 Rejected 是终态: 会触发 markTerminal 并清掉 pending —— 一张还活着的单被本地宣告死亡。
    val e = intercept[IllegalStateException](mapOrderStatus("SomethingNew", Coin(0.0)))
    assert(e.getMessage.contains("文档之外的 Bybit 订单状态"), e.getMessage)

  test("方向映射 (统一<->Bybit) 与 TimeInForce 映射"):
    assertEquals(sideToParam(Side.Long), "Buy")
    assertEquals(sideToParam(Side.Short), "Sell")
    assertEquals(sideFromBybit("Buy"), Side.Long)
    assertEquals(sideFromBybit("Sell"), Side.Short)
    intercept[IllegalStateException](sideFromBybit("")) // 空仓空串非有向语境，调用方须自行处理
    assertEquals(tifToParam(TimeInForce.GTC), "GTC")
    assertEquals(tifToParam(TimeInForce.IOC), "IOC")
    assertEquals(tifToParam(TimeInForce.FOK), "FOK")
    assertEquals(tifToParam(TimeInForce.PostOnly), "PostOnly")

  test("asDouble / asDoubleOrZero"):
    assertEquals("42000.5".asDouble, 42000.5)
    assertEquals("".asDoubleOrZero, 0.0)
    assertEquals("1.5".asDoubleOrZero, 1.5)
    intercept[IllegalStateException]("abc".asDouble)

  test("解析 orderbook.1 (level1, 取首档, 数量即币本位)"):
    val json =
      """{"topic":"orderbook.1.BTCUSDT","type":"snapshot","ts":1700000000123,"data":{"s":"BTCUSDT","b":[["42000.5","1.2"]],"a":[["42001.5","0.8"]],"u":123,"seq":456}}"""
    val push = readFromString[WsObj[OrderbookData]](json)
    assertEquals(push.topic, "orderbook.1.BTCUSDT")
    assertEquals(push.ts, 1700000000123L)
    assertEquals(push.data.b.head.head.asDouble, 42000.5)
    assertEquals(push.data.b.head(1).asDouble, 1.2)
    assertEquals(push.data.a.head.head.asDouble, 42001.5)
    assertEquals(push.data.a.head(1).asDouble, 0.8)

  test("解析 tickers snapshot (mark/index/funding 全字段)"):
    val json =
      """{"topic":"tickers.BTCUSDT","type":"snapshot","ts":1700000000200,"data":{"symbol":"BTCUSDT","markPrice":"42000.1","indexPrice":"42000.0","fundingRate":"0.0001","nextFundingTime":"1700028800000"}}"""
    val d = readFromString[WsObj[TickerData]](json).data
    assertEquals(d.markPrice.asDouble, 42000.1)
    assertEquals(d.indexPrice.asDouble, 42000.0)
    assertEquals(d.fundingRate.asDouble, 0.0001)
    assertEquals(d.nextFundingTime.toLong, 1700028800000L)

  test("解析 tickers delta (缺失字段=未变 -> 空串, 仅 fundingRate 出现)"):
    val json =
      """{"topic":"tickers.BTCUSDT","type":"delta","ts":1700000000300,"data":{"symbol":"BTCUSDT","fundingRate":"0.0002"}}"""
    val d = readFromString[WsObj[TickerData]](json).data
    assert(d.markPrice.isEmpty) // 未变 -> 不发布
    assert(d.indexPrice.isEmpty)
    assertEquals(d.fundingRate, "0.0002")

  test("解析 publicTrade (S=taker 方向, T=ms)"):
    val json =
      """{"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":1700000000400,"data":[{"T":1700000000390,"s":"BTCUSDT","S":"Sell","v":"0.5","p":"42000.0"}]}"""
    val d = readFromString[WsList[PublicTradeData]](json).data.head
    assertEquals(d.S, "Sell") // -> isBuyerMaker=true
    assertEquals(d.v.asDouble, 0.5)
    assertEquals(d.p.asDouble, 42000.0)
    assertEquals(d.T, 1700000000390L)

  test("解析 order 推送 (部分成交, cumExecQty)"):
    val json =
      """{"topic":"order","id":"x","creationTime":1700000000500,"data":[{"symbol":"BTCUSDT","orderId":"o1","orderLinkId":"c1","side":"Buy","orderStatus":"PartiallyFilled","price":"42000","qty":"0.01","cumExecQty":"0.003"}]}"""
    val d = readFromString[WsList[OrderData]](json).data.head
    assertEquals(d.orderId, "o1")
    assertEquals(d.orderLinkId, "c1")
    assertEquals(d.side, "Buy")
    assertEquals(d.qty.asDouble, 0.01)
    assertEquals(d.cumExecQty.asDouble, 0.003)
    assertEquals(mapOrderStatus(d.orderStatus, Coin(d.cumExecQty.asDouble)), OrderStatus.PartiallyFilled(Coin(0.003)))

  test("解析 execution 推送 (单笔成交 execQty/execPrice)"):
    val json =
      """{"topic":"execution","id":"x","data":[{"symbol":"BTCUSDT","side":"Buy","execQty":"0.003","execPrice":"42000","execTime":"1700000000490","orderId":"o1","orderLinkId":"c1"}]}"""
    val d = readFromString[WsList[ExecutionData]](json).data.head
    assertEquals(d.execQty.asDouble, 0.003)
    assertEquals(d.execPrice.asDouble, 42000.0)
    assertEquals(d.execTime.toLong, 1700000000490L)

  test("解析 wallet 推送 (totalEquity + 各币种 walletBalance)"):
    val json =
      """{"topic":"wallet","id":"x","creationTime":1700000000600,"data":[{"accountType":"UNIFIED","totalEquity":"50000.5","coin":[{"coin":"USDT","walletBalance":"10000"},{"coin":"BTC","walletBalance":"0.5"}]}]}"""
    val d = readFromString[WsList[WalletData]](json).data.head
    assertEquals(d.totalEquity.asDouble, 50000.5)
    assertEquals(d.coin.map(c => c.coin -> c.walletBalance.asDouble).toMap, Map("USDT" -> 10000.0, "BTC" -> 0.5))

  test("解析 instruments-info REST (priceFilter/lotSizeFilter 嵌套字段)"):
    val json =
      """{"retCode":0,"retMsg":"OK","result":{"category":"linear","list":[{"symbol":"BTCUSDT","priceFilter":{"tickSize":"0.5"},"lotSizeFilter":{"qtyStep":"0.001","minOrderQty":"0.001"}}],"nextPageCursor":""},"time":1700000000000}"""
    val resp = readFromString[InstrumentsResp](json)
    assertEquals(resp.retCode, 0)
    val d = resp.result.list.head
    assertEquals(d.symbol, "BTCUSDT")
    assertEquals(d.priceFilter.tickSize.asDouble, 0.5)
    assertEquals(d.lotSizeFilter.qtyStep.asDouble, 0.001)
    assertEquals(d.lotSizeFilter.minOrderQty.asDouble, 0.001)
    assertEquals(resp.result.nextPageCursor, "")

  test("解析 position/list REST (空仓 side='' / avgPrice='' 不报错)"):
    val json =
      """{"retCode":0,"retMsg":"OK","result":{"list":[{"symbol":"BTCUSDT","side":"Buy","size":"0.5","avgPrice":"41000","unrealisedPnl":"123.4"},{"symbol":"ETHUSDT","side":"","size":"0","avgPrice":"","unrealisedPnl":""}]}}"""
    val list = readFromString[PositionListResp](json).result.list
    assertEquals(list.head.side, "Buy")
    assertEquals(list.head.size.asDouble, 0.5)
    assertEquals(list.head.avgPrice.asDoubleOrZero, 41000.0)
    assertEquals(list(1).side, "") // 空仓
    assertEquals(list(1).avgPrice.asDoubleOrZero, 0.0)
    assertEquals(list(1).unrealisedPnl.asDoubleOrZero, 0.0)

  test("解析 order/create REST (orderId)"):
    val json = """{"retCode":0,"retMsg":"OK","result":{"orderId":"abc","orderLinkId":"c1"}}"""
    val resp = readFromString[OrderCreateResp](json)
    assertEquals(resp.retCode, 0)
    assertEquals(resp.result.orderId, "abc")

  test("控制帧 / 数据帧经 BybitWsMsg 探测"):
    val auth = readFromString[BybitWsMsg]("""{"success":true,"ret_msg":"","op":"auth","conn_id":"x"}""")
    assertEquals(auth.op, "auth")
    assert(auth.success)
    val subErr = readFromString[BybitWsMsg]("""{"success":false,"ret_msg":"error:topic","op":"subscribe","conn_id":"x"}""")
    assertEquals(subErr.op, "subscribe")
    assert(!subErr.success)
    assertEquals(subErr.ret_msg, "error:topic")
    // 数据帧无 op -> 空串, 据 topic 分派
    val data = readFromString[BybitWsMsg]("""{"topic":"tickers.BTCUSDT","ts":1,"data":{}}""")
    assertEquals(data.op, "")
    assertEquals(data.topic, "tickers.BTCUSDT")

  test("reduceOnly 缺失与 false 分得开 —— 缺失即抛, 不当成 false"):
    // jsoniter 对缺失字段取默认值, 于是 `Boolean = false` 会把"报文没带"变成"值是 false"。
    // 而策略拿 reduceOnly 给 resting 单分槽 (止盈槽 vs 加仓槽), 归错槽就会多挂一张单。
    val withoutFlag = readFromString[WsList[OrderData]](
      """{"topic":"order","id":"x","data":[{"symbol":"BTCUSDT","orderId":"o1","side":"Buy","orderStatus":"New"}]}"""
    ).data.head
    assertEquals(withoutFlag.reduceOnly, None, "codec 里必须是 Option, 不能是默认 false")
    val e = intercept[IllegalStateException](
      hft.exchange.RestTransport.requireFlag(withoutFlag.reduceOnly, "Bybit", "reduceOnly", "orderId=o1")
    )
    assert(e.getMessage.contains("不能当成 false"), e.getMessage)

    val withFlag = readFromString[WsList[OrderData]](
      """{"topic":"order","id":"x","data":[{"symbol":"BTCUSDT","orderId":"o1","side":"Buy","orderStatus":"New","reduceOnly":true}]}"""
    ).data.head
    assertEquals(hft.exchange.RestTransport.requireFlag(withFlag.reduceOnly, "Bybit", "reduceOnly", "o1"), true)

  test("REST 挂单查询同样必须带 reduceOnly (缺失即抛, 不当成 false)"):
    val withoutFlag = readFromString[OpenOrdersResp](
      """{"retCode":0,"retMsg":"OK","result":{"list":[{"symbol":"BTCUSDT","orderId":"o1","side":"Buy","orderStatus":"New","updatedTime":"1700000000000"}],"nextPageCursor":""}}"""
    ).result.list.head
    assertEquals(withoutFlag.reduceOnly, None)
    intercept[IllegalStateException](
      hft.exchange.RestTransport.requireFlag(withoutFlag.reduceOnly, "Bybit", "reduceOnly", "orderId=o1")
    )
