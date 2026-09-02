package hft.exchange.hyperliquid

import com.github.plokhotnyuk.jsoniter_scala.core.readFromString
import HyperliquidCodec.*
import HyperliquidCodec.given

/** 边界上的两件事：dex 归属判定，以及真实线路报文能不能解出来。
  *
  * 报文都取自 2026-09 的实测线路 —— 手编的样例证明不了"字段名对不对"，而字段名错了
  * 会静默取到默认值 (价格 0、时间 0)，没有任何症状。
  */
class HyperliquidCodecSpec extends munit.FunSuite:

  // ==================== dex 归属 ====================

  test("默认 dex 只认无前缀的 coin: 别的 dex 的同名资产不属于它"):
    assertEquals(fromHyperliquid("BTC", ""), Some("BTC"))
    assertEquals(fromHyperliquid("xyz:AAPL", ""), None)

  test("具名 dex 只认自己的前缀 —— 包括默认 dex 那个裸 coin"):
    assertEquals(fromHyperliquid("xyz:AAPL", "xyz"), Some("AAPL"))
    assertEquals(fromHyperliquid("abc:AAPL", "xyz"), None)
    // 这条是最危险的那一种: 剥掉前缀后两者都是 "AAPL", 混进来就是两个标的的行情落进同一个 Instrument
    assertEquals(fromHyperliquid("AAPL", "xyz"), None)

  test("Symbol 与线路 coin 可往返"):
    val wire = toHyperliquid("AAPL", "xyz")
    assertEquals(wire, "xyz:AAPL")
    assertEquals(fromHyperliquid(wire, "xyz"), Some("AAPL"))
    assertEquals(toHyperliquid("BTC", ""), "BTC")

  // ==================== 真实线路报文 ====================

  test("bbo: 双边盘口逐字段解出"):
    val raw = """{"channel":"bbo","data":{"coin":"xyz:AAPL","time":1788330949235,""" +
      """"bbo":[{"px":"324.18","sz":"3.085","n":1},{"px":"324.21","sz":"0.58","n":1}]}}"""
    val d = readFromString[BboPush](raw).data
    assertEquals(d.coin, "xyz:AAPL")
    assertEquals(d.time, 1788330949235L)
    assertEquals(d.bbo.flatten.map(_.px), List("324.18", "324.21"))
    assertEquals(d.bbo.flatten.map(_.sz), List("3.085", "0.58"))

  test("bbo: 单边盘口的 null 保留为 None —— 它是稀薄品种的合法状态, 不是坏报文"):
    val raw = """{"channel":"bbo","data":{"coin":"xyz:AAOI","time":1,"bbo":[null,{"px":"9.9","sz":"1.0","n":1}]}}"""
    val d = readFromString[BboPush](raw).data
    assertEquals(d.bbo.head, None)
    assertEquals(d.bbo(1).map(_.px), Some("9.9"))

  test("trades: side 是主动方, 'A' = 主动卖"):
    val raw = """{"channel":"trades","data":[{"coin":"xyz:AAPL","side":"A","px":"324.15","sz":"8.278",""" +
      """"time":1788330881768,"hash":"0xef","tid":890977834540870,"users":["0xf3","0x79"]}]}"""
    val trades = readFromString[TradesPush](raw).data
    assertEquals(trades.size, 1)
    assertEquals(trades.head.side, "A")
    assertEquals(trades.head.px, "324.15")
    assertEquals(trades.head.time, 1788330881768L)

  test("activeAssetCtx: 标记价/预言机价/资金费率同在一条推送里"):
    val raw = """{"channel":"activeAssetCtx","data":{"coin":"xyz:AAPL","ctx":{"funding":"0.00000625",""" +
      """"openInterest":"250233.61","prevDayPx":"316.31","premium":"0.0000107961","oraclePx":"324.19",""" +
      """"markPx":"324.19","midPx":"324.195","impactPxs":["324.165","324.222"],"dayBaseVlm":"132610.943"}}}"""
    val d = readFromString[AssetCtxPush](raw).data
    assertEquals(d.coin, "xyz:AAPL")
    assertEquals(d.ctx.markPx, "324.19")
    assertEquals(d.ctx.oraclePx, "324.19")
    assertEquals(d.ctx.funding, "0.00000625")

  test("meta: 下架标记与资产名解得出"):
    val raw = """{"universe":[{"szDecimals":3,"name":"xyz:TSLA","maxLeverage":20},""" +
      """{"szDecimals":2,"name":"xyz:OLD","maxLeverage":5,"isDelisted":true}]}"""
    val universe = readFromString[MetaResp](raw).universe
    assertEquals(universe.map(_.name), List("xyz:TSLA", "xyz:OLD"))
    assertEquals(universe.map(_.isDelisted), List(false, true))

  test("信封只看 channel, 其余字段一概跳过"):
    assertEquals(readFromString[WsEnvelope]("""{"channel":"pong"}""").channel, "pong")
    assertEquals(readFromString[WsEnvelope]("""{"channel":"error","data":"..."}""").channel, "error")

  test("非法数字即抛错 —— 静默归零会造成无法察觉的状态错误"):
    intercept[IllegalStateException]("nope".asDouble)

  test("资金费按整点结算: 下一个结算时刻是下一个整点"):
    val oneMinutePastTheHour = 1788330060000L
    val settle = HyperliquidMarketFeed.nextHourlySettle(oneMinutePastTheHour)
    assert(settle > oneMinutePastTheHour)
    assertEquals(settle % 3_600_000L, 0L)
    assert(settle - oneMinutePastTheHour <= 3_600_000L)
