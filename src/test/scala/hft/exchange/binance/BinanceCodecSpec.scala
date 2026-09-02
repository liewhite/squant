package hft.exchange.binance

import com.github.plokhotnyuk.jsoniter_scala.core.*

import BinanceCodec.*
import BinanceCodec.given

/** Binance 报文解析：方向穷举、订阅失败可辨、reduceOnly 缺失与 false 分得开。 */
class BinanceCodecSpec extends munit.FunSuite:

  test("方向穷举 BUY/SELL, 其余抛 —— 不再静默取反"):
    assertEquals(sideFromBinance("BUY"), hft.domain.Side.Long)
    assertEquals(sideFromBinance("SELL"), hft.domain.Side.Short)
    // 从前是 `if side == "BUY" then Long else Short`: 任何非 BUY 的值 (含 codec 默认空串)
    // 都变成 Short —— 方向静默取反。
    val e = intercept[IllegalStateException](sideFromBinance(""))
    assert(e.getMessage.contains("未知的 Binance 方向"), e.getMessage)
    intercept[IllegalStateException](sideFromBinance("buy"))

  test("公共流请求应答: 成功与失败都没有 e 字段, 必须能分辨"):
    // 从前一律当 SUBSCRIBE ack 忽略, 于是一次非法订阅被静默吞掉:
    // 订阅没生效、行情永不到达、零症状。
    val ack = readFromString[WsEnvelope]("""{"result":null,"id":1}""")
    assertEquals(ack.e, "")
    assertEquals(ack.error, None)
    val failed = readFromString[WsEnvelope]("""{"id":2,"error":{"code":-1121,"msg":"Invalid symbol."}}""")
    assertEquals(failed.e, "")
    assertEquals(failed.error.map(_.code), Some(-1121))

  test("reduceOnly 缺失与 false 分得开 (WS 的 R 与 REST 的 reduceOnly)"):
    val wsWithout = readFromString[OrderTradeUpdateMsg]("""{"E":1,"o":{"s":"BTCUSDT","i":7}}""").o
    assertEquals(wsWithout.R, None)
    intercept[IllegalStateException](
      hft.exchange.RestTransport.requireFlag(wsWithout.R, "Binance", "R(reduceOnly)", "orderId=7")
    )
    val wsWith = readFromString[OrderTradeUpdateMsg]("""{"E":1,"o":{"s":"BTCUSDT","i":7,"R":true}}""").o
    assertEquals(wsWith.R, Some(true))

    val restWithout = readFromString[List[OpenOrder]]("""[{"orderId":1,"symbol":"BTCUSDT","side":"BUY"}]""").head
    assertEquals(restWithout.reduceOnly, None)
    val restWith = readFromString[List[OpenOrder]]("""[{"orderId":1,"symbol":"BTCUSDT","side":"BUY","reduceOnly":true}]""").head
    assertEquals(restWith.reduceOnly, Some(true))
