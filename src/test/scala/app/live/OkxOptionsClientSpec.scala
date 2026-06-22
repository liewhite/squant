package app.live

import app.live.OkxOptionsClient.*

/** OKX 期权客户端的**纯解析逻辑**单测 (不触网): instruments 字段映射、clOrdId 幂等清洗、candle 行解析、
  * 响应外壳 code 判定。IO 路径由模拟盘人工验证 (见类注释)。 */
class OkxOptionsClientSpec extends munit.FunSuite:

  test("instrumentOf: 读 stk/optType/expTime 字段, 不解析符号"):
    val call = InstrumentItem("ETH-USD-240329-3000-C", "3000", "C", "1711699200000", "1", "1", "0.1")
    // lotSz/minSz 取不同值, 验证 qtyStep<-lotSz、minQty<-minSz 的映射方向
    val put = InstrumentItem("ETH-USD-240329-3000-P", "3000.5", "P", "1711699200000", "2", "0.1", "0.05")
    assertEquals(instrumentOf(call), Some(OptionInstrument("ETH-USD-240329-3000-C", 1711699200000L, 3000.0, OptionRight.Call, 1.0, 1.0, 0.1)))
    assertEquals(instrumentOf(put).map(_.right), Some(OptionRight.Put))
    assertEquals(instrumentOf(put).map(_.strike), Some(3000.5))
    assertEquals(instrumentOf(put).map(i => (i.minQty, i.qtyStep, i.tickSize)), Some((0.1, 2.0, 0.05)))

  test("instrumentOf: 非法/缺失字段 -> None (跳过该合约)"):
    assertEquals(instrumentOf(InstrumentItem("x", "abc", "C", "1711699200000", "1", "1", "0.1")), None) // strike 非数字
    assertEquals(instrumentOf(InstrumentItem("x", "3000", "X", "1711699200000", "1", "1", "0.1")), None) // optType 非 C/P
    assertEquals(instrumentOf(InstrumentItem("x", "3000", "C", "0", "1", "1", "0.1")), None)             // expTime<=0

  test("clOrdIdOf: 去连字符 + 截断 32, 确定性 (幂等)"):
    assertEquals(clOrdIdOf("vs-1711699200000-c"), "vs1711699200000c")
    assertEquals(clOrdIdOf("vs-1711699200000-p"), "vs1711699200000p")
    assert(clOrdIdOf("vs-" + "9" * 40 + "-c").length <= 32)
    // 同输入恒得同输出 (幂等的基础)
    assertEquals(clOrdIdOf("vs-123-c"), clOrdIdOf("vs-123-c"))

  test("Bar.parse: [ts,o,h,l,c,...] 取 ts/high/low/close"):
    assertEquals(Bar.parse(List("1711699200000", "3000", "3050", "2980", "3010", "100", "1")), Some(Bar(1711699200000L, 3050.0, 2980.0, 3010.0)))
    assertEquals(Bar.parse(List("1711699200000", "3000")), None) // 列不足

  test("Envelope.asEither: code=0 -> Right(data), 否则 Left(msg)"):
    assertEquals(Envelope("0", "", List(1, 2)).asEither, Right(List(1, 2)))
    assertEquals(Envelope("51000", "param error", List.empty[Int]).asEither, Left("OKX code=51000: param error"))
    assertEquals(CandlesEnvelope("0", "", List(List("a"))).asEither, Right(List(List("a"))))
    assert(CandlesEnvelope("1", "boom", Nil).asEither.isLeft)
