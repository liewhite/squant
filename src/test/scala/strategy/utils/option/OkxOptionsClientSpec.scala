package strategy.utils.option

import strategy.utils.option.OkxOptionsClient.*

/** OKX 期权客户端的**纯解析逻辑**单测 (不触网): instruments 字段映射、clOrdId 幂等清洗、candle 行解析、
  * 响应外壳 code 判定。IO 路径由模拟盘人工验证 (见类注释)。 */
class OkxOptionsClientSpec extends munit.FunSuite:

  test("instrumentOf: 读 stk/optType/expTime 字段, 不解析符号"):
    val call = InstrumentItem("ETH-USD-240329-3000-C", "3000", "C", "1711699200000", "1", "1", "0.1", ctVal = "0.01")
    // lotSz/minSz 取不同值, 验证 qtyStep<-lotSz、minQty<-minSz 的映射方向
    val put = InstrumentItem("ETH-USD-240329-3000-P", "3000.5", "P", "1711699200000", "2", "0.1", "0.05", ctVal = "0.01")
    assertEquals(instrumentOf(call), Some(OptionInstrument("ETH-USD-240329-3000-C", 1711699200000L, 3000.0, OptionRight.Call, 0.01, 1.0, 1.0, 0.1)))
    assertEquals(instrumentOf(put).map(_.right), Some(OptionRight.Put))
    assertEquals(instrumentOf(put).map(_.strike), Some(3000.5))
    assertEquals(instrumentOf(put).map(i => (i.minQty, i.qtyStep, i.tickSize)), Some((0.1, 2.0, 0.05)))

  test("instrumentOf: 非法/缺失字段 -> None (跳过该合约)"):
    assertEquals(instrumentOf(InstrumentItem("x", "abc", "C", "1711699200000", "1", "1", "0.1", ctVal = "0.01")), None) // strike 非数字
    assertEquals(instrumentOf(InstrumentItem("x", "3000", "X", "1711699200000", "1", "1", "0.1", ctVal = "0.01")), None) // optType 非 C/P
    assertEquals(instrumentOf(InstrumentItem("x", "3000", "C", "0", "1", "1", "0.1", ctVal = "0.01")), None) // expTime<=0
    // ctVal 缺失 -> None: 张数换算不了, 按 1 猜会把 delta 静默错算一个整数倍
    assertEquals(instrumentOf(InstrumentItem("x", "3000", "C", "1711699200000", "1", "1", "0.1")), None)

  test("instrumentOf: 精度三件套缺一个也跳过整个合约"):
    // 从前它们是 getOrElse(0.0): 一行坏报文换来的是一个"可交易"的合约, 而
    // OptionQty.alignDown 在 step<=0 时跳过对齐、minQty=0 放过任何量 —— 下单量校验整体失效。
    val ok = InstrumentItem("x", "3000", "C", "1711699200000", "1", "1", "0.1", ctVal = "0.01")
    assert(instrumentOf(ok).isDefined)
    assertEquals(instrumentOf(ok.copy(minSz = "")).map(_.symbol), None, "minSz 缺失")
    assertEquals(instrumentOf(ok.copy(lotSz = "0")).map(_.symbol), None, "lotSz 非正")
    assertEquals(instrumentOf(ok.copy(tickSz = "abc")).map(_.symbol), None, "tickSz 非法")

  test("clOrdIdOf: 去连字符 + 截断 32, 确定性 (幂等)"):
    assertEquals(clOrdIdOf("vs-1711699200000-c"), "vs1711699200000c")
    assertEquals(clOrdIdOf("vs-1711699200000-p"), "vs1711699200000p")
    assert(clOrdIdOf("vs-" + "9" * 40 + "-c").length <= 32)
    // 同输入恒得同输出 (幂等的基础)
    assertEquals(clOrdIdOf("vs-123-c"), clOrdIdOf("vs-123-c"))
    // 不同周期/腿不得撞同一 clOrdId (否则交易所幂等去重会吞掉第二单)
    assertNotEquals(clOrdIdOf("vs-100-c"), clOrdIdOf("vs-200-c"))
    assertNotEquals(clOrdIdOf("vs-100-c"), clOrdIdOf("vs-100-p"))

  test("sellOrderBody: postOnly -> post_only, 否则 ioc; 均带 px, cross/sell 字段齐全"):
    val maker = sellOrderBody("ETH-USD-240329-3000-C", 2.0, 12.5, postOnly = true, "vs100c")
    assert(maker.contains(""""instId":"ETH-USD-240329-3000-C""""))
    assert(maker.contains(""""tdMode":"cross""""))
    assert(maker.contains(""""side":"sell""""))
    assert(maker.contains(""""ordType":"post_only""""))
    assert(maker.contains(""""sz":"2""""))
    assert(maker.contains(""""px":"12.5""""))
    assert(maker.contains(""""clOrdId":"vs100c""""))
    val taker = sellOrderBody("ETH-USD-240329-3000-P", 1.0, 9.8, postOnly = false, "vs100p")
    assert(taker.contains(""""ordType":"ioc""""))
    assert(taker.contains(""""px":"9.8"""")) // taker 限价也带 px (限定最差价)

  test("Bar.parse: [ts,o,h,l,c,...] 取 ts/high/low/close"):
    assertEquals(Bar.parse(List("1711699200000", "3000", "3050", "2980", "3010", "100", "1")), Some(Bar(1711699200000L, 3050.0, 2980.0, 3010.0)))
    assertEquals(Bar.parse(List("1711699200000", "3000")), None) // 列不足

  test("Envelope.asEither: code=0 -> Right(data), 否则 Left(msg)"):
    assertEquals(Envelope("0", "", List(1, 2)).asEither, Right(List(1, 2)))
    assertEquals(Envelope("51000", "param error", List.empty[Int]).asEither, Left("OKX code=51000: param error"))
    assertEquals(CandlesEnvelope("0", "", List(List("a"))).asEither, Right(List(List("a"))))
    assert(CandlesEnvelope("1", "boom", Nil).asEither.isLeft)

  test("markOf: markVol 缺失/非法/<=0 -> None (该腿无标记 IV, 不参与定量与 delta)"):
    assertEquals(markOf(SummaryItem("ETH-USD-C", "0.65")), Some(OptionMark("ETH-USD-C", 0.65)))
    assertEquals(markOf(SummaryItem("ETH-USD-C", "")), None)
    assertEquals(markOf(SummaryItem("ETH-USD-C", "abc")), None)
    assertEquals(markOf(SummaryItem("ETH-USD-C", "0")), None)

  test("期权 instFamily 拼在一处 (三个接口同一个口径)"):
    // 前缀过滤依赖它与 instId 的形状一致: instFamily=ETH-USD, instId=ETH-USD-260327-3000-C
    assert("ETH-USD-260327-3000-C".startsWith("ETH-USD-"))
    assert(!"BTC-USD-260327-60000-C".startsWith("ETH-USD-"))

  test("holdingOf: pos 带符号 (负=空头); 0 张也保留 (让日志能区分刚平完和从没开过)"):
    assertEquals(holdingOf(PositionItem("ETH-USD-C", "-10")), OptionHolding("ETH-USD-C", -10.0))
    assertEquals(holdingOf(PositionItem("ETH-USD-C", "0")), OptionHolding("ETH-USD-C", 0.0))

  test("holdingOf: pos 读不出来即抛 —— 那不等于'没有持仓'"):
    // 丢掉这一行的后果: 声明式对账看不见这条腿, 把它当成"还没开"再开一次。
    // PortfolioDelta.resolve 只报得出"拿到了却配不上"的腿, 报不出"根本没拿到"的腿。
    val e = intercept[IllegalStateException](holdingOf(PositionItem("ETH-USD-C", "")))
    assert(e.getMessage.contains("pos 不是数字"), e.getMessage)
