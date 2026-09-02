package strategy.utils.option

/** 期权符号解析, 以及 [[OptionInstrument]] / [[Quote]] 的类型不变量 —— 精度与两边报价都是
  * **交易所必给的事实**, 判据放在类型上, 下游不必各自防。 */
class OptionContractSpec extends munit.FunSuite:
  test("parseSymbol: Bybit 期权符号 -> (base, strike, right)"):
    assertEquals(OptionContract.parseSymbol("ETH-26SEP25-3000-C"), Some(("ETH", 3000.0, OptionRight.Call)))
    assertEquals(OptionContract.parseSymbol("BTC-29DEC23-10000-P"), Some(("BTC", 10000.0, OptionRight.Put)))
    assertEquals(OptionContract.parseSymbol("ETH-26SEP25-3500.5-C"), Some(("ETH", 3500.5, OptionRight.Call)))
    // Bybit 实际为 USDT 结算的 5 段格式
    assertEquals(OptionContract.parseSymbol("ETH-26MAR27-1400-P-USDT"), Some(("ETH", 1400.0, OptionRight.Put)))
    assertEquals(OptionContract.parseSymbol("BTC-26DEC25-100000-C-USDT"), Some(("BTC", 100000.0, OptionRight.Call)))

  test("parseSymbol: 非法格式 -> None"):
    assertEquals(OptionContract.parseSymbol("ETHUSDT"), None)
    assertEquals(OptionContract.parseSymbol("ETH-26SEP25-3000-X"), None)
    assertEquals(OptionContract.parseSymbol("ETH-26SEP25-abc-C"), None)

  private def inst(minQty: Double = 1.0, qtyStep: Double = 1.0, tickSize: Double = 0.1) =
    OptionInstrument("ETH-240329-3000-C", 1_711_699_200_000L, 3000.0, OptionRight.Call, ctVal = 0.1, minQty, qtyStep, tickSize)

  test("OptionInstrument: 三个精度参数非正即拒 —— 校验不了精度的合约不该可交易"):
    inst() // 正常构造不抛
    assert(intercept[IllegalArgumentException](inst(minQty = 0.0)).getMessage.contains("最小下单量"))
    assert(intercept[IllegalArgumentException](inst(qtyStep = 0.0)).getMessage.contains("步长"))
    assert(intercept[IllegalArgumentException](inst(tickSize = -1.0)).getMessage.contains("最小变动"))

  test("Quote: 单边报价不构造 Quote —— 那是 optionQuote 返回 None 的情形"):
    // 从前 bid=0 能构造, 于是 SellPlan 里要写 `if bid > 0 then ask/bid else Infinity`,
    // sellQuote 里要写 `require(mid > 0)` —— 两处都在替一个上游已排除的情况兜底。
    Quote(39.0, 40.0)
    assert(intercept[IllegalArgumentException](Quote(0.0, 40.0)).getMessage.contains("买一价"))
    assert(intercept[IllegalArgumentException](Quote(39.0, 0.0)).getMessage.contains("卖一价"))
