package app.live

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
