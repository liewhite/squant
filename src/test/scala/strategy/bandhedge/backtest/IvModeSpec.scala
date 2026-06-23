package strategy.bandhedge.backtest

/** IvMode.parse 纯逻辑单测：IV 政策解析 (trailing/fair/inflated:<f>)，非法值 fail-fast。 */
class IvModeSpec extends munit.FunSuite:
  test("解析 trailing / fair / inflated:<factor>"):
    assertEquals(IvMode.parse("trailing"), IvMode.Trailing)
    assertEquals(IvMode.parse("fair"), IvMode.Fair)
    assertEquals(IvMode.parse("inflated:1.2"), IvMode.Inflated(1.2))

  test("非法 ivMode -> 抛错 (失败可见, 不静默退化)"):
    intercept[RuntimeException](IvMode.parse("bogus"))
    intercept[RuntimeException](IvMode.parse("inflated:abc"))
