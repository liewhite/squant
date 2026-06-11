package hft.domain

class SymbolMetaSpec extends munit.FunSuite:
  private val meta = SymbolMeta(
    exchange = Exchange.Binance,
    symbol = "BTCUSDT",
    tickSize = 0.1,
    sizeStep = 0.001,
    minOrderSize = 0.001,
    contractSize = 1.0,
  )

  test("roundPrice 四舍五入到 tickSize"):
    assertEquals(meta.roundPrice(62761.333), 62761.3)
    assertEquals(meta.roundPrice(62761.35), 62761.4)
    assertEquals(meta.roundPrice(62761.3), 62761.3)

  test("roundSizeDown 向下取整到 sizeStep"):
    assertEquals(meta.roundSizeDown(0.0015), 0.001)
    assertEquals(meta.roundSizeDown(0.0029), 0.002)
    assertEquals(meta.roundSizeDown(0.003), 0.003)

  test("formatPrice 去除多余的尾零"):
    assertEquals(meta.formatPrice(62700.0), "62700")
    assertEquals(meta.formatPrice(62761.30), "62761.3")

  test("formatSize 取整后格式化"):
    assertEquals(meta.formatSize(0.0015), "0.001")

  test("coinToQty/qtyToCoin 按合约乘数互逆"):
    val okxStyle = meta.copy(contractSize = 0.1)
    assertEqualsDouble(okxStyle.coinToQty(0.5), 5.0, 1e-12)
    assertEqualsDouble(okxStyle.qtyToCoin(5.0), 0.5, 1e-12)

  test("isValid 拒绝零精度"):
    assert(!meta.copy(tickSize = 0.0).isValid)
    assert(!meta.copy(sizeStep = 0.0).isValid)
    assert(meta.isValid)
