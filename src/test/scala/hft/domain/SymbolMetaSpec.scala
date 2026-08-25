package hft.domain

import hft.TestUnits.given

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
    assertEquals(meta.roundPrice(62761.333).value, 62761.3)
    assertEquals(meta.roundPrice(62761.35).value, 62761.4)
    assertEquals(meta.roundPrice(62761.3).value, 62761.3)

  test("roundSize 就近取整到 sizeStep"):
    assertEquals(meta.roundSize(Contracts(0.0015)).value, 0.002)
    assertEquals(meta.roundSize(Contracts(0.0014)).value, 0.001)
    assertEquals(meta.roundSize(Contracts(0.003)).value, 0.003)

  test("formatPrice 去除多余的尾零"):
    assertEquals(meta.formatPrice(62700.0), "62700")
    assertEquals(meta.formatPrice(62761.30), "62761.3")

  test("formatSize 取整后格式化"):
    assertEquals(meta.formatSize(Contracts(0.0015)), "0.002")

  test("coinToQty/qtyToCoin 按合约乘数互逆"):
    val okxStyle = meta.copy(contractSize = 0.1)
    assertEqualsDouble(okxStyle.toContracts(Coin(0.5)).value, 5.0, 1e-12)
    assertEqualsDouble(okxStyle.toCoin(Contracts(5.0)).value, 0.5, 1e-12)

  test("isValid 拒绝零精度"):
    assert(!meta.copy(tickSize = 0.0).isValid)
    assert(!meta.copy(sizeStep = 0.0).isValid)
    assert(meta.isValid)

  test("浮点累加漂移后的仓位能被平干净 —— 取整方向决定这件事"):
    // 1000 笔 0.1 的成交, 交易所十进制记账下持仓精确是 100; 我们的浮点求和会漂到 99.9999999999986。
    // 向下取整会把这 1.4e-12 的漂移放大成整整一档 (99.999), 留下 0.000999 的残仓 ——
    // 它比 Position.Epsilon 大七个数量级 (判不出归零), 又小于最小下单量 (发不出单), 永远平不掉。
    var drifted = 0.0
    (1 to 1000).foreach(_ => drifted += 0.1)
    assert(drifted != 100.0, s"前提: 浮点累加确实会漂, got $drifted")
    val aligned = meta.roundCoin(Coin(drifted))
    assertEquals(aligned.value, 100.0, "对齐到 step 网格应当把真值恢复回来")
    assert((Coin(drifted) - aligned).abs.isZero, "不该留下任何平不掉的残仓")

  test("就近取整不会把已对齐的数量再削一档"):
    // toExchangeContracts 已按 HALF_UP 对齐, formatSize 若再 FLOOR 一次会把 7.0 削成 6
    val okx = SymbolMeta(Exchange.Okx, "BTCUSDT", tickSize = 0.1, sizeStep = 1.0, minOrderSize = 1.0, contractSize = 0.01)
    assertEquals(okx.toExchangeContracts(Coin(0.07)).value, 7.0)
    assertEquals(okx.formatSize(Contracts(6.999999999999999)), "7")

  test("多取一档只发生在确实过半时 (不是无条件放大)"):
    assertEquals(meta.roundCoin(Coin(0.0014)).value, 0.001)
    assertEquals(meta.roundCoin(Coin(0.0016)).value, 0.002)

  test("minOrderSize 在张数域判定 —— contractSize≠1 时不能拿币本位直接比"):
    // OKX: minSz/lotSz 都是张数, 每张 0.01 币 -> 最小下单 10 张 = 0.1 币
    val okx = SymbolMeta(Exchange.Okx, "BTCUSDT", tickSize = 0.1, sizeStep = 1.0, minOrderSize = 10.0, contractSize = 0.01)
    assert(!okx.meetsMinOrderSize(Coin(0.09)), "9 张 < 10 张, 收不下")
    assert(okx.meetsMinOrderSize(Coin(0.10)), "10 张, 正好够")
    // 若错误地拿币本位 0.10 去和 minOrderSize=10 比, 会得出"远远不够"的相反结论
    assert(0.10 < okx.minOrderSize, "这正是搞错单位时会踩的坑")

  test("被取整成 0 的数量不算合法下单量"):
    assertEquals(meta.roundCoin(Coin(0.0004)).value, 0.0)
    assert(!meta.meetsMinOrderSize(Coin(0.0004)), "从前它会被当成一张数量为 0 的单发出去")

  test("minOrderSize = 0 (交易所未提供) 时只要求数量为正"):
    val loose = SymbolMeta(Exchange.Binance, "X", tickSize = 0.01, sizeStep = 0.001, minOrderSize = 0.0, contractSize = 1.0)
    assert(loose.meetsMinOrderSize(Coin(0.001)))
    assert(!loose.meetsMinOrderSize(Coin(0.0004)))
