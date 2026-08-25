package hft.domain

/** 发往交易所的数量字符串必须是合法精度。
  *
  * 回归防线：币本位对齐 -> 换回张数 -> 格式化，中间的浮点往返会留下
  * `7.000000000000001` / `2.9999999999999996` 这类尾巴，而 client 的格式化不再取整，
  * 交易所会按 lot size 直接拒单。用 Binance (contractSize = 1) 测不出来 —— dust 恒为零，
  * 而回测基线正好用的是它。
  */
class ExchangeOrderPrecisionSpec extends munit.FunSuite:
  private def metaOf(contractSize: Double, sizeStep: Double) =
    SymbolMeta(Exchange.Okx, "BTC-USDT-SWAP", tickSize = 0.1, sizeStep = sizeStep, minOrderSize = sizeStep, contractSize = contractSize)

  private def sentQuantity(contractSize: Double, sizeStep: Double, wanted: Double): String =
    val meta = metaOf(contractSize, sizeStep)
    val metas = Map((meta.exchange, meta.symbol) -> meta)
    val order = Order("", meta.exchange, meta.symbol, Side.Long, OrderType.Market, Coin(wanted), reduceOnly = false, clientOrderId = "c1")
    val aligned = OrderConversion.alignToExchange(order, metas).getOrElse(fail("应能对齐"))
    meta.formatSize(OrderConversion.toExchangeOrder(aligned, metas).quantity)

  test("contractSize != 1: 发出的数量是整档，不带浮点尾巴"):
    assertEquals(sentQuantity(contractSize = 0.01, sizeStep = 1.0, wanted = 0.07), "7")
    assertEquals(sentQuantity(contractSize = 0.1, sizeStep = 1.0, wanted = 0.3), "3")
    assertEquals(sentQuantity(contractSize = 0.001, sizeStep = 1.0, wanted = 1.001), "1001")

  test("非整档的目标数量就近取整到整档"):
    // 0.075 币 = 7.5 张 (恰好半档) -> HALF_UP 取 8 张。
    // 从前这里取 7 张 (FLOOR)。改成就近是因为向下会把浮点累加漂移放大成整整一档:
    // 一个真值 100 的仓位浮点求和成 99.9999999999986, FLOOR 后是 99.999, 残仓永远平不掉
    // (见 SymbolMetaSpec 的漂移用例)。多取一档的代价小得多 —— reduceOnly 有撮合层截断,
    // 开仓也只是多一个 step 的敞口。
    assertEquals(sentQuantity(contractSize = 0.01, sizeStep = 1.0, wanted = 0.075), "8")
    // 未过半仍然向下, 不是无条件放大
    assertEquals(sentQuantity(contractSize = 0.01, sizeStep = 1.0, wanted = 0.074), "7")

  test("sizeStep 不为 1 时同样对齐"):
    // contractSize=0.01, sizeStep=5: 0.32 币 = 32 张 -> 下取整到 30 张
    assertEquals(sentQuantity(contractSize = 0.01, sizeStep = 5.0, wanted = 0.32), "30")

  test("contractSize = 1 (Binance) 同样是就近取整"):
    assertEquals(sentQuantity(contractSize = 1.0, sizeStep = 0.001, wanted = 0.0015), "0.002")
    assertEquals(sentQuantity(contractSize = 1.0, sizeStep = 0.001, wanted = 0.0014), "0.001")

  test("低于最小下单量 -> 明确拒绝, 而不是发一张交易所收不下的单"):
    val metas = Map((Exchange.Okx, "BTCUSDT") ->
      SymbolMeta(Exchange.Okx, "BTCUSDT", tickSize = 0.1, sizeStep = 1.0, minOrderSize = 10.0, contractSize = 0.01))
    def attempt(wanted: Double) = OrderConversion.alignToExchange(
      Order("", Exchange.Okx, "BTCUSDT", Side.Long, OrderType.Market, Coin(wanted), reduceOnly = false, clientOrderId = "c"),
      metas,
    )
    assert(attempt(0.09).isLeft, "9 张 < 最小 10 张")
    assert(attempt(0.09).left.exists(_.contains("最小下单量")), attempt(0.09).toString)
    assert(attempt(0.10).isRight, "10 张够了")
