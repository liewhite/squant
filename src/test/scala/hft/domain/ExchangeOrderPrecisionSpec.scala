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
    val aligned = OrderConversion.roundToExchangePrecision(order, metas)
    meta.formatSize(OrderConversion.toExchangeOrder(aligned, metas).quantity)

  test("contractSize != 1: 发出的数量是整档，不带浮点尾巴"):
    assertEquals(sentQuantity(contractSize = 0.01, sizeStep = 1.0, wanted = 0.07), "7")
    assertEquals(sentQuantity(contractSize = 0.1, sizeStep = 1.0, wanted = 0.3), "3")
    assertEquals(sentQuantity(contractSize = 0.001, sizeStep = 1.0, wanted = 1.001), "1001")

  test("非整档的目标数量向下取整到整档"):
    // 0.075 币 = 7.5 张，sizeStep=1 -> 7 张 (下取整，不放大敞口)
    assertEquals(sentQuantity(contractSize = 0.01, sizeStep = 1.0, wanted = 0.075), "7")

  test("sizeStep 不为 1 时同样对齐"):
    // contractSize=0.01, sizeStep=5: 0.32 币 = 32 张 -> 下取整到 30 张
    assertEquals(sentQuantity(contractSize = 0.01, sizeStep = 5.0, wanted = 0.32), "30")

  test("contractSize = 1 (Binance) 行为不变"):
    assertEquals(sentQuantity(contractSize = 1.0, sizeStep = 0.001, wanted = 0.0015), "0.001")
