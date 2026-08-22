package hft.domain

/** 币本位订单 -> 交易所格式：数量换成合约张数、价格与数量按交易所精度取整。
  *
  * 独立于任何组件，因为**不止一个地方**要发单：策略经 [[hft.engine.StrategyRunner]] 发，
  * 监督者降级时也要发一张平仓单。同一份换算只能有一处实现 —— 各写各的，迟早会在
  * 取整方向或张数换算上错开，而这种错开的症状是"下单数量莫名其妙差一点"。
  */
object OrderConversion:
  /** 缺 [[SymbolMeta]] 说明发单方引用了未预加载的标的，是配置错误，立即终止 */
  def toExchangeFormat(order: Order, symbolMetas: Map[(Exchange, Symbol), SymbolMeta]): Order =
    val meta = symbolMetas.getOrElse(
      (order.exchange, order.symbol),
      sys.error(s"SymbolMeta not found for ${order.exchange} ${order.symbol}, cannot convert order"),
    )
    val quantity = meta.roundSizeDown(meta.coinToQty(order.quantity))
    val orderType = order.orderType match
      case OrderType.Market            => OrderType.Market
      case OrderType.Limit(price, tif) => OrderType.Limit(meta.roundPrice(price), tif)
    order.copy(quantity = quantity, orderType = orderType)
