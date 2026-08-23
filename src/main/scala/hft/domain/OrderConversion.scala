package hft.domain

/** 已换算成**交易所格式**的订单：数量是合约张数、价格已按 tick 取整。
  *
  * 独立类型而非复用 [[Order]]，是为了让"哪一侧的单"由类型回答。框架内部一律用 [[Order]]
  * （币本位），只有 [[hft.exchange.ExchangeClient.placeOrder]] 收本类型 —— 于是"忘了换算就
  * 直接下单"在类型上写不出来。张数这个概念也就被关在了适配层里。
  */
final case class ExchangeOrder(
    exchange: Exchange,
    symbol: Symbol,
    side: Side,
    orderType: OrderType,
    quantity: Contracts,
    reduceOnly: Boolean,
    clientOrderId: String,
)

/** 订单在"框架内"与"交易所侧"之间的换算。
  *
  * 分成两步是有原因的：
  *   - [[roundToExchangePrecision]] 在策略产出后立刻做，把数量对齐到交易所收得下的精度，
  *     **但结果仍是币本位**。回测撮合与实盘因此看到同一个数，不必为了取整而把张数
  *     泄漏进框架。
  *   - [[toExchangeOrder]] 是发往真实交易所的最后一步，只有 exchange 适配层需要。
  *
  * 不止一个地方要发单（策略经 [[hft.engine.StrategyRunner]]、监督者降级平仓），
  * 同一份换算只能有一处实现 —— 各写各的迟早会在取整方向或张数换算上错开。
  */
object OrderConversion:
  /** 缺 [[SymbolMeta]] 说明发单方引用了未预加载的标的，是配置错误，立即终止 */
  private def metaOf(order: Order, symbolMetas: Map[(Exchange, Symbol), SymbolMeta]): SymbolMeta =
    symbolMetas.getOrElse(
      (order.exchange, order.symbol),
      sys.error(s"SymbolMeta not found for ${order.exchange} ${order.symbol}, cannot convert order"),
    )

  /** 把数量与价格对齐到交易所精度，**数量仍是币本位** */
  def roundToExchangePrecision(order: Order, symbolMetas: Map[(Exchange, Symbol), SymbolMeta]): Order =
    val meta = metaOf(order, symbolMetas)
    val orderType = order.orderType match
      case OrderType.Market            => OrderType.Market
      case OrderType.Limit(price, tif) => OrderType.Limit(meta.roundPrice(price), tif)
    order.copy(quantity = meta.roundCoinDown(order.quantity), orderType = orderType)

  /** 换算成交易所格式（币本位 -> 合约张数）—— 发往真实交易所前的最后一步 */
  def toExchangeOrder(order: Order, symbolMetas: Map[(Exchange, Symbol), SymbolMeta]): ExchangeOrder =
    val meta = metaOf(order, symbolMetas)
    ExchangeOrder(
      exchange = order.exchange,
      symbol = order.symbol,
      side = order.side,
      orderType = order.orderType,
      // 用 toExchangeContracts 而非 toContracts：后者是裸 double 除法，会留下
      // 让交易所拒单的浮点尾差 (见 SymbolMeta.toExchangeContracts)
      quantity = meta.toExchangeContracts(order.quantity),
      reduceOnly = order.reduceOnly,
      clientOrderId = order.clientOrderId,
    )
