package hft.domain

/** 已换算成**交易所格式**的订单：数量是合约张数、价格已按 tick 取整。
  *
  * 独立类型而非复用 [[Order]]，是为了让"哪一侧的单"由类型回答。框架内部一律用 [[Order]]
  * （币本位），只有 [[hft.exchange.TradingClient.placeOrder]] 收本类型 —— 于是"忘了换算就
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

/** 订单在"框架内"与"交易所侧"之间的换算 —— **只在柜台里发生**。
  *
  * 精度是交易所的事实：tick 大小、最小下单量、张数换算，策略与引擎都不需要知道。
  * 从前这一层泄漏到了策略侧 (策略执行器持有全交易所的合约规格表)，代价是每个发单方
  * 都得自己回答"收不下怎么办"，而它们回答得并不一致。
  *
  * 分成两步是有原因的：
  *   - [[alignToExchange]] 把数量对齐到交易所收得下的精度（收不下就明说），
  *     **但结果仍是币本位**。回测撮合与实盘因此看到同一个数，不必为了取整而把张数
  *     泄漏进框架。
  *   - [[toExchangeOrder]] 是发往真实交易所的最后一步。虚拟柜台不走这一步 ——
  *     它撮合的就是币本位。
  */
object OrderConversion:
  /** 把数量与价格对齐到交易所精度，**数量仍是币本位**。
    *
    * 返回 `Left` 表示对齐之后交易所**收不下**这一单（低于最小下单量，含被取整成 0 的情形）。
    * 用 Either 而不是直接返回 Order，是为了让调用方在编译期就被迫回答"收不下怎么办"
    * —— 从前这个判定压根不存在：`minOrderSize` 三家交易所都解析了却从没被用过，
    * 低于一档的量被向下取整成 0 之后照样发出去，换回一个拒单和一次白跑的往返。
    *
    * Left 里给的是**拼好的说明**而不是错误码：它最终会作为拒单理由回流给策略，
    * 沿途每一层各自查一遍规格拼一遍消息只会写出几种说法。
    */
  def alignToExchange(order: Order, meta: SymbolMeta): Either[String, Order] =
    val orderType = order.orderType match
      case OrderType.Market            => OrderType.Market
      case OrderType.Limit(price, tif) => OrderType.Limit(meta.roundPrice(price), tif)
    val aligned = order.copy(quantity = meta.roundCoin(order.quantity), orderType = orderType)
    if meta.meetsMinOrderSize(aligned.quantity) then Right(aligned)
    else
      Left(
        s"${order.exchange} ${order.symbol} 数量 ${order.quantity.value} 对齐后为 ${aligned.quantity.value} " +
          s"(${meta.toExchangeContracts(aligned.quantity).value} 张), 低于最小下单量 ${meta.minOrderSize} 张"
      )

  /** 换算成交易所格式（币本位 -> 合约张数）—— 发往真实交易所前的最后一步 */
  def toExchangeOrder(order: Order, meta: SymbolMeta): ExchangeOrder =
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
