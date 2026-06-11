package hft.domain

import java.util.UUID

/** 交易所枚举 */
enum Exchange:
  case Binance

  /** 生成交易所合法的 client_order_id */
  def newClientOrderId: String =
    val hex = UUID.randomUUID().toString.replace("-", "")
    this match
      case Binance => s"0x$hex" // 34 字符, Binance 上限 36

/** 交易方向 */
enum Side:
  case Long, Short

  def opposite: Side = this match
    case Long  => Short
    case Short => Long

/** 限价单有效方式 */
enum TimeInForce:
  case GTC, IOC, FOK, PostOnly

/** 订单类型 */
enum OrderType:
  case Market
  case Limit(price: Price, tif: TimeInForce)

/** 订单状态 */
enum OrderStatus:
  /** 本地已创建，等待交易所响应 */
  case Created
  /** 交易所已收到，等待成交 */
  case Pending
  case PartiallyFilled(filled: Quantity)
  case Filled
  case Cancelled
  case Rejected(reason: String)
  /** 下单请求发送失败 (REST API 错误) */
  case Error(reason: String)

  /** 是否为终态 (订单生命周期结束) */
  def isTerminal: Boolean = this match
    case Filled | Cancelled | Rejected(_) | Error(_) => true
    case _                                           => false

  /** 是否已被交易所确认 (非 Created 状态) */
  def isConfirmed: Boolean = this != Created

/** 订单。quantity 统一为币本位数量，发送交易所前由 SymbolMeta 转换 */
final case class Order(
    id: OrderId,
    exchange: Exchange,
    symbol: Symbol,
    side: Side,
    orderType: OrderType,
    quantity: Quantity,
    reduceOnly: Boolean,
    clientOrderId: String,
)

/** 订单更新事件 */
final case class OrderUpdate(
    orderId: OrderId,
    clientOrderId: Option[String],
    exchange: Exchange,
    symbol: Symbol,
    side: Side,
    status: OrderStatus,
    /** 订单价格 (限价单) */
    price: Price,
    /** 订单总数量 (币本位) */
    quantity: Quantity,
    /** 累计成交量 */
    filledQuantity: Quantity,
    /** 本次成交量 (用于乐观更新 position) */
    fillSize: Quantity,
    timestamp: Timestamp,
)

/** 成交事件 (用于乐观更新仓位) */
final case class Fill(
    exchange: Exchange,
    symbol: Symbol,
    side: Side,
    price: Price,
    size: Quantity,
    timestamp: Timestamp,
)

/** 仓位。size 为正表示多头，为负表示空头 */
final case class Position(
    exchange: Exchange,
    symbol: Symbol,
    size: Quantity,
    entryPrice: Price,
    unrealizedPnl: Double,
):
  /** 判断是否空仓 (epsilon 比较避免浮点精度问题) */
  def isEmpty: Boolean = math.abs(size) < Position.Epsilon

  /** 持仓方向，空仓返回 None */
  def side: Option[Side] =
    if isEmpty then None
    else if size > 0 then Some(Side.Long)
    else Some(Side.Short)

object Position:
  val Epsilon: Double = 1e-10

  def empty(exchange: Exchange, symbol: Symbol): Position =
    Position(exchange, symbol, 0.0, 0.0, 0.0)

/** 资产余额 */
final case class Balance(
    exchange: Exchange,
    asset: String,
    available: Double,
    timestamp: Timestamp,
)

/** Best Bid Offer (L1 行情) */
final case class BBO(
    exchange: Exchange,
    symbol: Symbol,
    bidPrice: Price,
    bidQty: Quantity,
    askPrice: Price,
    askQty: Quantity,
    timestamp: Timestamp,
):
  def spread: Price = askPrice - bidPrice
  def midPrice: Price = (bidPrice + askPrice) / 2.0

/** 资金费率 */
final case class FundingRate(
    exchange: Exchange,
    symbol: Symbol,
    rate: Rate,
    nextSettleTime: Timestamp,
    /** 数据时间戳，用于计算基于剩余时间的日化费率 */
    timestamp: Timestamp,
):
  /** 基于剩余时间的日化费率: rate * 24 / hoursToSettle (最小 1 小时防止结算临近时爆炸) */
  def dailyRate: Rate = dailyRateWithBaseTime(nextSettleTime, timestamp)

  /** 基于指定时间基准的日化费率，用于跨交易所公平比较 */
  def dailyRateWithBaseTime(baseSettleTime: Timestamp, currentTime: Timestamp): Rate =
    if currentTime >= baseSettleTime then 0.0
    else
      val hoursToSettle = (baseSettleTime - currentTime).toDouble / (1000.0 * 60 * 60)
      rate * (24.0 / math.max(hoursToSettle, 1.0))

/** 标记价格 */
final case class MarkPrice(
    exchange: Exchange,
    symbol: Symbol,
    price: Price,
    timestamp: Timestamp,
)

/** 指数价格 */
final case class IndexPrice(
    exchange: Exchange,
    symbol: Symbol,
    price: Price,
    timestamp: Timestamp,
)

/** 账户信息 (净值 + 总持仓名义价值，原子读取) */
final case class AccountInfo(
    /** 账户净值 (balance + unrealizedPnl) */
    equity: Double,
    /** 总持仓名义价值 (用于计算杠杆率) */
    notional: Double,
)

/** 交易对元数据 (精度、合约乘数) */
final case class SymbolMeta(
    exchange: Exchange,
    symbol: Symbol,
    /** 价格最小变动单位 */
    tickSize: Double,
    /** 数量最小变动单位 */
    sizeStep: Double,
    /** 最小下单数量 */
    minOrderSize: Double,
    /** 合约乘数: 每张合约对应的币本位数量 (Binance 为 1.0) */
    contractSize: Double,
):
  def isValid: Boolean = tickSize > 0 && sizeStep > 0 && contractSize > 0

  /** 币本位数量 -> 下单数量 (张) */
  def coinToQty(coinAmount: Quantity): Quantity = coinAmount / contractSize

  /** 下单数量 (张) -> 币本位数量 */
  def qtyToCoin(qty: Quantity): Quantity = qty * contractSize

  /** 价格取整到合法精度 (四舍五入到 tickSize) */
  def roundPrice(price: Price): Price =
    SymbolMeta.roundToStep(price, tickSize, BigDecimal.RoundingMode.HALF_UP)

  /** 数量向下取整到合法精度 */
  def roundSizeDown(size: Quantity): Quantity =
    SymbolMeta.roundToStep(size, sizeStep, BigDecimal.RoundingMode.FLOOR)

  /** 格式化价格为 API 请求字符串 */
  def formatPrice(price: Price): String =
    BigDecimal(roundPrice(price)).underlying.stripTrailingZeros.toPlainString

  /** 格式化数量为 API 请求字符串 */
  def formatSize(size: Quantity): String =
    BigDecimal(roundSizeDown(size)).underlying.stripTrailingZeros.toPlainString

object SymbolMeta:
  /** 用 BigDecimal 精确计算，按 step 取整 */
  private def roundToStep(value: Double, step: Double, mode: BigDecimal.RoundingMode.Value): Double =
    if step <= 0 then value
    else
      val ticks = BigDecimal(value) / BigDecimal(step)
      (ticks.setScale(0, mode) * BigDecimal(step)).toDouble
