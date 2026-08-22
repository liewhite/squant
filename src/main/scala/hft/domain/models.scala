package hft.domain

import java.util.UUID

/** 交易所枚举 */
enum Exchange:
  case Binance
  case Okx
  case Bybit

  /** 生成交易所合法的 client_order_id */
  def newClientOrderId: String =
    val hex = UUID.randomUUID().toString.replace("-", "")
    this match
      case Binance => s"0x$hex" // 34 字符, Binance 上限 36
      case Okx     => hex // 32 字符纯字母数字, OKX clOrdId 上限 32
      case Bybit   => hex // 32 字符, Bybit orderLinkId 上限 36

/** 账户身份。
  *
  * 同一份策略逻辑可以同时跑在实盘与若干模拟账户上 —— 它们看同一份行情、下同样的单，
  * 只有账户不同。因此账户**不是**策略的属性，而是装配期绑定的：策略自己不知道、
  * 也不该知道它跑在哪个账户上，否则同一份逻辑就没法既做实盘又做影子盘。
  *
  * 账户归属是**必填**的结构字段，不靠"来源即实盘"这类推断。参考实现在这里栽过：
  * 用一张手工维护的分类表判断私有事件归属，新增一个变体漏改一行就会把实盘私有事件
  * 广播给模拟策略 —— 危险侧、且编译器不报。
  */
enum AccountId:
  /** 真实交易所账户 */
  case Live
  /** 本地模拟账户 (影子盘)。同一进程可以有多个 */
  case Paper(id: Int)

  override def toString: String = this match
    case Live     => "live"
    case Paper(n) => s"paper$n"

/** 标的 = (交易所, 交易对) —— 公共行情与持仓/订单/成交的路由键。
  *
  * 独立类型而非 `(Exchange, Symbol)` 元组：它是事件路由的键，会进哈希表、进日志、
  * 进订阅声明，具名类型让这些地方读起来是"标的"而不是"某个二元组"。
  */
final case class Instrument(exchange: Exchange, symbol: Symbol):
  override def toString: String = s"$exchange:$symbol"

/** 某账户在某标的上的口子 —— **私有回报**的路由键。
  *
  * 行情按 [[Instrument]] 路由 (一份服务所有账户)，私有回报按本类型路由。于是实盘策略与
  * 影子策略即便交易同一标的，也从投递层就收不到对方的成交与订单回报 —— 订单归属由路由
  * 保证，不需要在消费侧再判一次"这笔是不是我的"。
  */
final case class AccountInstrument(account: AccountId, instrument: Instrument):
  override def toString: String = s"$account@$instrument"

/** 某账户在某交易所的口子 —— **账户级读数** (余额/净值/希腊值) 的路由键 */
final case class AccountExchange(account: AccountId, exchange: Exchange):
  override def toString: String = s"$account@$exchange"

/** 撤单时如何指名一张订单。
  *
  * 两种指名方式不是等价的备选，而对应订单生命周期的两个阶段：交易所确认之前，本地只有
  * 自己生成的 clientOrderId；确认之后才有交易所 id。撤下一个策略时它可能正好有单在途，
  * 只认交易所 id 就撤不掉那张单 —— 而策略已经停了，没有"下一次收尾"来兜底，
  * 那张 GTC 单会留在交易所无人跟踪。
  */
enum OrderRef:
  /** 交易所分配的 id */
  case ByExchangeId(id: OrderId)
  /** 我们下单时自己生成的 id (Binance origClientOrderId / OKX clOrdId / Bybit orderLinkId) */
  case ByClientId(id: String)

  def raw: String = this match
    case ByExchangeId(id) => id
    case ByClientId(id)   => id

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
    account: AccountId,
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

/** 公共成交印记 (市场上的匿名成交，非本账户成交)。
  * isBuyerMaker=true 表示买方是挂单方 -> 本笔为主动卖出 (taker 卖)。仅作策略可见的市场信号，不参与撮合。
  */
final case class MarketTrade(
    exchange: Exchange,
    symbol: Symbol,
    price: Price,
    qty: Quantity,
    isBuyerMaker: Boolean,
    timestamp: Timestamp,
)

/** 成交事件 (用于乐观更新仓位) */
final case class Fill(
    account: AccountId,
    exchange: Exchange,
    symbol: Symbol,
    side: Side,
    price: Price,
    size: Quantity,
    timestamp: Timestamp,
)

/** 仓位。size 为正表示多头，为负表示空头 */
final case class Position(
    account: AccountId,
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

  def empty(account: AccountId, exchange: Exchange, symbol: Symbol): Position =
    Position(account, exchange, symbol, 0.0, 0.0, 0.0)

/** 资产余额 */
final case class Balance(
    account: AccountId,
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

/** 账户级期权希腊字母 (按币种 ccy 聚合)。
  *
  * 这是该币种**所有期权持仓的净希腊字母**，而非单个合约——OKX `account/greeks` 直接返回此聚合值，
  * 回测的 BS 合成源亦将单合约希腊字母按持仓聚合为同一形态，使策略对实盘/回测无感。
  *
  * delta 为**原始期权 delta**；总敞口需叠加现货/合约 delta，见 `hft.state.StateManager.greeks`
  * 用 cashBal 做的修正。delta>0 表示该币种看多敞口。
  *
  * **通道规范单位 (SSOT)**：所有来源 (OKX 轮询 / BS 合成) 必须统一为——
  *   - delta/gamma: 币本位 (dPrice/dS、d²Price/dS²)
  *   - theta: **每日** 时间衰减
  *   - vega : 对 **1% (0.01)** 波动率变动的敏感度
  * 以此保证策略数值读 theta/vega 时实盘与回测一致。(OKX deltaBS/thetaBS/vegaBS 视为已遵循此约定;
  * BS 合成源在 [[hft.backtest.BsGreeksSource]] 内把数学约定的每年 theta、对 1.0 vega 换算到此约定。)
  */
final case class Greeks(
    account: AccountId,
    exchange: Exchange,
    /** 币种, e.g. "BTC" / "ETH" (非 symbol "BTCUSDT") */
    ccy: String,
    delta: Double,
    gamma: Double,
    theta: Double,
    vega: Double,
    timestamp: Timestamp,
)

/** 账户信息 (净值 + 总持仓名义价值，原子读取) */
final case class AccountInfo(
    account: AccountId,
    exchange: Exchange,
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
