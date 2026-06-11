package hft.domain

/** 数量类型 (持仓数量、订单数量) */
type Quantity = Double

/** 价格类型 */
type Price = Double

/** 费率类型 */
type Rate = Double

/** 订单 ID */
type OrderId = String

/** 交易对标识 (各交易所统一格式, 如 "BTCUSDT") */
type Symbol = String

/** 时间戳 (毫秒级 Unix 时间戳) */
type Timestamp = Long

/** 资产常量 */
val USDT = "USDT"

/** 获取当前时间戳 (毫秒) */
def nowMs: Timestamp = System.currentTimeMillis()

/** 交易所交互错误 */
enum ExchangeError(val message: String):
  /** HTTP 错误响应 */
  case Http(status: Int, body: String) extends ExchangeError(s"HTTP $status: $body")
  /** 网络错误 */
  case Network(reason: String) extends ExchangeError(reason)
  /** 响应解析失败 */
  case Parse(reason: String) extends ExchangeError(reason)
  /** 鉴权失败 / 缺少凭证 */
  case Auth(reason: String) extends ExchangeError(reason)
  /** 其它错误 */
  case Other(reason: String) extends ExchangeError(reason)
