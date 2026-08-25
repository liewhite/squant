package hft.domain

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
  /** 订单不存在 (已成交/已撤/未知)：撤单时常见且非致命，终态由私有流推送。
    * 各交易所在自己的边界把专属错误码 (如 Binance -2011) 归一到此类型 */
  case OrderNotFound(reason: String) extends ExchangeError(reason)
  /** 其它错误 */
  case Other(reason: String) extends ExchangeError(reason)
