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

  /** 交易所**明确拒绝**了这次请求 —— 订单确定未成立 (保证金不足、价格越界、精度不符…)。
    *
    * ## 为什么必须有这个类型
    *
    * 「订单确定未成立」与「订单是否成立不确定」是两种处理方式完全相反的失败：前者是正常业务
    * 结果，以拒单回报回流策略；后者本地状态无法保证正确，必须终止进程由重启后的对齐恢复。
    *
    * 而三家交易所表达「明确拒绝」的**形状各不相同**：Binance 是 HTTP 4xx，OKX 是 HTTP 200 +
    * `data[0].sCode != "0"`，Bybit 是 HTTP 200 + `retCode != 0`。共享层曾经按 HTTP 状态码分类，
    * 于是这条判据只对 Binance 成立 —— OKX/Bybit 上一次「保证金不足」会被判成"结果不确定"
    * 而杀掉整个进程。归一到本类型之后，共享层只看语义，数字留在各交易所自己的边界里。
    *
    * @param code   交易所自己的错误码 (Binance HTTP 状态、OKX sCode、Bybit retCode)，进日志用
    */
  case Rejected(code: String, reason: String) extends ExchangeError(s"rejected[$code]: $reason")

  /** 限频 / 封禁。
    *
    * 单独成一类而不是归进 [[Rejected]]：按拒单回流会让策略重挂、于是发出更多请求，形成
    * 重试风暴。它意味着"订单生命周期自然限速"这个前提已经不成立，只能终止。 */
  case RateLimited(reason: String) extends ExchangeError(reason)

  /** 其它错误 (含结果不确定的系统错误) */
  case Other(reason: String) extends ExchangeError(reason)
