package hft.exchange

import com.github.plokhotnyuk.jsoniter_scala.core.*
import hft.domain.ExchangeError
import sttp.client4.*
import sttp.model.{Method, Uri}

import scala.concurrent.duration.*

/** 各交易所 REST 客户端共用的传输层：**超时、中断识别、错误归类、JSON 解析、出站数字格式**。
  *
  * ## 为什么收成一处
  *
  * 这五件事与交易所无关，从前在 Binance/OKX/Bybit/Hyperliquid 四个客户端里各写一遍
  * (`isInterrupt` 四份逐字相同、`parse` 三份、`fmt` 三份，`readTimeout(3.seconds)` 三处魔法值)。
  * 同一逻辑四份副本的代价不是行数，而是**改一处、忘三处**：
  * "超时必须小于策略的 orderTimeoutMs"这条约束写在框架文档里，却没有任何一处代码承载它。
  *
  * 交易所各自独有的部分 —— 签名、路径、业务错误码的语义 —— **不在这里**，仍由各客户端负责。
  */
object RestTransport:

  /** REST 读超时。
    *
    * **必须小于策略的 `orderTimeoutMs`**，让"本地超时"与"请求可能已丢失"两种语义对齐：
    * 若本地等得比策略的订单超时还久，策略会先判定订单丢失、而请求其实还在路上。 */
  val ReadTimeout: FiniteDuration = 3.seconds

  /** 查询类接口的读超时。合约列表这类响应大 (几百个标的)，与下单路径的时限无关。 */
  val QueryTimeout: FiniteDuration = 10.seconds

  /** 发一次 HTTP 请求，取回原始响应体。
    *
    * 非 2xx 一律归类：429/418 归 [[ExchangeError.RateLimited]] (HTTP 层的限频信号三家一致)，
    * 其余归 [[ExchangeError.Http]] —— **业务语义留给各交易所自己判**，因为同一个 HTTP 状态在
    * 三家的含义不同 (Binance 用 4xx 表达业务拒单，OKX/Bybit 用 200 + 业务码)。
    *
    * 作用域取消 (被 sttp 包裹的 `InterruptedException`) 必须重抛，不能误判为网络错误：
    * 那会把一次正常停机变成一条"网络失败"日志，并让停机路径继续往下跑。
    */
  def send(
      backend: SyncBackend,
      method: Method,
      url: String,
      headers: Map[String, String] = Map.empty,
      body: Option[String] = None,
      timeout: FiniteDuration = ReadTimeout,
  ): Either[ExchangeError, String] =
    try
      val base = basicRequest
        .method(method, Uri.unsafeParse(url))
        .readTimeout(timeout)
        .response(asStringAlways)
      val withHeaders = headers.foldLeft(base)((r, kv) => r.header(kv._1, kv._2))
      val request = body.fold(withHeaders)(withHeaders.body)
      val response = request.send(backend)
      if response.code.isSuccess then Right(response.body)
      else if isRateLimitStatus(response.code.code) then
        Left(ExchangeError.RateLimited(s"HTTP ${response.code.code}: ${response.body}"))
      else Left(ExchangeError.Http(response.code.code, response.body))
    catch
      case e: Exception if isInterrupt(e) => throw e
      case e: Exception                   => Left(ExchangeError.Network(s"$method $url: ${e.getMessage}"))

  /** 429 Too Many Requests / 418 (Binance 的封禁码)。
    *
    * 依据：Binance 用 429 与 418 (`/fapi` 文档)；Bybit 与 OKX 的 REST 限频同样以 HTTP 429 返回
    * (两家文档均如此写明)。三家一致，因此这一层可以判。 */
  private def isRateLimitStatus(status: Int): Boolean = status == 429 || status == 418

  /** 异常 cause 链中是否包含线程中断 (ox 作用域取消的信号)。链深度设上限，避免自引用 cause 死循环。 */
  def isInterrupt(t: Throwable): Boolean =
    Iterator.iterate(t)(_.getCause).takeWhile(_ != null).take(10).exists {
      case _: InterruptedException | _: java.io.InterruptedIOException => true
      case _                                                           => false
    }

  /** 解析 JSON 响应体。失败带上原始报文 —— 少了它，"解析失败"这条日志无法定位。 */
  def parse[T: JsonValueCodec](body: String): Either[ExchangeError, T] =
    try Right(readFromString[T](body))
    catch case e: Exception => Left(ExchangeError.Parse(s"${e.getMessage}; body=$body"))

  /** 读一个**契约上必返**的布尔字段。缺失即抛。
    *
    * jsoniter 对缺失字段取默认值，于是 `Boolean = false` 会把"报文没带"变成"值是 false" ——
    * 正是要消灭的默认值填充。因此这类字段在 codec 里声明为 `Option`，读取时经这里，
    * 让"缺失"变成一次带上下文的失败。 */
  def requireFlag(value: Option[Boolean], exchange: String, field: String, context: => String): Boolean =
    value.getOrElse(
      throw IllegalStateException(s"$exchange 报文缺字段 $field ($context) —— 它不是可选项, 不能当成 false")
    )

  /** 出站数字格式的**单一数据源**：定点、无科学计数、去掉尾随零。
    *
    * `Double.toString` 会在小量级上给出 `1.0E-4` 这种科学计数法，交易所一律拒收；
    * 尾随零则会在某些交易所触发精度校验。 */
  def fmt(d: Double): String = BigDecimal(d).underlying.stripTrailingZeros.toPlainString
