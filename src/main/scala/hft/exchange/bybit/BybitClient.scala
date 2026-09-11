package hft.exchange.bybit

import com.github.plokhotnyuk.jsoniter_scala.core.*
import hft.domain.*
import hft.exchange.{ExchangeClient, RestTransport, TradingClient}
import sttp.client4.*
import sttp.model.{Method, Uri}

import java.net.URLEncoder
import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import scala.concurrent.duration.*

import BybitCodec.*
import BybitCodec.given
import hft.exchange.MetaTable

/** Bybit 凭证。签名只需 apiKey + apiSecret (无 OKX 的 passphrase)。 */
final case class BybitCredentials(apiKey: String, apiSecret: String):
  /** WebSocket 登录签名: hex(HMAC-SHA256(secret, "GET/realtime" + expires))，expires 为毫秒时间戳 */
  def signWsAuth(expires: Long): String =
    BybitClient.hmacSha256Hex(apiSecret, s"GET/realtime$expires")

object BybitClient:
  /** 只读客户端（无凭证）：只能取公共数据，私有端点在**类型上**够不着。 */
  def public(backend: SyncBackend, accountType: String = "UNIFIED"): ExchangeClient =
    new BybitPublicClient(backend, accountType, BybitClient.RestBaseUrl)

  /** 交易客户端（带凭证）：拿到它即意味着凭证已具备，无需再问 `hasCredentials`。 */
  def trading(backend: SyncBackend, credentials: BybitCredentials, accountType: String = "UNIFIED"): BybitClient =
    new BybitClient(backend, credentials, accountType, BybitClient.RestBaseUrl)
  /** 限频类 retCode。依据 Bybit v5 错误码文档 (docs/v5/error)：
    *   - 10006 Too many visits. Exceeded the API Rate Limit.
    *   - 10018 Exceeded the IP Rate Limit.
    *   - 20003 Too frequent requests under the same session
    *   - 429 / 10429 系统级频率保护
    * 它们不能按拒单回流 —— 那会变成"拒单 -> 重挂 -> 更多请求"的重试风暴。 */
  private[bybit] val RateLimitRetCodes: Set[Int] = Set(429, 10006, 10018, 10429, 20003)

  /** 一次 Bybit 请求的失败结果 —— **保留结构化的 retCode**。
    *
    * 从前 `ensureOk` 把 retCode 格式化进错误消息，`classifyWriteError` 再用正则从消息里捞回来:
    * 语义信息在同一个模块内先被丢进字符串再解析出来，两处必须同步改。根因是
    * `Either[ExchangeError, Unit]` 承载不了"交易所原始错误码"这个事实，于是拿消息文本当传输
    * 通道。收成一个类型之后，翻译只发生在 [[classify]] 一处。 */
  private[bybit] final case class Fault(retCode: Int, retMsg: String):
    def describe(what: String): String = s"Bybit $what 失败: retCode=$retCode retMsg=$retMsg"

  /** 把一次 Bybit 失败翻译成框架语义。
    *
    * @param write true = 写单路径 (下单/撤单/改杠杆)。Bybit 用 HTTP 200 + 顶层 retCode 表达
    *              业务结果，因此写单路径必须在这里分流，否则「保证金不足」会被共享层判成
    *              "结果不确定"而终止整个进程。读接口没有这个区分的必要，一律 `Other`。
    */
  private[bybit] def classify(fault: Fault, what: String, write: Boolean): ExchangeError =
    if RateLimitRetCodes.contains(fault.retCode) then ExchangeError.RateLimited(fault.describe(what))
    else if write then ExchangeError.Rejected(fault.retCode.toString, fault.describe(what))
    else ExchangeError.Other(fault.describe(what))

  val RestBaseUrl = "https://api.bybit.com"
  val WsPublicLinearUrl = "wss://stream.bybit.com/v5/public/linear"
  val WsPrivateUrl = "wss://stream.bybit.com/v5/private"

  /** 请求有效窗口 (ms)，随签名一起参与计算 */
  val RecvWindow = "5000"

  /** instruments-info 单页上限，linear 永续总数 (~600) 一页可覆盖，多余分页跟进 cursor */
  private[bybit] val InstrumentsPageLimit = 1000

  /** 撤单时表示"订单不存在/已撤/已完成"的 Bybit retCode，归一为 ExchangeError.OrderNotFound */
  private val OrderNotFoundCodes: Set[Int] = Set(110001, 170213)

  /** set-leverage 幂等：杠杆未变化视为成功 */
  private val LeverageNotModifiedCode = 110043

  /** Bybit REST/WS 签名: hex(HMAC-SHA256(secret, data))，小写十六进制 (区别于 OKX 的 base64) */
  def hmacSha256Hex(secret: String, data: String): String =
    val mac = Mac.getInstance("HmacSHA256")
    mac.init(SecretKeySpec(secret.getBytes(UTF_8), "HmacSHA256"))
    mac.doFinal(data.getBytes(UTF_8)).map(b => f"$b%02x").mkString

/** Bybit v5 USDT linear 永续 REST 客户端。所有请求经 sttp 同步 backend 阻塞执行 (运行在虚拟线程上)。
  *
  * 与 OKX 的差异：签名 hex 编码、payload = ts+apiKey+recvWindow+(queryString|body)、走 X-BAPI-* 请求头；
  * 顶层结果码 retCode 为数字 (0 成功)。与 Binance 的相同点：symbol 原生即 "BTCUSDT"、qty 为币本位 (contractSize=1.0)。
  */
/** Bybit v5 linear 永续的**公共** REST 客户端 —— 无凭证即可用。私有端点在 [[BybitClient]]。 */
class BybitPublicClient protected[bybit] (
    protected val backend: SyncBackend,
    protected val accountType: String,
    protected val restBase: String,
) extends ExchangeClient:

  /** 本客户端的规格表。子类 (交易客户端) 继承同一张 —— 它们是同一个连接口的两副面孔。 */
  override val metaTable: MetaTable = MetaTable()


  override def exchange: Exchange = Exchange.Bybit


  // ==================== ExchangeClient ====================

  /** Bybit v5 按 `category` 分口, 本适配层只接 `linear`。期权 (`category=option`) 的
    * REST 端点形状相同, 但响应侧没接: 私有流要单独订 `category=option`, 公共行情更是
    * 另一条 WS 地址 —— 只放开请求侧就是发得出单、收不到回报。 */
  override val supportedKinds: Set[InstrumentKind] = Set(InstrumentKind.LinearPerp)

  override protected def fetchSupportedMetas(kind: InstrumentKind): Either[ExchangeError, Vector[SymbolMeta]] =
    fetchLinearMetas()

  private def fetchLinearMetas(): Either[ExchangeError, Vector[SymbolMeta]] =
    // instruments-info 为公共端点 (免签)，分页跟进 nextPageCursor
    def loop(cursor: Option[String], acc: Vector[SymbolMeta]): Either[ExchangeError, Vector[SymbolMeta]] =
      val query =
        s"category=linear&limit=${BybitClient.InstrumentsPageLimit}" +
          cursor.map(c => s"&cursor=${URLEncoder.encode(c, UTF_8)}").getOrElse("")
      // 合约清单响应大, 与下单路径的时限无关
      publicGet[InstrumentsResp]("/v5/market/instruments-info", query, RestTransport.QueryTimeout).flatMap { resp =>
        ensureOk(resp.retCode, resp.retMsg).flatMap { _ =>
          val metas = resp.result.list.iterator.flatMap { d =>
            fromBybit(d.symbol).map { sym =>
              SymbolMeta(
                exchange = Exchange.Bybit,
                symbol = sym,
                tickSize = d.priceFilter.tickSize.asDouble,
                sizeStep = d.lotSizeFilter.qtyStep.asDouble,
                minOrderSize = d.lotSizeFilter.minOrderQty.asDouble,
                contractSize = 1.0, // linear 永续 qty 即币本位，无合约乘数
              )
            }
          }.filter(_.isValid).toVector
          val next = resp.result.nextPageCursor
          if next.nonEmpty then loop(Some(next), acc ++ metas) else Right(acc ++ metas)
        }
      }
    loop(None, Vector.empty)


  // ==================== 请求基础设施 ====================

  protected def publicGet[T: JsonValueCodec](
      path: String,
      query: String,
      timeout: scala.concurrent.duration.FiniteDuration = RestTransport.ReadTimeout,
  ): Either[ExchangeError, T] =
    val url = if query.nonEmpty then s"$restBase$path?$query" else s"$restBase$path"
    send(Method.GET, url, Map.empty, None, timeout).flatMap(parse[T])


  protected def send(
      method: Method,
      url: String,
      headers: Map[String, String],
      body: Option[String],
      timeout: scala.concurrent.duration.FiniteDuration = RestTransport.ReadTimeout,
  ): Either[ExchangeError, String] =
    RestTransport.send(backend, method, url, headers, body, timeout)

  protected def parse[T: JsonValueCodec](body: String): Either[ExchangeError, T] = RestTransport.parse[T](body)

  /** 读接口的顶层 retCode 校验: 0 为成功，非 0 是一次失败的查询。限频仍单独归类。 */
  protected def ensureOk(retCode: Int, retMsg: String, what: String = "API"): Either[ExchangeError, Unit] =
    if retCode == 0 then Right(())
    else Left(BybitClient.classify(BybitClient.Fault(retCode, retMsg), what, write = false))

  /** 写单路径 (下单/撤单/改杠杆) 的顶层 retCode 校验 —— 非 0 归 `Rejected`/`RateLimited`。 */
  protected def ensureWriteOk(retCode: Int, retMsg: String, what: String): Either[ExchangeError, Unit] =
    if retCode == 0 then Right(())
    else Left(BybitClient.classify(BybitClient.Fault(retCode, retMsg), what, write = true))

  protected def fmt(d: Double): String = RestTransport.fmt(d)

/** Bybit v5 linear 永续的**交易** REST 客户端。凭证是构造参数而非 `Option`，签名路径因此没有
  * "万一没配密钥"的分支，`ExchangeError.Auth` 回归它本来的含义：交易所真的拒绝了鉴权。 */
final class BybitClient private[bybit] (
    backend: SyncBackend,
    val credentials: BybitCredentials,
    accountType: String,
    restBase: String,
) extends BybitPublicClient(backend, accountType, restBase),
      TradingClient:


  /** 供账户流构建 WS auth 帧使用 */
  def wsCredentials: BybitCredentials = credentials

  // ==================== ExchangeClient ====================




  override def placeOrder(order: ExchangeOrder): Either[ExchangeError, OrderId] =
    val symbol = linearSymbol(order.instrument)
    val (ordType, pxField) = order.orderType match
      case OrderType.Market => ("Market", "")
      case OrderType.Limit(price, tif) =>
        ("Limit", s""","price":"${fmt(price.value)}","timeInForce":"${tifToParam(tif)}"""")
    val reduceField = if order.reduceOnly then ""","reduceOnly":true""" else ""
    val linkField = if order.clientOrderId.nonEmpty then s""","orderLinkId":"${order.clientOrderId}"""" else ""
    val body =
      s"""{"category":"linear","symbol":"$symbol","side":"${sideToParam(order.side)}","orderType":"$ordType","qty":"${fmt(order.quantity.value)}"$pxField$reduceField$linkField}"""
    signedPost[OrderCreateResp]("/v5/order/create", body).flatMap { resp =>
      ensureWriteOk(resp.retCode, resp.retMsg, "下单").flatMap { _ =>
        if resp.result.orderId.nonEmpty then Right(resp.result.orderId)
        else Left(ExchangeError.Other("Bybit no orderId in response"))
      }
    }


  /** Bybit 的 symbol 就是原生交易对；品种由 [[supportedKinds]] 那一处守。 */
  private def linearSymbol(instrument: Instrument): Symbol = requireSupported(instrument).symbol

  override def cancelOrder(instrument: Instrument, ref: OrderRef): Either[ExchangeError, Unit] =
    val symbol = linearSymbol(instrument)
    val idField = ref match
      case OrderRef.ByExchangeId(id) => s""""orderId":"$id""""
      case OrderRef.ByClientId(id)   => s""""orderLinkId":"$id""""
    val body = s"""{"category":"linear","symbol":"$symbol",$idField}"""
    signedPost[CancelResp]("/v5/order/cancel", body).flatMap { resp =>
      if resp.retCode == 0 then Right(())
      // 110001/170213: 订单不存在/已撤/已完成——归一为类型化错误，不外泄魔法码
      else if BybitClient.OrderNotFoundCodes.contains(resp.retCode) then
        Left(ExchangeError.OrderNotFound(s"Bybit ${resp.retCode}: ${ref.raw}"))
      else Left(BybitClient.classify(BybitClient.Fault(resp.retCode, resp.retMsg), "撤单", write = true))
    }


  /** 当前挂单。**必须翻页** —— `/v5/order/realtime` 默认每页 20 条并给出 `nextPageCursor`。
    *
    * 不翻页的后果不是"少看几张单": 对齐时 [[PositionBook.align]] 会漏掉部分成交单的记账进度,
    * 于是下一条推送把已经含在仓位里的成交**再记一遍**。同文件的 fetchMetas 本就跟着
    * cursor 走, 这里漏了。 */
  override def fetchPendingOrders(instrument: Instrument): Either[ExchangeError, Vector[OrderUpdate]] =
    val symbol = linearSymbol(instrument)
    def loop(cursor: Option[String], acc: Vector[OrderUpdate]): Either[ExchangeError, Vector[OrderUpdate]] =
      val query = s"category=linear&symbol=$symbol" + cursor.fold("")(c => s"&cursor=$c")
      signedGet[OpenOrdersResp]("/v5/order/realtime", query).flatMap { resp =>
        ensureOk(resp.retCode, resp.retMsg).flatMap { _ =>
          val page = resp.result.list.iterator.flatMap { d =>
            fromBybit(d.symbol).map { sym =>
              val filled = Coin(d.cumExecQty.asDouble)
              OrderUpdate(
                account = AccountId.Live,
                orderId = d.orderId,
                // 没有 orderLinkId 的单 (手工下的) 就是没有 —— 拿 orderId 冒充会在策略的
                // pending 索引里凭空造出一个不存在的键, 而 WS 路径给的是 None。
                clientOrderId = Option.when(d.orderLinkId.nonEmpty)(d.orderLinkId),
                exchange = Exchange.Bybit,
                symbol = sym,
                side = sideFromBybit(d.side),
                status = mapOrderStatus(d.orderStatus, filled),
                price = Price(d.price.asDoubleOrZero), // 市价单的委托价为空, 只用于回显
                quantity = Coin(d.qty.asDouble),
                filledQuantity = filled,
                reduceOnly = RestTransport.requireFlag(d.reduceOnly, "Bybit", "reduceOnly", s"orderId=${d.orderId}"),
                timestamp = d.updatedTime.toLongOption.getOrElse(
                  throw IllegalStateException(s"Bybit order updatedTime 不是时间戳: '${d.updatedTime}' (${d.orderId})")
                ),
              )
            }
          }.toVector
          val next = resp.result.nextPageCursor
          if next.nonEmpty then loop(Some(next), acc ++ page) else Right(acc ++ page)
        }
      }
    loop(None, Vector.empty)



  /** 净值取 wallet 的 totalEquity；notional (持仓名义价值) Bybit wallet 不直供，置 0
    * (与 OKX REST 路径一致，需精确杠杆率时由持仓聚合计算)
    */
  override def fetchAccountInfo(): Either[ExchangeError, AccountInfo] =
    wallet().map(w => AccountInfo(AccountId.Live, exchange, equity = w.totalEquity.asDouble))

  /** 完整钱包: `/v5/account/wallet-balance` 一次返回整份币种明细。
    * **必须靠它**建立钱包 —— wallet 频道文档明确写着订阅成功时不给 snapshot。 */
  override def fetchWallet(): Either[ExchangeError, Map[String, Double]] =
    wallet().map(_.coin.map(c => c.coin -> c.walletBalance.asDouble).toMap)

  /** 净值与币种明细来自同一个响应 —— 分两次拉会拿到两个时刻的账户状态。 */
  private def wallet(): Either[ExchangeError, BybitCodec.WalletData] =
    signedGet[WalletResp]("/v5/account/wallet-balance", s"accountType=$accountType").flatMap { resp =>
      ensureOk(resp.retCode, resp.retMsg, "查钱包").flatMap { _ =>
        resp.result.list.headOption.toRight(ExchangeError.Other("Bybit no wallet data"))
      }
    }

  /** 启动期持仓对齐：REST 直查 (settleCoin=USDT 取所有 USDT 永续持仓)，方向由 side 决定符号 */


  /** 启动期持仓对齐：REST 直查 (settleCoin=USDT 取所有 USDT 永续持仓)，方向由 side 决定符号 */
  override def fetchPositions(): Either[ExchangeError, Vector[Position]] =
    signedGet[PositionListResp]("/v5/position/list", s"category=linear&settleCoin=$USDT").flatMap { resp =>
        ensureOk(resp.retCode, resp.retMsg).map { _ =>
          resp.result.list.iterator.flatMap { d =>
            fromBybit(d.symbol).map { sym =>
              // Bybit linear 的原生数量就是币本位 (contractSize = 1)，与 WS 路径一致
              val absSize = Coin(d.size.asDouble)
              // 穷举: 未知方向从前归零, 而 size 非零时那等于把一笔真实持仓丢掉。
              // WS 路径对同一件事的答案是"记成多头" —— 同一事实两个答案, 一并收口。
              val signedSize = d.side match
                case "Buy"                                      => absSize
                case "Sell"                                     => -absSize
                // 空仓: side 为空串 (docs/v5/position)。非空 size 配空 side 是矛盾, 落到下面抛。
                case "" if absSize.value == 0.0                 => Coin.Zero
                case other =>
                  throw IllegalStateException(s"未知的 Bybit 持仓方向: '$other' (symbol=${d.symbol} size=${d.size})")
              Position(
                account = AccountId.Live,
                exchange = Exchange.Bybit,
                symbol = sym,
                size = signedSize,
              )
            }
          }.toVector
        }
      }

  // ==================== 请求基础设施 ====================


  private def signedGet[T: JsonValueCodec](path: String, query: String): Either[ExchangeError, T] =
    request(Method.GET, path, query, body = "").flatMap(parse[T])


  private def signedPost[T: JsonValueCodec](path: String, body: String): Either[ExchangeError, T] =
    request(Method.POST, path, query = "", body = body).flatMap(parse[T])

  /** 统一请求。signed=true 时附 Bybit 签名头 (payload = ts+apiKey+recvWindow+(GET query | POST body))，
    * 缺凭证返回 Auth 错误。GET 的 query 既拼进 URL 也参与签名，须严格一致。
    */


  /** 统一请求。signed=true 时附 Bybit 签名头 (payload = ts+apiKey+recvWindow+(GET query | POST body))，
    * 缺凭证返回 Auth 错误。GET 的 query 既拼进 URL 也参与签名，须严格一致。
    */
  private def request(
      method: Method,
      path: String,
      query: String,
      body: String,
  ): Either[ExchangeError, String] =
    val c = credentials
    val ts = Instant.now().toEpochMilli.toString
    val signContent = if method == Method.GET then query else body
    val prehash = ts + c.apiKey + BybitClient.RecvWindow + signContent
    val base = Map(
      "X-BAPI-API-KEY" -> c.apiKey,
      "X-BAPI-SIGN" -> BybitClient.hmacSha256Hex(c.apiSecret, prehash),
      "X-BAPI-TIMESTAMP" -> ts,
      "X-BAPI-RECV-WINDOW" -> BybitClient.RecvWindow,
    )
    val headers = if method == Method.GET then base else base + ("Content-Type" -> "application/json")
    val url = if query.nonEmpty then s"$restBase$path?$query" else s"$restBase$path"
    send(method, url, headers, if method == Method.GET then None else Some(body))
