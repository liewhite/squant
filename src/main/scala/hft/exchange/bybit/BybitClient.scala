package hft.exchange.bybit

import com.github.plokhotnyuk.jsoniter_scala.core.*
import hft.domain.*
import hft.exchange.{ExchangeClient, TradingClient}
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


  override def exchange: Exchange = Exchange.Bybit


  // ==================== ExchangeClient ====================

  override def fetchAllSymbolMetas(): Either[ExchangeError, Vector[SymbolMeta]] =
    // instruments-info 为公共端点 (免签)，分页跟进 nextPageCursor
    def loop(cursor: Option[String], acc: Vector[SymbolMeta]): Either[ExchangeError, Vector[SymbolMeta]] =
      val query =
        s"category=linear&limit=${BybitClient.InstrumentsPageLimit}" +
          cursor.map(c => s"&cursor=${URLEncoder.encode(c, UTF_8)}").getOrElse("")
      publicGet[InstrumentsResp]("/v5/market/instruments-info", query).flatMap { resp =>
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

  protected def publicGet[T: JsonValueCodec](path: String, query: String): Either[ExchangeError, T] =
    val url = if query.nonEmpty then s"$restBase$path?$query" else s"$restBase$path"
    send(Method.GET, url, Map.empty, None).flatMap(parse[T])


  protected def send(
      method: Method,
      url: String,
      headers: Map[String, String],
      body: Option[String],
  ): Either[ExchangeError, String] =
    try
      val base = basicRequest
        .method(method, Uri.unsafeParse(url))
        // 显式短超时: 必须小于策略的 orderTimeoutMs，让"超时"与"请求丢失"语义对齐
        .readTimeout(3.seconds)
        .response(asStringAlways)
      val withHeaders = headers.foldLeft(base)((r, kv) => r.header(kv._1, kv._2))
      val req = body.fold(withHeaders)(withHeaders.body)
      val response = req.send(backend)
      if response.code.isSuccess then Right(response.body)
      else Left(ExchangeError.Http(response.code.code, response.body))
    catch
      case e: Exception if isInterrupt(e) => throw e
      case e: Exception                   => Left(ExchangeError.Network(s"$method $url: ${e.getMessage}"))

  /** 异常 cause 链中是否包含线程中断 (ox 作用域取消的信号)，是则重抛而非误判为网络错误 */


  /** 异常 cause 链中是否包含线程中断 (ox 作用域取消的信号)，是则重抛而非误判为网络错误 */
  protected def isInterrupt(t: Throwable): Boolean =
    Iterator.iterate(t)(_.getCause).takeWhile(_ != null).take(10).exists {
      case _: InterruptedException | _: java.io.InterruptedIOException => true
      case _                                                           => false
    }


  protected def parse[T: JsonValueCodec](body: String): Either[ExchangeError, T] =
    try Right(readFromString[T](body))
    catch case e: Exception => Left(ExchangeError.Parse(s"${e.getMessage}; body=$body"))

  /** Bybit 顶层 retCode 校验: 0 为成功，否则映射为类型化错误 */


  /** Bybit 顶层 retCode 校验: 0 为成功，否则映射为类型化错误 */
  protected def ensureOk(retCode: Int, retMsg: String): Either[ExchangeError, Unit] =
    if retCode == 0 then Right(())
    else Left(ExchangeError.Other(s"Bybit API error: retCode=$retCode retMsg=$retMsg"))


  protected def fmt(d: Double): String =
    BigDecimal(d).underlying.stripTrailingZeros.toPlainString

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
    val (ordType, pxField) = order.orderType match
      case OrderType.Market => ("Market", "")
      case OrderType.Limit(price, tif) =>
        ("Limit", s""","price":"${fmt(price.value)}","timeInForce":"${tifToParam(tif)}"""")
    val reduceField = if order.reduceOnly then ""","reduceOnly":true""" else ""
    val linkField = if order.clientOrderId.nonEmpty then s""","orderLinkId":"${order.clientOrderId}"""" else ""
    val body =
      s"""{"category":"linear","symbol":"${order.symbol}","side":"${sideToParam(order.side)}","orderType":"$ordType","qty":"${fmt(order.quantity.value)}"$pxField$reduceField$linkField}"""
    signedPost[OrderCreateResp]("/v5/order/create", body).flatMap { resp =>
      ensureOk(resp.retCode, resp.retMsg).flatMap { _ =>
        if resp.result.orderId.nonEmpty then Right(resp.result.orderId)
        else Left(ExchangeError.Other("Bybit no orderId in response"))
      }
    }


  override def cancelOrder(symbol: Symbol, ref: OrderRef): Either[ExchangeError, Unit] =
    val idField = ref match
      case OrderRef.ByExchangeId(id) => s""""orderId":"$id""""
      case OrderRef.ByClientId(id)   => s""""orderLinkId":"$id""""
    val body = s"""{"category":"linear","symbol":"$symbol",$idField}"""
    signedPost[CancelResp]("/v5/order/cancel", body).flatMap { resp =>
      if resp.retCode == 0 then Right(())
      // 110001/170213: 订单不存在/已撤/已完成——归一为类型化错误，不外泄魔法码
      else if BybitClient.OrderNotFoundCodes.contains(resp.retCode) then
        Left(ExchangeError.OrderNotFound(s"Bybit ${resp.retCode}: ${ref.raw}"))
      else Left(ExchangeError.Other(s"Bybit cancel failed: retCode=${resp.retCode} retMsg=${resp.retMsg}"))
    }


  override def fetchPendingOrders(symbol: Symbol): Either[ExchangeError, Vector[OrderUpdate]] =
    signedGet[OpenOrdersResp]("/v5/order/realtime", s"category=linear&symbol=$symbol").flatMap { resp =>
      ensureOk(resp.retCode, resp.retMsg).map { _ =>
        resp.result.list.iterator.flatMap { d =>
          fromBybit(d.symbol).map { sym =>
            val filled = Coin(d.cumExecQty.asDouble)
            OrderUpdate(
              account = AccountId.Live,
              orderId = d.orderId,
              clientOrderId = Some(if d.orderLinkId.nonEmpty then d.orderLinkId else d.orderId),
              exchange = Exchange.Bybit,
              symbol = sym,
              side = sideFromBybit(d.side),
              status = mapOrderStatus(d.orderStatus, filled),
              price = Price(d.price.asDoubleOrZero),
              quantity = Coin(d.qty.asDouble),
              filledQuantity = filled,
              timestamp = nowMs,
            )
          }
        }.toVector
      }
    }


  override def setLeverage(symbol: Symbol, leverage: Int): Either[ExchangeError, Unit] =
    // 单向持仓模式下 buy/sell 杠杆须一致
    val body = s"""{"category":"linear","symbol":"$symbol","buyLeverage":"$leverage","sellLeverage":"$leverage"}"""
    signedPost[SimpleResp]("/v5/position/set-leverage", body).flatMap { resp =>
      if resp.retCode == 0 || resp.retCode == BybitClient.LeverageNotModifiedCode then Right(())
      else Left(ExchangeError.Other(s"Bybit set-leverage failed: retCode=${resp.retCode} retMsg=${resp.retMsg}"))
    }

  /** 净值取 wallet 的 totalEquity；notional (持仓名义价值) Bybit wallet 不直供，置 0
    * (与 OKX REST 路径一致，需精确杠杆率时由持仓聚合计算)
    */


  /** 净值取 wallet 的 totalEquity；notional (持仓名义价值) Bybit wallet 不直供，置 0
    * (与 OKX REST 路径一致，需精确杠杆率时由持仓聚合计算)
    */
  override def fetchAccountInfo(): Either[ExchangeError, AccountInfo] =
    signedGet[WalletResp]("/v5/account/wallet-balance", s"accountType=$accountType").flatMap { resp =>
      ensureOk(resp.retCode, resp.retMsg).flatMap { _ =>
        resp.result.list.headOption
          .toRight(ExchangeError.Other("Bybit no wallet data"))
          .map(w => AccountInfo(AccountId.Live, exchange, equity = w.totalEquity.asDouble))
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
              val signedSize = d.side match
                case "Buy"  => absSize
                case "Sell" => -absSize
                case _      => Coin.Zero // 空仓 side=""
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
