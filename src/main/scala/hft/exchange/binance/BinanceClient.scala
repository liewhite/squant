package hft.exchange.binance

import com.github.plokhotnyuk.jsoniter_scala.core.*
import hft.domain.*
import hft.exchange.ExchangeClient
import sttp.client4.*
import sttp.model.{Method, Uri}

import java.nio.charset.StandardCharsets.UTF_8
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

import BinanceCodec.*
import BinanceCodec.given

final case class BinanceCredentials(apiKey: String, apiSecret: String)

object BinanceClient:
  val RestBaseUrl = "https://fapi.binance.com"
  /** WS 基地址，路由端点 (/public/ws, /market/ws, /private/ws) 由 Connector 拼接 */
  val WsBaseUrl = "wss://fstream.binance.com"

/** Binance USDⓈ-M 合约 REST 客户端。
  *
  * 所有请求经 sttp 同步 backend 阻塞执行 (运行在虚拟线程上)。
  */
final class BinanceClient(
    backend: SyncBackend,
    credentials: Option[BinanceCredentials],
    restBase: String = BinanceClient.RestBaseUrl,
) extends ExchangeClient:

  override def exchange: Exchange = Exchange.Binance

  def hasCredentials: Boolean = credentials.isDefined

  // ==================== ExchangeClient ====================

  override def fetchAllSymbolMetas(): Either[ExchangeError, Vector[SymbolMeta]] =
    publicGet[ExchangeInfo]("/fapi/v1/exchangeInfo", Map.empty).map { info =>
      info.symbols.iterator
        .filter(s => s.status == "TRADING" && s.contractType == "PERPETUAL")
        .map { s =>
          val tickSize = s.filters.find(_.filterType == "PRICE_FILTER").map(_.tickSize.asDouble).getOrElse(0.0)
          val lotSize = s.filters.find(_.filterType == "LOT_SIZE")
          SymbolMeta(
            exchange = Exchange.Binance,
            symbol = s.symbol,
            tickSize = tickSize,
            sizeStep = lotSize.map(_.stepSize.asDouble).getOrElse(0.0),
            minOrderSize = lotSize.map(_.minQty.asDouble).getOrElse(0.0),
            contractSize = 1.0, // Binance 直接按币的数量下单
          )
        }
        .filter(_.isValid)
        .toVector
    }

  override def placeOrder(order: Order): Either[ExchangeError, OrderId] =
    val base = Map(
      "symbol" -> order.symbol,
      "side" -> sideParam(order.side),
      "quantity" -> fmt(order.quantity),
      "newClientOrderId" -> order.clientOrderId,
    ) ++ (if order.reduceOnly then Map("reduceOnly" -> "true") else Map.empty)
    val params = order.orderType match
      case OrderType.Market => base + ("type" -> "MARKET")
      case OrderType.Limit(price, tif) =>
        base + ("type" -> "LIMIT") + ("price" -> fmt(price)) + ("timeInForce" -> tifParam(tif))
    signedRequest[NewOrderResp](Method.POST, "/fapi/v1/order", params).map(_.orderId.toString)

  override def cancelOrder(symbol: Symbol, orderId: OrderId): Either[ExchangeError, Unit] =
    signedRaw(Method.DELETE, "/fapi/v1/order", Map("symbol" -> symbol, "orderId" -> orderId)).map(_ => ())

  override def fetchPendingOrders(symbol: Symbol): Either[ExchangeError, Vector[OrderUpdate]] =
    signedRequest[List[OpenOrder]](Method.GET, "/fapi/v1/openOrders", Map("symbol" -> symbol)).map {
      orders =>
        orders.iterator.map { o =>
          val filled = o.executedQty.asDouble
          OrderUpdate(
            orderId = o.orderId.toString,
            clientOrderId = Some(o.clientOrderId),
            exchange = Exchange.Binance,
            symbol = o.symbol,
            side = if o.side == "BUY" then Side.Long else Side.Short,
            status = if filled > 0 then OrderStatus.PartiallyFilled(filled) else OrderStatus.Pending,
            price = o.price.asDouble,
            quantity = o.origQty.asDouble,
            filledQuantity = filled,
            fillSize = 0.0,
            timestamp = o.time,
          )
        }.toVector
    }

  override def setLeverage(symbol: Symbol, leverage: Int): Either[ExchangeError, Unit] =
    signedRaw(Method.POST, "/fapi/v1/leverage", Map("symbol" -> symbol, "leverage" -> leverage.toString))
      .map(_ => ())

  override def fetchAccountInfo(): Either[ExchangeError, AccountInfo] =
    signedRequest[AccountResp](Method.GET, "/fapi/v2/account", Map.empty).map { account =>
      AccountInfo(
        equity = account.totalMarginBalance.asDouble,
        notional = account.positions.map(p => math.abs(p.notional.asDouble)).sum,
      )
    }

  /** REST 直查持仓 (positionRisk) */
  override def fetchPositions(): Either[ExchangeError, Vector[Position]] =
    signedRequest[List[PositionRisk]](Method.GET, "/fapi/v2/positionRisk", Map.empty).map { risks =>
      risks.iterator
        .filter(_.positionAmt.asDouble != 0.0)
        .map { p =>
          Position(
            exchange = Exchange.Binance,
            symbol = p.symbol,
            size = p.positionAmt.asDouble,
            entryPrice = p.entryPrice.asDouble,
            unrealizedPnl = p.unRealizedProfit.asDouble,
          )
        }
        .toVector
    }

  // ==================== 私有流 listenKey (供 Connector 使用) ====================

  /** 创建 user data stream 的 listenKey (仅需 API key，无需签名) */
  def createListenKey(): Either[ExchangeError, String] =
    withCredentials { c =>
      request(Method.POST, s"$restBase/fapi/v1/listenKey", Some(c.apiKey)).flatMap(parse[ListenKeyResp])
    }.map(_.listenKey)

  /** 延长当前 listenKey 有效期 (Binance 要求每 60 分钟内至少一次) */
  def keepAliveListenKey(): Either[ExchangeError, Unit] =
    withCredentials { c =>
      request(Method.PUT, s"$restBase/fapi/v1/listenKey", Some(c.apiKey))
    }.map(_ => ())

  // ==================== 请求基础设施 ====================

  private def publicGet[T: JsonValueCodec](path: String, params: Map[String, String]): Either[ExchangeError, T] =
    request(Method.GET, s"$restBase$path${queryString(params)}", apiKey = None).flatMap(parse[T])

  private def signedRequest[T: JsonValueCodec](
      method: Method,
      path: String,
      params: Map[String, String],
  ): Either[ExchangeError, T] =
    signedRaw(method, path, params).flatMap(parse[T])

  private def signedRaw(
      method: Method,
      path: String,
      params: Map[String, String],
  ): Either[ExchangeError, String] =
    withCredentials { c =>
      val query = (params + ("timestamp" -> nowMs.toString) + ("recvWindow" -> "5000"))
        .map((k, v) => s"$k=$v")
        .mkString("&")
      val signature = hmacSha256Hex(c.apiSecret, query)
      request(method, s"$restBase$path?$query&signature=$signature", Some(c.apiKey))
    }

  private def withCredentials[T](f: BinanceCredentials => Either[ExchangeError, T]): Either[ExchangeError, T] =
    credentials.toRight(ExchangeError.Auth("Binance credentials required")).flatMap(f)

  private def request(method: Method, url: String, apiKey: Option[String]): Either[ExchangeError, String] =
    try
      val base = basicRequest.method(method, Uri.unsafeParse(url)).response(asStringAlways)
      val response = apiKey.fold(base)(k => base.header("X-MBX-APIKEY", k)).send(backend)
      if response.code.isSuccess then Right(response.body)
      else Left(ExchangeError.Http(response.code.code, response.body))
    catch
      case e: InterruptedException => throw e
      case e: Exception            => Left(ExchangeError.Network(s"$method $url: ${e.getMessage}"))

  private def parse[T: JsonValueCodec](body: String): Either[ExchangeError, T] =
    try Right(readFromString[T](body))
    catch case e: Exception => Left(ExchangeError.Parse(s"${e.getMessage}; body=$body"))

  private def queryString(params: Map[String, String]): String =
    if params.isEmpty then "" else params.map((k, v) => s"$k=$v").mkString("?", "&", "")

  private def fmt(d: Double): String =
    BigDecimal(d).underlying.stripTrailingZeros.toPlainString

  private def sideParam(side: Side): String = side match
    case Side.Long  => "BUY"
    case Side.Short => "SELL"

  private def tifParam(tif: TimeInForce): String = tif match
    case TimeInForce.GTC      => "GTC"
    case TimeInForce.IOC      => "IOC"
    case TimeInForce.FOK      => "FOK"
    case TimeInForce.PostOnly => "GTX"

  private def hmacSha256Hex(secret: String, data: String): String =
    val mac = Mac.getInstance("HmacSHA256")
    mac.init(SecretKeySpec(secret.getBytes(UTF_8), "HmacSHA256"))
    mac.doFinal(data.getBytes(UTF_8)).map("%02x".format(_)).mkString
