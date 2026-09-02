package hft.exchange.binance

import com.github.plokhotnyuk.jsoniter_scala.core.*
import hft.domain.*
import hft.exchange.{ExchangeClient, TradingClient}
import sttp.client4.*
import sttp.model.{Method, Uri}

import java.nio.charset.StandardCharsets.UTF_8
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import scala.concurrent.duration.*

import BinanceCodec.*
import BinanceCodec.given

final case class BinanceCredentials(apiKey: String, apiSecret: String)

object BinanceClient:
  /** 只读客户端（无凭证）：只有公共端点，私有面在**类型上**够不着。 */
  /** 返回具体类型而非 `ExchangeClient`：合约清单里有些事实是币安独有的
    * (如传统资产永续的分类，见 [[BinancePublicClient.fetchTradFiPerps]])，
    * 统一接口不该为了一家的分类法长出一个字段，调用方也不该为此再发一次请求。 */
  def public(backend: SyncBackend, restBase: String = RestBaseUrl): BinancePublicClient =
    new BinancePublicClient(backend, restBase)

  /** 交易客户端（带凭证）：凭证是构造参数而不是 `Option`，
    * 于是签名路径里没有"万一没有凭证"这个分支要处理。 */
  def trading(backend: SyncBackend, credentials: BinanceCredentials, restBase: String = RestBaseUrl): BinanceClient =
    new BinanceClient(backend, credentials, restBase)
  val RestBaseUrl = "https://fapi.binance.com"
  /** WS 基地址，路由端点 (/public/ws, /market/ws, /private/ws) 由 Connector 拼接 */
  val WsBaseUrl = "wss://fstream.binance.com"

/** Binance USDⓈ-M 合约的**公共** REST 客户端 —— 无凭证即可用。
  *
  * 所有请求经 sttp 同步 backend 阻塞执行 (运行在虚拟线程上)。
  * 私有端点在 [[BinanceClient]] 上，那里凭证是构造参数。
  */
class BinancePublicClient protected[binance] (
    protected val backend: SyncBackend,
    protected val restBase: String,
) extends ExchangeClient:

  override def exchange: Exchange = Exchange.Binance

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

  /** 本所上市的**传统资产永续** (contractType = `TRADIFI_PERPETUAL`)：标的资产代码 -> 合约 symbol。
    *
    * 覆盖股票、ETF、商品、外汇与盘前 —— 币安用 `underlyingType` 再细分 (EQUITY / COMMODITY /
    * KR_EQUITY / PREMARKET)，本方法不做这层区分：使用方 (跨所价差) 关心的是"这个代码在别的所
    * 有没有同一个标的"，而不是它属于哪一类。
    *
    * 键取 `baseAsset` 而不是从 symbol 上剥掉计价币：剥字符串要先假定后缀，而 `baseAsset`
    * 是币安自己给出的答案。
    *
    * **不并入 [[fetchAllSymbolMetas]]**：那里的口径是加密永续 (`PERPETUAL`)，合并会静默改变
    * 既有调用方 (全市场扫描器) 的标的集合；而且传统资产永续有交易时段与休市停更，
    * 与 7×24 的加密永续混进同一张表，后来者就分不出哪些标的会整段没有行情。
    */
  def fetchTradFiPerps(): Either[ExchangeError, Map[String, Symbol]] =
    publicGet[ExchangeInfo]("/fapi/v1/exchangeInfo", Map.empty).map { info =>
      info.symbols.iterator
        .filter(s => s.status == "TRADING" && s.contractType == "TRADIFI_PERPETUAL" && s.baseAsset.nonEmpty)
        .map(s => s.baseAsset -> s.symbol)
        .toMap
    }

  protected def publicGet[T: JsonValueCodec](path: String, params: Map[String, String]): Either[ExchangeError, T] =
    request(Method.GET, s"$restBase$path${queryString(params)}", apiKey = None).flatMap(parse[T])

  protected def request(method: Method, url: String, apiKey: Option[String]): Either[ExchangeError, String] =
    try
      val base = basicRequest
        .method(method, Uri.unsafeParse(url))
        // 显式短超时: 必须小于策略的 orderTimeoutMs，让"超时"与"请求丢失"语义对齐
        .readTimeout(3.seconds)
        .response(asStringAlways)
      val response = apiKey.fold(base)(k => base.header("X-MBX-APIKEY", k)).send(backend)
      if response.code.isSuccess then Right(response.body)
      else Left(ExchangeError.Http(response.code.code, response.body))
    catch
      // 作用域取消 (可能被 sttp 包裹) 必须重抛，不能误判为网络错误
      case e: Exception if isInterrupt(e) => throw e
      case e: Exception                   => Left(ExchangeError.Network(s"$method $url: ${e.getMessage}"))

  /** 异常 cause 链中是否包含线程中断 (ox 作用域取消的信号) */
  protected def isInterrupt(t: Throwable): Boolean =
    Iterator.iterate(t)(_.getCause).takeWhile(_ != null).take(10).exists {
      case _: InterruptedException | _: java.io.InterruptedIOException => true
      case _                                                           => false
    }

  protected def parse[T: JsonValueCodec](body: String): Either[ExchangeError, T] =
    try Right(readFromString[T](body))
    catch case e: Exception => Left(ExchangeError.Parse(s"${e.getMessage}; body=$body"))

  protected def queryString(params: Map[String, String]): String =
    if params.isEmpty then "" else params.map((k, v) => s"$k=$v").mkString("?", "&", "")

  protected def fmt(d: Double): String =
    BigDecimal(d).underlying.stripTrailingZeros.toPlainString

final class BinanceClient private[binance] (
    backend: SyncBackend,
    protected val credentials: BinanceCredentials,
    restBase: String,
) extends BinancePublicClient(backend, restBase),
      TradingClient:

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
    locally {
      val c = credentials
      val query = (params + ("timestamp" -> nowMs.toString) + ("recvWindow" -> "5000"))
        .map((k, v) => s"$k=$v")
        .mkString("&")
      val signature = hmacSha256Hex(c.apiSecret, query)
      request(method, s"$restBase$path?$query&signature=$signature", Some(c.apiKey))
    }


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

/** Binance USDⓈ-M 合约的**交易** REST 客户端。
  *
  * 凭证是构造参数而非 `Option` —— 拿到本类型即证明凭证具备，签名路径因此没有
  * "万一没配密钥"的分支，`ExchangeError.Auth` 也回归它本来的含义：交易所真的拒绝了鉴权。
  */




  override def placeOrder(order: ExchangeOrder): Either[ExchangeError, OrderId] =
    val base = Map(
      "symbol" -> order.symbol,
      "side" -> sideParam(order.side),
      "quantity" -> fmt(order.quantity.value),
      "newClientOrderId" -> order.clientOrderId,
    ) ++ (if order.reduceOnly then Map("reduceOnly" -> "true") else Map.empty)
    val params = order.orderType match
      case OrderType.Market => base + ("type" -> "MARKET")
      case OrderType.Limit(price, tif) =>
        base + ("type" -> "LIMIT") + ("price" -> fmt(price.value)) + ("timeInForce" -> tifParam(tif))
    signedRequest[NewOrderResp](Method.POST, "/fapi/v1/order", params).map(_.orderId.toString)

  override def cancelOrder(symbol: Symbol, ref: OrderRef): Either[ExchangeError, Unit] =
    val idParam = ref match
      case OrderRef.ByExchangeId(id) => "orderId" -> id
      case OrderRef.ByClientId(id)   => "origClientOrderId" -> id
    signedRaw(Method.DELETE, "/fapi/v1/order", Map("symbol" -> symbol, idParam)) match
      case Right(_) => Right(())
      // Binance -2011 Unknown order: 已成交/已撤——在交易所边界归一为类型化错误，不外泄魔法码
      case Left(ExchangeError.Http(_, body)) if body.contains("-2011") =>
        Left(ExchangeError.OrderNotFound(s"Binance -2011 unknown order: ${ref.raw}"))
      case Left(e) => Left(e)

  override def fetchPendingOrders(symbol: Symbol): Either[ExchangeError, Vector[OrderUpdate]] =
    signedRequest[List[OpenOrder]](Method.GET, "/fapi/v1/openOrders", Map("symbol" -> symbol)).map {
      orders =>
        orders.iterator.map { o =>
          // Binance USDⓈ-M 的原生数量就是**币本位** (contractSize = 1)，与 WS 路径一致
          val filled = Coin(o.executedQty.asDouble)
          OrderUpdate(
            account = AccountId.Live,
            orderId = o.orderId.toString,
            clientOrderId = Some(o.clientOrderId),
            exchange = Exchange.Binance,
            symbol = o.symbol,
            side = if o.side == "BUY" then Side.Long else Side.Short,
            status = if filled.nonZero then OrderStatus.PartiallyFilled(filled) else OrderStatus.Pending,
            price = o.price.asPrice,
            quantity = Coin(o.origQty.asDouble),
            filledQuantity = filled,
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
        account = AccountId.Live,
        exchange,
        equity = account.totalMarginBalance.asDouble,
      )
    }

  /** REST 直查持仓 (positionRisk) */
  override def fetchPositions(): Either[ExchangeError, Vector[Position]] =
    signedRequest[List[PositionRisk]](Method.GET, "/fapi/v2/positionRisk", Map.empty).map { risks =>
      risks.iterator
        .filter(_.positionAmt.asDouble != 0.0)
        .map { p =>
          Position(
            account = AccountId.Live,
            exchange = Exchange.Binance,
            symbol = p.symbol,
            size = Coin(p.positionAmt.asDouble),
          )
        }
        .toVector
    }

  // ==================== 私有流 listenKey (供 Connector 使用) ====================

  /** 创建 user data stream 的 listenKey (仅需 API key，无需签名) */
  def createListenKey(): Either[ExchangeError, String] =
    locally {
      val c = credentials
      request(Method.POST, s"$restBase/fapi/v1/listenKey", Some(c.apiKey)).flatMap(parse[ListenKeyResp])
    }.map(_.listenKey)

  /** 延长当前 listenKey 有效期 (Binance 要求每 60 分钟内至少一次) */
  def keepAliveListenKey(): Either[ExchangeError, Unit] =
    locally {
      val c = credentials
      request(Method.PUT, s"$restBase/fapi/v1/listenKey", Some(c.apiKey))
    }.map(_ => ())

  // ==================== 请求基础设施 ====================

