package hft.exchange.binance

import com.github.plokhotnyuk.jsoniter_scala.core.*
import hft.domain.*
import hft.exchange.{ExchangeClient, RestTransport, TradingClient}
import sttp.client4.*
import sttp.model.Method

import java.nio.charset.StandardCharsets.UTF_8
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

import BinanceCodec.*
import BinanceCodec.given

final case class BinanceCredentials(apiKey: String, apiSecret: String)

object BinanceClient:
  /** 撮合引擎超时 —— **执行状态未知**。
    *
    * Binance 文档原文: HTTP 408 "used when a timeout has occurred while waiting for a response
    * from the backend server"，对应业务码 `-1007 TIMEOUT`: "Send status unknown; execution
    * status unknown."，且明确写着 "This does not always mean that the request failed in the
    * Matching Engine."
    *
    * 它落在 4xx 里，但语义与"明确拒绝"**相反**：按拒单回流会清掉 pending 登记、策略随后重下
    * 一单，而原来那一单可能真的活在交易所 —— 幽灵挂单加双倍敞口。因此必须从 Rejected 里摘出来。 */
  private val UnknownOutcomeStatus: Int = 408
  private val UnknownOutcomeCode: String = "-1007"

  /** 写单路径的错误归类 —— **纯函数**，可直接断言。
    *
    * Binance 用 HTTP 4xx 表达业务拒单 (请求没过校验、订单确定未进撮合)，唯一的例外是
    * 408/-1007 (见 [[UnknownOutcomeStatus]])。5xx 与网络错误结果同样不确定，原样保留。
    * 限频 (429/418) 已由 [[RestTransport]] 在 HTTP 层归类。 */
  def classifyWriteError(e: ExchangeError): ExchangeError = e match
    case ExchangeError.Http(status, body)
        if status == UnknownOutcomeStatus || body.contains(UnknownOutcomeCode) =>
      // 结果不确定: 保持原类型, 让共享层走"终止"通道, 由重启后的对齐恢复
      e
    case ExchangeError.Http(status, body) if status >= 400 && status < 500 =>
      ExchangeError.Rejected(status.toString, body)
    case other => other

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

  /** 本客户端接的是 USDⓈ-M 永续。币安的期权是另一套 API (eapi), 框架没接。 */
  override def fetchMetas(kind: InstrumentKind): Either[ExchangeError, Vector[SymbolMeta]] =
    if kind != InstrumentKind.LinearPerp then
      Left(ExchangeError.Rejected("unsupported", s"Binance 适配层只接 USDⓈ-M 永续, 拿不到 $kind 的规格"))
    else fetchPerpMetas()

  private def fetchPerpMetas(): Either[ExchangeError, Vector[SymbolMeta]] =
    // exchangeInfo 是三家里最大的响应 (几百个标的), 与下单路径的时限无关
    publicGet[ExchangeInfo]("/fapi/v1/exchangeInfo", Map.empty, RestTransport.QueryTimeout).map { info =>
      info.symbols.iterator
        .filter(s => s.status == "TRADING" && s.contractType == "PERPETUAL")
        .map { s =>
          // 缺 filter 即抛并带上 symbol 与字段名。
          //
          // 从前是 `getOrElse(0.0)` 再由 `.filter(_.isValid)` 滤掉 —— 一个报文异常的 symbol
          // 会**从规格表里静默消失**, 直到对齐时报"没有合约规格 (装配时未加载?)"。
          // 故障点被搬走了, 而且那句提示还是误导的。
          def filterOf(kind: String) =
            s.filters
              .find(_.filterType == kind)
              .getOrElse(throw IllegalStateException(s"Binance ${s.symbol} 的 exchangeInfo 缺 $kind filter"))
          val lotSize = filterOf("LOT_SIZE")
          SymbolMeta(
            exchange = Exchange.Binance,
            symbol = s.symbol,
            tickSize = filterOf("PRICE_FILTER").tickSize.asDouble,
            sizeStep = lotSize.stepSize.asDouble,
            minOrderSize = lotSize.minQty.asDouble,
            contractSize = 1.0, // Binance 直接按币的数量下单
          )
        }
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
    * **不并入 [[fetchMetas]] 的永续口径**：那里的口径是加密永续 (`PERPETUAL`)，合并会静默改变
    * 既有调用方 (全市场扫描器) 的标的集合；而且传统资产永续有交易时段与休市停更，
    * 与 7×24 的加密永续混进同一张表，后来者就分不出哪些标的会整段没有行情。
    */
  def fetchTradFiPerps(): Either[ExchangeError, Map[String, Symbol]] =
    publicGet[ExchangeInfo]("/fapi/v1/exchangeInfo", Map.empty, RestTransport.QueryTimeout).map { info =>
      info.symbols.iterator
        .filter(s => s.status == "TRADING" && s.contractType == "TRADIFI_PERPETUAL" && s.baseAsset.nonEmpty)
        .map(s => s.baseAsset -> s.symbol)
        .toMap
    }

  protected def publicGet[T: JsonValueCodec](
      path: String,
      params: Map[String, String],
      timeout: scala.concurrent.duration.FiniteDuration = RestTransport.ReadTimeout,
  ): Either[ExchangeError, T] =
    request(Method.GET, s"$restBase$path${queryString(params)}", apiKey = None, timeout).flatMap(parse[T])

  protected def request(
      method: Method,
      url: String,
      apiKey: Option[String],
      timeout: scala.concurrent.duration.FiniteDuration = RestTransport.ReadTimeout,
  ): Either[ExchangeError, String] =
    RestTransport.send(
      backend,
      method,
      url,
      headers = apiKey.fold(Map.empty)(k => Map("X-MBX-APIKEY" -> k)),
      timeout = timeout,
    )

  protected def parse[T: JsonValueCodec](body: String): Either[ExchangeError, T] = RestTransport.parse[T](body)

  protected def queryString(params: Map[String, String]): String =
    if params.isEmpty then "" else params.map((k, v) => s"$k=$v").mkString("?", "&", "")

  protected def fmt(d: Double): String = RestTransport.fmt(d)

  /** 把 Binance 的失败形态归一到框架语义 —— **写单路径 (下单/撤单/改杠杆) 专用**。
    *
    * Binance 用 HTTP 4xx 表达业务拒单：请求没通过校验，订单确定未进撮合。5xx 与网络错误则
    * 结果不确定，原样保留 (共享层会据此终止)。限频已由 [[RestTransport]] 在 HTTP 层归类。 */
  protected def classifyWrite(e: ExchangeError): ExchangeError = BinanceClient.classifyWriteError(e)

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
      "symbol" -> perpSymbol(order.instrument),
      "side" -> sideParam(order.side),
      "quantity" -> fmt(order.quantity.value),
      "newClientOrderId" -> order.clientOrderId,
    ) ++ (if order.reduceOnly then Map("reduceOnly" -> "true") else Map.empty)
    val params = order.orderType match
      case OrderType.Market => base + ("type" -> "MARKET")
      case OrderType.Limit(price, tif) =>
        base + ("type" -> "LIMIT") + ("price" -> fmt(price.value)) + ("timeInForce" -> tifParam(tif))
    signedRequest[NewOrderResp](Method.POST, "/fapi/v1/order", params)
      .map(_.orderId.toString)
      .left
      .map(classifyWrite)

  /** 本客户端接的是 USDⓈ-M 永续，别的品种没有对应端点 —— **立即失败**。
    *
    * 不静默当永续处理: 那会把一张期权单发到永续端点上, 要么被交易所拒 (白跑一趟),
    * 要么撞上一个同名的永续合约。品种是调用方明确写下的事实, 对不上就是装配错了。 */
  private def perpSymbol(instrument: Instrument): Symbol =
    require(
      instrument.kind == InstrumentKind.LinearPerp,
      s"Binance 适配层只支持 U 本位永续, 收到 ${instrument.kind}: $instrument",
    )
    instrument.symbol

  override def cancelOrder(instrument: Instrument, ref: OrderRef): Either[ExchangeError, Unit] =
    val symbol = perpSymbol(instrument)
    val idParam = ref match
      case OrderRef.ByExchangeId(id) => "orderId" -> id
      case OrderRef.ByClientId(id)   => "origClientOrderId" -> id
    signedRaw(Method.DELETE, "/fapi/v1/order", Map("symbol" -> symbol, idParam)) match
      case Right(_) => Right(())
      // Binance -2011 Unknown order: 已成交/已撤——在交易所边界归一为类型化错误，不外泄魔法码
      case Left(ExchangeError.Http(_, body)) if body.contains("-2011") =>
        Left(ExchangeError.OrderNotFound(s"Binance -2011 unknown order: ${ref.raw}"))
      case Left(e) => Left(classifyWrite(e))

  override def fetchPendingOrders(instrument: Instrument): Either[ExchangeError, Vector[OrderUpdate]] =
    signedRequest[List[OpenOrder]](Method.GET, "/fapi/v1/openOrders", Map("symbol" -> perpSymbol(instrument))).map {
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
            side = BinanceCodec.sideFromBinance(o.side),
            status = if filled.nonZero then OrderStatus.PartiallyFilled(filled) else OrderStatus.Pending,
            price = o.price.asPrice,
            quantity = Coin(o.origQty.asDouble),
            filledQuantity = filled,
            reduceOnly = RestTransport.requireFlag(o.reduceOnly, "Binance", "reduceOnly", s"orderId=${o.orderId}"),
            timestamp = o.time,
          )
        }.toVector
    }

  /** 账户是否处于**单向持仓**模式 (`/fapi/v1/positionSide/dual` 的 `dualSidePosition == false`)。
    *
    * 持仓模式是账户级事实，因此该由装配期校验，而不是等第一条 ACCOUNT_UPDATE 推来才发现 ——
    * "配置错误即终止"要在装配/首次使用时发生，不能跑了半天再崩。 */
  def isOneWayPositionMode(): Either[ExchangeError, Boolean] =
    signedRequest[PositionSideDual](Method.GET, "/fapi/v1/positionSide/dual", Map.empty)
      .map(!_.dualSidePosition)


  override def fetchAccountInfo(): Either[ExchangeError, AccountInfo] =
    accountSnapshot().map { account =>
      AccountInfo(
        account = AccountId.Live,
        exchange,
        equity = account.totalMarginBalance.asDouble,
      )
    }

  /** 完整钱包: `/fapi/v2/account` 的 assets 一次返回整份资产余额。 */
  override def fetchWallet(): Either[ExchangeError, Map[String, Double]] =
    accountSnapshot().map(_.assets.map(a => a.asset -> a.walletBalance.asDouble).toMap)

  /** 净值与资产明细来自同一个响应 —— 分两次拉会拿到两个时刻的账户状态。 */
  private def accountSnapshot(): Either[ExchangeError, AccountResp] =
    signedRequest[AccountResp](Method.GET, "/fapi/v2/account", Map.empty)

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

