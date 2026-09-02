package hft.exchange.okx

import com.github.plokhotnyuk.jsoniter_scala.core.*
import hft.domain.*
import hft.exchange.{ExchangeClient, RestTransport, TradingClient}
import sttp.client4.*
import sttp.model.{Method, Uri}

import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.time.ZoneOffset
import java.util.Base64
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import scala.concurrent.duration.*

import OkxCodec.*
import OkxCodec.given

/** OKX 凭证。OKX 比 Binance 多一个 passphrase (REST 头与 WS 登录都需要)。
  *
  * 计价币 quote 不在此——它是行情/合约配置而非鉴权信息，单独作为客户端参数 (默认 USDT)。
  */
final case class OkxCredentials(apiKey: String, secret: String, passphrase: String):
  /** WebSocket 登录签名: base64(HMAC-SHA256(secret, timestamp + "GET/users/self/verify")) */
  def signWsLogin(timestamp: String): String =
    OkxClient.hmacSha256Base64(secret, s"${timestamp}GET/users/self/verify")

object OkxClient:
  /** 读 `/api/v5/account/greeks` 的一个希腊字母字段。**空串是协议规定的合法值, 不是坏报文。**
    *
    * OKX 通用约定: `"" will be returned for inapplicable fields under the current account level`,
    * 而 `gammaBS/thetaBS/vegaBS` 标注 `only applicable to OPTION` —— 该币种没有期权持仓时
    * 这几个字段就是空串 (Get Greeks 的官方响应示例本身即 `{"deltaBS":"","gammaBS":"",...,"ccy":"BTC"}`)。
    * 不带 `ccy` 请求时接口返回账户里所有有余额的币种, 因此一个 USDT 行、或者卖出第一张期权
    * 之前的 ETH 行, 就足以触发。
    *
    * 把它当坏报文抛的后果: [[OkxAccountFeed.pollGreeks]] 只把 `Left` 当可重试, 异常会穿出
    * 受管任务并**级联终止整个引擎** —— 而 OKX 路径上它是唯一的 greeks 来源。
    *
    * 只有**非空且非数字**才是真的坏报文, 那时必须抛: 静默归零会让账户 delta 少算一块,
    * 而对冲正是按它下单。
    *
    * 这条规则公开在这里 (而不是留在 `private[okx]` 的 codec 里) 是因为它有**两个消费者**:
    * [[OkxClient.fetchGreeks]] 与 `strategy.utils.option.OkxOptionsClient.optionAccountGreeks`,
    * 而后者在别的包。各写一份的结果是同一段文档在同一个仓库里有两个相反的实现 ——
    * 那正是本条规则被抽出来的原因。报文类型本身仍留在适配层内, 不跨包泄漏。 */
  def greekField(raw: String, field: String, ccy: String): Double =
    if raw.isEmpty then 0.0
    else
      raw.toDoubleOption.getOrElse(
        throw IllegalStateException(s"OKX greeks 的 $field 不是数字: ccy=$ccy 原始值='$raw'")
      )

  /** 只读客户端（无凭证）：只能取公共数据，私有端点在**类型上**够不着。
    *
    * 返回具体类型而非 `ExchangeClient`：行情插件要读 `quote` 才能拼出 instId
    * (见 [[hft.exchange.okx.OkxMarketFeed]])，那是公共客户端的事实，不该为了拿到它
    * 而要求一份根本用不上的凭证。 */
  def public(backend: SyncBackend, quote: String = "USDT"): OkxPublicClient =
    new OkxPublicClient(backend, quote, OkxClient.RestBaseUrl)

  /** 交易客户端（带凭证）：拿到它即意味着凭证已具备，无需再问 `hasCredentials`。 */
  def trading(backend: SyncBackend, credentials: OkxCredentials, quote: String = "USDT"): OkxClient =
    new OkxClient(backend, credentials, quote, OkxClient.RestBaseUrl)
  val RestBaseUrl = "https://www.okx.com"
  val WsPublicUrl = "wss://ws.okx.com:8443/ws/v5/public"
  val WsPrivateUrl = "wss://ws.okx.com:8443/ws/v5/private"

  /** OKX REST 签名: base64(HMAC-SHA256(secret, prehash))，prehash = ts+method+path+body */
  def hmacSha256Base64(secret: String, data: String): String =
    val mac = Mac.getInstance("HmacSHA256")
    mac.init(SecretKeySpec(secret.getBytes(UTF_8), "HmacSHA256"))
    Base64.getEncoder.encodeToString(mac.doFinal(data.getBytes(UTF_8)))

  private val IsoMillisUtc =
    DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC)

  /** OKX 签名请求头的**单一数据源** (prehash=ts+method+pathQuery+body, base64-HMAC)。框架永续客户端
    * 与 [[strategy.utils.option.OkxOptionsClient]] 期权客户端共用——签名规则只此一处, 杜绝改一处忘改另一处。
    * `pathQuery` 须含 query string (OKX 要求 requestPath 参与签名), GET 的 `body` 传空串。 */
  def signedHeaders(c: OkxCredentials, method: String, pathQuery: String, body: String): Map[String, String] =
    val ts = IsoMillisUtc.format(Instant.now())
    Map(
      "OK-ACCESS-KEY" -> c.apiKey,
      "OK-ACCESS-SIGN" -> hmacSha256Base64(c.secret, ts + method + pathQuery + body),
      "OK-ACCESS-TIMESTAMP" -> ts,
      "OK-ACCESS-PASSPHRASE" -> c.passphrase,
      "Content-Type" -> "application/json",
    )

  /** 下单数量/价格格式化。出站数字格式的单一数据源现在是
    * [[hft.exchange.RestTransport.fmt]] (三家交易所同一份)，这里只做转发，供期权客户端复用。 */
  def fmt(d: Double): String = RestTransport.fmt(d)

  /** 撤单时表示"订单不存在/已撤/已完成"的 OKX sCode，归一为 ExchangeError.OrderNotFound */
  private val OrderNotFoundCodes: Set[String] = Set("51400", "51401", "51402")

  /** OKX **顶层** code 的限频码。依据 v5 文档: 50011 = "Rate limit reached. Please refer to
    * API documentation and throttle requests accordingly."
    *
    * 只出现在请求级 (`code`)，不出现在逐单结果 (`sCode`) 里 —— 逐单 sCode 说的是这一单被拒。 */
  private[okx] val RateLimitCode: String = "50011"

/** OKX 永续合约 REST 客户端。所有请求经 sttp 同步 backend 阻塞执行 (运行在虚拟线程上)。
  *
  * 与 Binance 的差异：签名走请求头 (OK-ACCESS-*) 而非 query；数量单位为合约张数 (sz)，
  * 与统一币本位的转换由 StrategyRunner (出站) / Engine (入站) / 账户流 (WS) 用 SymbolMeta 完成。
  */
/** OKX 永续的**公共** REST 客户端 —— 无凭证即可用。私有端点在 [[OkxClient]]。 */
class OkxPublicClient protected[okx] (
    protected val backend: SyncBackend,
    val quote: String,
    protected val restBase: String,
) extends ExchangeClient:


  override def exchange: Exchange = Exchange.Okx


  // ==================== ExchangeClient ====================

  override def fetchAllSymbolMetas(): Either[ExchangeError, Vector[SymbolMeta]] =
    // 合约清单响应大, 与下单路径的时限无关
    publicGet[InstrumentsResp]("/api/v5/public/instruments?instType=SWAP", RestTransport.QueryTimeout).flatMap { resp =>
      ensureOk(resp.code, resp.msg).map { _ =>
        resp.data.iterator
          // 只取配置 quote 的**已上市**永续 —— quote 判定收在 fromOkx 里 (见它的说明);
          // "尚未上市"的判据收在 InstrumentData 里 (见它的说明)
          .filterNot(_.notYetListed)
          .flatMap { d =>
            fromOkx(d.instId, quote).map { sym =>
              SymbolMeta(
                exchange = Exchange.Okx,
                symbol = sym,
                tickSize = d.tickSz.asDouble,
                sizeStep = d.lotSz.asDouble,
                minOrderSize = d.minSz.asDouble,
                contractSize = d.ctVal.asDouble, // 每张合约对应的币本位数量
              )
            }
          }
          .filter(_.isValid)
          .toVector
      }
    }


  /** 把交易所回报里的**张数**换回框架统一的币本位。
    * 规格取自 [[ExchangeClient.symbolMetas]] —— 与行情源、汇报面、柜台读的是同一份。 */
  protected def metaOf(symbol: Symbol): SymbolMeta =
    symbolMetas.getOrElse(symbol, sys.error(s"SymbolMeta not found: $exchange $symbol"))


  // ==================== 请求基础设施 ====================

  protected def publicGet[T: JsonValueCodec](
      path: String,
      timeout: scala.concurrent.duration.FiniteDuration = RestTransport.ReadTimeout,
  ): Either[ExchangeError, T] =
    send(Method.GET, s"$restBase$path", Map.empty, None, timeout).flatMap(parse[T])


  protected def send(
      method: Method,
      url: String,
      headers: Map[String, String],
      body: Option[String],
      timeout: scala.concurrent.duration.FiniteDuration = RestTransport.ReadTimeout,
  ): Either[ExchangeError, String] =
    RestTransport.send(backend, method, url, headers, body, timeout)

  protected def parse[T: JsonValueCodec](body: String): Either[ExchangeError, T] = RestTransport.parse[T](body)

  /** OKX **顶层** code 校验: "0" 为成功。
    *
    * 顶层 code 说的是"这次请求"的结果，不是"这一单"的结果 —— 后者在 `data[i].sCode`
    * (见 [[placeOrder]])。限频 50011 出现在顶层，因此在这里归类。
    * 依据 OKX v5 文档: 50011 = "Rate limit reached. Please refer to API documentation and
    * throttle requests accordingly."
    */
  protected def ensureOk(code: String, msg: String): Either[ExchangeError, Unit] =
    if code == "0" then Right(())
    else if code == OkxClient.RateLimitCode then
      Left(ExchangeError.RateLimited(s"OKX rate limit: code=$code msg=$msg"))
    else Left(ExchangeError.Other(s"OKX API error: code=$code msg=$msg"))


  protected def fmt(d: Double): String = RestTransport.fmt(d)


  protected def sideParam(side: Side): String = side match
    case Side.Long  => "buy"
    case Side.Short => "sell"


  protected def sideFromOkx(side: String): Side = side match
    case "buy"  => Side.Long
    case "sell" => Side.Short
    case other  => throw IllegalStateException(s"Unknown OKX side: '$other'")


  protected def tifToOrdType(tif: TimeInForce): String = tif match
    case TimeInForce.GTC      => "limit"
    case TimeInForce.IOC      => "ioc"
    case TimeInForce.FOK      => "fok"
    case TimeInForce.PostOnly => "post_only"

/** OKX 永续的**交易** REST 客户端。凭证是构造参数而非 `Option`，签名路径因此没有
  * "万一没配密钥"的分支，`ExchangeError.Auth` 回归它本来的含义：交易所真的拒绝了鉴权。 */
final class OkxClient private[okx] (
    backend: SyncBackend,
    val credentials: OkxCredentials,
    quote: String,
    restBase: String,
) extends OkxPublicClient(backend, quote, restBase),
      TradingClient:


  /** 供账户流构建 WS 登录帧使用 */
  def wsCredentials: OkxCredentials = credentials

  // ==================== ExchangeClient ====================


  override def placeOrder(order: ExchangeOrder): Either[ExchangeError, OrderId] =
    // order.quantity 已由 StrategyRunner 转为合约张数并取整
    val instId = toOkx(order.symbol, quote)
    val (ordType, pxField) = order.orderType match
      case OrderType.Market            => ("market", "")
      case OrderType.Limit(price, tif) => (tifToOrdType(tif), s""","px":"${fmt(price.value)}"""")
    val reduceField = if order.reduceOnly then ""","reduceOnly":true""" else ""
    val clOrdField = if order.clientOrderId.nonEmpty then s""","clOrdId":"${order.clientOrderId}"""" else ""
    val body =
      s"""{"instId":"$instId","tdMode":"cross","side":"${sideParam(order.side)}","ordType":"$ordType","sz":"${fmt(order.quantity.value)}"$pxField$reduceField$clOrdField}"""
    signedRequest[PlaceOrderResp](Method.POST, "/api/v5/trade/order", body).flatMap { resp =>
      resp.data.headOption match
        // 逐单结果: sCode 非 0 表示**这一单**没进撮合 —— 交易所明确拒绝, 订单确定未成立。
        // 这是 OKX 表达业务拒单的形状 (HTTP 200 + data[0].sCode)，共享层只认语义、不认数字。
        case Some(d) if d.sCode != "0" => Left(ExchangeError.Rejected(d.sCode, d.sMsg))
        case Some(d)                   => Right(d.ordId)
        // 连逐单结果都没有: 请求级失败 (含限频)，订单是否成立不确定，交给 ensureOk 分流
        case None => ensureOk(resp.code, resp.msg).flatMap(_ => Left(ExchangeError.Other("OKX no order data in response")))
    }


  override def cancelOrder(symbol: Symbol, ref: OrderRef): Either[ExchangeError, Unit] =
    val idField = ref match
      case OrderRef.ByExchangeId(id) => s""""ordId":"$id""""
      case OrderRef.ByClientId(id)   => s""""clOrdId":"$id""""
    val body = s"""{"instId":"${toOkx(symbol, quote)}",$idField}"""
    signedRequest[CancelResp](Method.POST, "/api/v5/trade/cancel-order", body).flatMap { resp =>
      resp.data.headOption match
        case Some(d) if d.sCode != "0" =>
          // OKX 51400/51401/51402: 订单不存在/已撤/已完成——归一为类型化错误，不外泄魔法码
          if OkxClient.OrderNotFoundCodes.contains(d.sCode) then
            Left(ExchangeError.OrderNotFound(s"OKX ${d.sCode}: ${ref.raw}"))
          // 逐单 sCode 非 0 = 交易所明确拒绝了这次撤单 (不是"结果不确定")
          else Left(ExchangeError.Rejected(d.sCode, s"OKX cancel failed: ${d.sMsg}"))
        case Some(_) => Right(())
        case None    => ensureOk(resp.code, resp.msg)
    }


  override def fetchPendingOrders(symbol: Symbol): Either[ExchangeError, Vector[OrderUpdate]] =
    val path = s"/api/v5/trade/orders-pending?instId=${toOkx(symbol, quote)}&instType=SWAP"
    signedRequest[PendingResp](Method.GET, path).flatMap { resp =>
      ensureOk(resp.code, resp.msg).map { _ =>
        resp.data.iterator.flatMap { d =>
          fromOkx(d.instId, quote).map { sym =>
            val filled = metaOf(sym).toCoin(Contracts(d.accFillSz.asDouble))
            OrderUpdate(
              account = AccountId.Live,
              orderId = d.ordId,
              // 没有 clOrdId 的单 (手工下的) 就是没有 —— 拿 ordId 冒充会在策略的 pending
              // 索引里凭空造出一个不存在的键, 而 WS 路径给的是 None (同一事实两个答案)。
              clientOrderId = Option.when(d.clOrdId.nonEmpty)(d.clOrdId),
              exchange = Exchange.Okx,
              symbol = sym,
              side = sideFromOkx(d.side),
              status = mapOrderState(d.state, filled),
              price = Price(d.px.asDoubleOrZero),
              quantity = metaOf(sym).toCoin(Contracts(d.sz.asDouble)),
              filledQuantity = filled,
              reduceOnly = OkxCodec.booleanFrom(d.reduceOnly, "reduceOnly"),
              // 交易所侧的更新时刻。用本地钟会让延迟基准恒为零 (柜台把它当 exchangeTs 用)。
              timestamp = d.uTime.toLongOption.getOrElse(
                throw IllegalStateException(s"OKX orders-pending 缺 uTime: ordId=${d.ordId} 原始值='${d.uTime}'")
              ),
            )
          }
        }.toVector
      }
    }



  /** OKX 净值走 REST (totalEq)；名义价值由私有 WS account 频道推送，此处置 0 */
  override def fetchAccountInfo(): Either[ExchangeError, AccountInfo] =
    balance().map(b => AccountInfo(AccountId.Live, exchange, equity = b.totalEq.asDouble))

  /** 完整钱包: `/api/v5/account/balance` 的 details 就是整份币种明细 (余额为 0 的币种 OKX
    * 不下发该行, 因此"未列出 = 0"正是它的语义)。 */
  override def fetchWallet(): Either[ExchangeError, Map[String, Double]] =
    balance().map(_.details.map(d => d.ccy -> d.cashBal.asDouble).toMap)

  /** 净值与币种明细来自同一个响应 —— 分两次拉会拿到两个时刻的账户状态。 */
  private def balance(): Either[ExchangeError, OkxCodec.BalanceRespData] =
    signedRequest[BalanceResp](Method.GET, "/api/v5/account/balance").flatMap { r =>
      ensureOk(r.code, r.msg).flatMap { _ =>
        r.data.headOption.toRight(ExchangeError.Other("OKX no balance data"))
      }
    }

  /** GET /api/v5/account/positions?instType=SWAP —— **REST 直查**。
    *
    * 从前这里返回空, 理由是"初始持仓由私有 WS 登录后下发 snapshot"。**那条理由在仓位归柜台
    * 算之后已经不成立**, 有两道各自独立的原因:
    *
    *   1. WS 推来的仓位走 [[AccountReport.PositionReported]], 而柜台只把它当作**交易所第三方
    *      读数**记进对账 (见 [[hft.exchange.RestTradingGateway]]) —— 不进账本、不发总线。
    *   2. 就算它进账本也赶不上: 柜台的 `connect()` 在 onStart 里跑, 必然早于对齐指令,
    *      那条 snapshot 到达时柜台还不知道自己管哪些标的, 在入口就被分流掉了。
    *
    * 于是账户带着仓位重启 -> 账本从零开始 -> 对齐给策略推一串零仓 -> 策略按空仓决策,
    * 而三方对账只会报一句"账本与交易所对不上", 措辞还会把人引向"对齐竞态"。
    *
    * **对齐要用 REST**: 它查的是快照, 不参与推送的流竞争。这与对账用推送 (便宜、及时) 是
    * 两件事 —— 检测用推送, 修复用 REST。
    *
    * `instType=SWAP` 返回的是**全部计价币种**的永续 (USDT / USDC / 币本位)。非本 quote 的
    * 在 [[fromOkx]] 就被挡掉了 —— 挡不住的话, `ETH-USD-SWAP` 会和 `ETH-USDT-SWAP` 收敛成
    * 同一个 `"ETH"`, 拿 USDT 的 ctVal 去换币本位的张数, 还会在下面 `toMap` 时静默覆盖真的
    * 那一行。剩下的若仍缺合约规格 (新上市还没进 metas) 也跳过: 张->币换不了, 而柜台只会
    * 问它对齐的那几个标的。
    */
  override def fetchPositions(): Either[ExchangeError, Vector[Position]] =
    signedRequest[PositionsResp](Method.GET, "/api/v5/account/positions?instType=SWAP").flatMap { resp =>
      ensureOk(resp.code, resp.msg).map { _ =>
        resp.data.iterator.flatMap { d =>
          for
            sym <- fromOkx(d.instId, quote)
            meta <- symbolMetas.get(sym)
          yield Position(
            account = AccountId.Live,
            exchange = Exchange.Okx,
            symbol = sym,
            size = meta.toCoin(Contracts(d.pos.asDouble)), // 张 -> 币; OKX 的 pos 正多负空
          )
        }.toVector
      }
    }

  // ==================== 账户级希腊字母 (供 Greeks 轮询使用) ====================

  /** GET /api/v5/account/greeks —— 账户级、按币种聚合的希腊字母 */


  // ==================== 账户级希腊字母 (供 Greeks 轮询使用) ====================

  /** GET /api/v5/account/greeks —— 账户级、按币种聚合的希腊字母 */
  def fetchGreeks(): Either[ExchangeError, Vector[Greeks]] =
    signedRequest[GreeksResp](Method.GET, "/api/v5/account/greeks").flatMap { resp =>
      ensureOk(resp.code, resp.msg).map { _ =>
        resp.data.iterator.map { d =>
          Greeks(
            account = AccountId.Live,
            exchange = Exchange.Okx,
            ccy = d.ccy,
            // 空串 = 该币种没有期权持仓, 是合法值; 判据见 GreeksData.greek
            delta = OkxClient.greekField(d.deltaBS, "deltaBS", d.ccy),
            gamma = OkxClient.greekField(d.gammaBS, "gammaBS", d.ccy),
            theta = OkxClient.greekField(d.thetaBS, "thetaBS", d.ccy),
            vega = OkxClient.greekField(d.vegaBS, "vegaBS", d.ccy),
            timestamp = d.ts.toLongOption.getOrElse(throw IllegalStateException(s"Invalid OKX greeks ts: '${d.ts}'")),
          )
        }.toVector
      }
    }

  // ==================== 请求基础设施 ====================


  private def signedRequest[T: JsonValueCodec](
      method: Method,
      path: String,
      body: String = "",
  ): Either[ExchangeError, T] =
    signedSend(method, path, if method == Method.GET then None else Some(body)).flatMap(parse[T])

  /** 统一请求。signed=true 时附 OKX 签名头 (prehash = ts+method+path+body)，缺凭证返回 Auth 错误 */


  /** 签名请求 (prehash = ts+method+path+body)。凭证是构造参数，没有"缺凭证"这一路。 */
  private def signedSend(method: Method, path: String, body: Option[String]): Either[ExchangeError, String] =
    val headers = OkxClient.signedHeaders(credentials, method.method, path, body.getOrElse(""))
    send(method, s"$restBase$path", headers, body)
