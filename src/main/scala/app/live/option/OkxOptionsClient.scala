package app.live.option

import com.github.plokhotnyuk.jsoniter_scala.core.*
import com.github.plokhotnyuk.jsoniter_scala.macros.*
import hft.exchange.okx.{OkxClient, OkxCredentials}
import org.slf4j.LoggerFactory
import sttp.client4.*
import sttp.model.Uri

/** OKX v5 **期权** REST 客户端 (instType=OPTION)——[[OptionsExchange]] 的 OKX 实现, 与 Bybit 实现并列
  * (开放封闭: 新增交易所 = 新增实现, 不改既有)。复用 hft 的 [[OkxClient]] 签名原语 (base64 HMAC + 头鉴权)
  * 与 [[OkxCredentials]] (含 passphrase)。
  *
  * **符号约定** (与框架 OKX 统一 symbol 一致): 标的方法的 `symbol` 传**基础币** (如 "ETH"), 内部拼 instId
  * `ETH-USDT-SWAP` 取永续 K 线; 期权方法的 `symbol` 传**期权 instId** (如 `ETH-USD-240329-1400-P`, 由
  * [[optionChain]] 原样回传)。OKX 期权为币本位, instFamily=`<base>-USD`。
  *
  * **实盘**: sellOption 直接提交真实订单 (无 dry-run)。`simulated=true` 加 `x-simulated-trading:1` 头走模拟盘。
  * **首次上真金白银前务必在模拟盘验证**: instId 格式 / 合约张数单位 (ctVal) / 最小下单量 / 价格精度 / clOrdId 规则。
  *
  * @param quote     永续计价币 (拼标的 instId, 默认 USDT)
  * @param optionCcy 期权净 greeks 只统计该币种 (None=全部相加)。account/greeks 按币种分行, 单标的部署
  *                  应传 Some(基础币) 避免多币种 delta 跨币量纲相加; 见 [[optionAccountGreeks]]。
  * @param simulated 模拟盘开关 (OKX 用请求头切换, 非独立域名)
  */
final class OkxOptionsClient(
    backend: SyncBackend,
    credentials: Option[OkxCredentials],
    quote: String = "USDT",
    optionCcy: Option[String] = None,
    simulated: Boolean = false,
) extends OptionsExchange:
  import OkxOptionsClient.*

  private val logger = LoggerFactory.getLogger(classOf[OkxOptionsClient])
  private val base = OkxClient.RestBaseUrl

  /** 基础币 -> 永续 instId (与 hft OkxCodec.toOkx 同口径: BASE-QUOTE-SWAP) */
  private def swapInstId(symbol: String): String = s"$symbol-$quote-SWAP"

  override def underlyingCloses5m(symbol: String, bars: Int): Either[String, Vector[Double]] =
    candles(swapInstId(symbol), bar = "5m", bars).map(_.map(_.close))

  override def underlyingSpot(symbol: String): Either[String, Double] =
    underlyingCloses5m(symbol, 1).flatMap(_.lastOption.toRight(s"no kline for $symbol"))

  override def linearKlines(symbol: String, interval: String, bars: Int): Either[String, Vector[(Double, Double, Double)]] =
    candles(swapInstId(symbol), bar = interval, bars).map(_.map(c => (c.high, c.low, c.close)))

  /** OKX history-candles 分页 (每页 ≤100, 最新在前; `after`=ts 返回更早记录)。归并去重后按时间升序, 取最后 bars 根。 */
  private def candles(instId: String, bar: String, bars: Int): Either[String, Vector[Bar]] =
    def page(after: Option[Long], need: Int, acc: Vector[Bar]): Either[String, Vector[Bar]] =
      val lim = math.min(100, need)
      val q = s"instId=$instId&bar=$bar&limit=$lim" + after.fold("")(a => s"&after=$a")
      publicGet[CandlesEnvelope](s"/api/v5/market/history-candles?$q").flatMap { env =>
        env.asEither.flatMap { rows =>
          val bs = rows.flatMap(Bar.parse)
          val merged = acc ++ bs
          // 终止按**去重后**数量判定 (OKX after 为开区间通常无重叠, 但防御重复行导致 need 偏小提前停)
          if bs.isEmpty || merged.distinctBy(_.ts).sizeIs >= bars then Right(merged)
          else page(Some(bs.map(_.ts).min), bars - bs.size, merged)
        }
      }
    page(None, bars, Vector.empty).map(_.distinctBy(_.ts).sortBy(_.ts).takeRight(bars))

  override def optionChain(baseCoin: String): Either[String, Vector[OptionInstrument]] =
    // OKX 期权链一次返回全量 (无游标), 直接读 stk/optType/expTime 字段, 无需解析符号
    publicGet[Envelope[InstrumentItem]](s"/api/v5/public/instruments?instType=OPTION&instFamily=$baseCoin-USD").flatMap { env =>
      env.asEither.map(_.flatMap(instrumentOf).toVector)
    }

  override def optionBestAsk(symbol: String): Either[String, Option[Double]] =
    publicGet[Envelope[TickerItem]](s"/api/v5/market/ticker?instId=$symbol").map { env =>
      env.result.flatMap(_.askPx.toDoubleOption).filter(_ > 0)
    }

  override def optionAccountGreeks(): Either[String, (Double, Double)] =
    // OKX 账户级、按币种(分行)聚合的 BS 希腊字母 (deltaBS/gammaBS, 与框架 hft.OkxClient.fetchGreeks 同字段同口径——
    // 该路径已是 OKX 永续对冲使用的 delta 定义, 故复用一致)。optionCcy=Some 时只取该币行 (避免多币 delta 跨币相加)。
    signedGet[Envelope[GreeksItem]]("/api/v5/account/greeks", "").flatMap { env =>
      env.asEither.map { rows =>
        val rel = optionCcy.fold(rows)(c => rows.filter(_.ccy == c))
        (rel.flatMap(_.deltaBS.toDoubleOption).sum, rel.flatMap(_.gammaBS.toDoubleOption).sum)
      }
    }

  override def sellOption(symbol: String, qty: Double, limitPrice: Option[Double], orderLinkId: String): Either[String, String] =
    val clOrdId = clOrdIdOf(orderLinkId)
    val body = sellOrderBody(symbol, qty, limitPrice, clOrdId)
    credentials match // 实盘下单, 无 dry-run
      case None => Left("缺少 OKX_API_KEY/SECRET/PASSPHRASE, 无法下单")
      case Some(_) =>
        logger.warn(s"[实盘] 提交: SELL $symbol sz=$qty ${limitPrice.fold("MKT")(p => s"@$p PostOnly")} clOrdId=$clOrdId")
        signedPost[Envelope[OrderItem]]("/api/v5/trade/order", body).flatMap { env =>
          env.asEither.flatMap { items =>
            items.headOption.toRight("OKX 下单无返回数据").flatMap { d =>
              if d.sCode == "0" then Right(d.ordId)
              else Left(s"OKX 下单被拒 sCode=${d.sCode} sMsg=${d.sMsg}")
            }
          }
        }

  // ---- HTTP (OKX: 顶层 code/msg/data, 签名头 OK-ACCESS-*, prehash=ts+method+path+body) ----
  /** 模拟盘请求头 (OKX 用 x-simulated-trading:1 切换模拟盘, 非独立域名) */
  private val simHeaders: Map[String, String] = if simulated then Map("x-simulated-trading" -> "1") else Map.empty

  private def publicGet[T: JsonValueCodec](pathQuery: String): Either[String, T] =
    try
      val req0 = basicRequest.get(Uri.unsafeParse(s"$base$pathQuery")).response(asStringAlways)
      val resp = simHeaders.foldLeft(req0)((r, kv) => r.header(kv._1, kv._2)).send(backend)
      if resp.code.isSuccess then Right(readFromString[T](resp.body))
      else Left(s"HTTP ${resp.code} GET $pathQuery: ${resp.body.take(300)}")
    catch case e: Throwable => Left(s"GET $pathQuery failed: ${e.getMessage}")

  private def signedGet[T: JsonValueCodec](path: String, query: String): Either[String, T] =
    val pathQuery = if query.isEmpty then path else s"$path?$query"
    signed[T]("GET", pathQuery, body = "")

  private def signedPost[T: JsonValueCodec](path: String, body: String): Either[String, T] =
    signed[T]("POST", path, body)

  private def signed[T: JsonValueCodec](method: String, pathQuery: String, body: String): Either[String, T] =
    credentials match
      case None => Left("缺少 OKX 凭证")
      case Some(c) =>
        try
          // 签名头复用框架 OkxClient.signedHeaders (与永续客户端同一 SSOT), 叠加模拟盘头
          val headers = simHeaders ++ OkxClient.signedHeaders(c, method, pathQuery, body)
          val baseReq = basicRequest
            .method(sttp.model.Method(method), Uri.unsafeParse(s"$base$pathQuery"))
            .response(asStringAlways)
          val withHeaders = headers.foldLeft(baseReq)((r, kv) => r.header(kv._1, kv._2))
          val req = if body.isEmpty then withHeaders else withHeaders.body(body)
          val resp = req.send(backend)
          if resp.code.isSuccess then Right(readFromString[T](resp.body))
          else Left(s"HTTP ${resp.code} $method $pathQuery: ${resp.body.take(300)}")
        catch case e: Throwable => Left(s"$method $pathQuery failed: ${e.getMessage}")

object OkxOptionsClient:
  /** OKX 卖出期权下单 body (纯函数, 便于断言)。limitPrice=Some -> post_only(maker)+px; None -> market。
    * tdMode=cross (期权跨保证金), side=sell。数字格式复用框架 [[OkxClient.fmt]] (出站格式 SSOT)。 */
  def sellOrderBody(instId: String, qty: Double, limitPrice: Option[Double], clOrdId: String): String =
    val (ordType, pxField) = limitPrice match
      case Some(p) => ("post_only", s""","px":"${OkxClient.fmt(p)}"""")
      case None    => ("market", "")
    s"""{"instId":"$instId","tdMode":"cross","side":"sell","ordType":"$ordType","sz":"${OkxClient.fmt(qty)}"$pxField,"clOrdId":"$clOrdId"}"""

  private def rightOf(optType: String): Option[OptionRight] = optType.toUpperCase match
    case "C" => Some(OptionRight.Call)
    case "P" => Some(OptionRight.Put)
    case _   => None

  /** OKX clOrdId 仅允许字母数字 (≤32 字符); 去掉 vs-<ts>-<tag> 的连字符, 确定性映射 -> 幂等 (重启/重试不重复下单) */
  def clOrdIdOf(orderLinkId: String): String = orderLinkId.filter(_.isLetterOrDigit).take(32)

  /** OKX 期权 instruments 行 -> [[OptionInstrument]] (直接读 stk/optType/expTime 字段, 不解析符号)。
    * strike/optType/expTime 任一缺失或非法 -> None (跳过该合约, 不污染期权链)。 */
  def instrumentOf(i: InstrumentItem): Option[OptionInstrument] =
    for
      strike <- i.stk.toDoubleOption
      right <- rightOf(i.optType)
      exp <- i.expTime.toLongOption.filter(_ > 0)
    yield OptionInstrument(i.instId, exp, strike, right,
      minQty = i.minSz.toDoubleOption.getOrElse(0.0),
      qtyStep = i.lotSz.toDoubleOption.getOrElse(0.0),
      tickSize = i.tickSz.toDoubleOption.getOrElse(0.0))

  /** 一根 K 线 (ts ms + OHLC), 由 OKX candles 行 [ts,o,h,l,c,...] 解析 */
  final case class Bar(ts: Long, high: Double, low: Double, close: Double)
  object Bar:
    def parse(row: List[String]): Option[Bar] =
      for
        ts <- row.lift(0).flatMap(_.toLongOption)
        h <- row.lift(2).flatMap(_.toDoubleOption)
        l <- row.lift(3).flatMap(_.toDoubleOption)
        c <- row.lift(4).flatMap(_.toDoubleOption)
      yield Bar(ts, h, l, c)

  /** OKX v5 统一响应外壳 (data 为数组)。code="0" 成功。 */
  final case class Envelope[T](code: String, msg: String, data: List[T]):
    def asEither: Either[String, List[T]] =
      if code == "0" then Right(data) else Left(s"OKX code=$code: $msg")
    def result: Option[T] = if code == "0" then data.headOption else None

  /** candles 专用外壳 (data 为字符串数组的数组) */
  final case class CandlesEnvelope(code: String, msg: String, data: List[List[String]]):
    def asEither: Either[String, List[List[String]]] =
      if code == "0" then Right(data) else Left(s"OKX code=$code: $msg")

  final case class InstrumentItem(instId: String, stk: String, optType: String, expTime: String, lotSz: String, minSz: String, tickSz: String)
  final case class TickerItem(askPx: String)
  final case class OrderItem(ordId: String, clOrdId: String, sCode: String, sMsg: String)
  final case class GreeksItem(ccy: String, deltaBS: String, gammaBS: String)

  given candlesCodec: JsonValueCodec[CandlesEnvelope] = JsonCodecMaker.make
  given instrumentsCodec: JsonValueCodec[Envelope[InstrumentItem]] = JsonCodecMaker.make
  given tickerCodec: JsonValueCodec[Envelope[TickerItem]] = JsonCodecMaker.make
  given orderCodec: JsonValueCodec[Envelope[OrderItem]] = JsonCodecMaker.make
  given greeksCodec: JsonValueCodec[Envelope[GreeksItem]] = JsonCodecMaker.make
