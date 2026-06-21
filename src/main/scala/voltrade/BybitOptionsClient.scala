package voltrade

import com.github.plokhotnyuk.jsoniter_scala.core.*
import com.github.plokhotnyuk.jsoniter_scala.macros.*
import hft.exchange.bybit.{BybitClient, BybitCredentials}
import org.slf4j.LoggerFactory
import sttp.client4.*
import sttp.model.Uri

import java.time.Instant

/** Bybit v5 **期权** REST 客户端 (category=option)——独立于 hft 的 BybitClient(linear), 只复用其 HMAC 签名原语。
  *
  * 已实现 (按官方 v5 文档): 标的 5min K 线 (category=linear, 公共)、期权链 instruments-info (category=option, 公共)、
  * 期权 tickers 最优买价 (公共)、下单 order/create (category=option, 签名)。
  *
  * **安全**: dryRun=true 时下单只打日志不真实提交; testnet=true 走 api-testnet。**实盘前务必在 testnet 验证**
  * (符号格式/合约张数单位/最小下单量/价格精度), 本客户端未做精度对齐与撤单/持仓查询 (按需补)。
  */
final class BybitOptionsClient(
    backend: SyncBackend,
    credentials: Option[BybitCredentials],
    dryRun: Boolean = true,
    testnet: Boolean = false,
) extends OptionsExchange:
  import BybitOptionsClient.*

  private val logger = LoggerFactory.getLogger(classOf[BybitOptionsClient])
  private val base = if testnet then "https://api-testnet.bybit.com" else BybitClient.RestBaseUrl

  override def underlyingCloses5m(symbol: String, bars: Int): Either[String, Vector[Double]] =
    // Bybit kline 上限 1000/页且按 startTime 倒序; 多页时用 end 游标向更早翻
    def page(endMs: Option[Long], need: Int, acc: Vector[(Long, Double)]): Either[String, Vector[(Long, Double)]] =
      val lim = math.min(1000, need)
      val q = s"category=linear&symbol=$symbol&interval=5&limit=$lim" + endMs.fold("")(e => s"&end=$e")
      publicGet[Envelope[KlineResult]](s"/v5/market/kline?$q").flatMap { env =>
        env.asEither.flatMap { r =>
          val rows = r.list.flatMap(row => row.lift(0).flatMap(_.toLongOption).zip(row.lift(4).flatMap(_.toDoubleOption)))
          val merged = acc ++ rows
          if rows.isEmpty || merged.sizeIs >= bars then Right(merged)
          else page(Some(rows.map(_._1).min - 1), bars - rows.size, merged)
        }
      }
    page(None, bars, Vector.empty).map(_.distinctBy(_._1).sortBy(_._1).map(_._2).takeRight(bars))

  override def underlyingSpot(symbol: String): Either[String, Double] =
    underlyingCloses5m(symbol, 1).flatMap(_.lastOption.toRight(s"no kline for $symbol"))

  override def optionChain(baseCoin: String): Either[String, Vector[OptionInstrument]] =
    def page(cursor: Option[String], acc: Vector[OptionInstrument]): Either[String, Vector[OptionInstrument]] =
      val q = s"category=option&baseCoin=$baseCoin&limit=1000" + cursor.fold("")(c => s"&cursor=$c")
      publicGet[Envelope[InstrumentsResult]](s"/v5/market/instruments-info?$q").flatMap { env =>
        env.asEither.flatMap { r =>
          val insts = r.list.flatMap { i =>
            OptionContract.parseSymbol(i.symbol).flatMap { case (_, strike, right) =>
              i.deliveryTime.toLongOption.filter(_ > 0).map(exp => OptionInstrument(i.symbol, exp, strike, right))
            }
          }
          val merged = acc ++ insts
          r.nextPageCursor.filter(_.nonEmpty) match
            case Some(c) => page(Some(c), merged)
            case None    => Right(merged)
        }
      }
    page(None, Vector.empty)

  override def optionBestBid(symbol: String): Either[String, Option[Double]] =
    publicGet[Envelope[TickersResult]](s"/v5/market/tickers?category=option&symbol=$symbol").map { env =>
      env.result.flatMap(_.list.headOption).flatMap(_.bid1Price.toDoubleOption).filter(_ > 0)
    }

  override def sellOption(symbol: String, qty: Double, limitPrice: Option[Double], orderLinkId: String): Either[String, String] =
    val (ordType, pxField, tif) = limitPrice match
      case Some(p) => ("Limit", s""","price":"${fmt(p)}"""", "PostOnly")
      case None    => ("Market", "", "IOC")
    val body =
      s"""{"category":"option","symbol":"$symbol","side":"Sell","orderType":"$ordType","qty":"${fmt(qty)}"$pxField,"timeInForce":"$tif","orderLinkId":"$orderLinkId"}"""
    if dryRun then
      logger.warn(s"[DRY-RUN] 不提交真实订单: SELL $symbol qty=$qty ${limitPrice.fold("MKT")(p => s"@$p PostOnly")} link=$orderLinkId  body=$body")
      Right(s"dryrun-$orderLinkId")
    else
      credentials match
        case None => Left("缺少 BYBIT_API_KEY/SECRET, 无法实盘下单")
        case Some(c) =>
          signedPost[Envelope[OrderResult]](c, "/v5/order/create", body).flatMap { env =>
            env.asEither.map(_.orderId)
          }

  // ---- HTTP ----
  private def publicGet[T: JsonValueCodec](pathQuery: String): Either[String, T] =
    try
      val resp = basicRequest.get(Uri.unsafeParse(s"$base$pathQuery")).response(asStringAlways).send(backend)
      if resp.code.isSuccess then Right(readFromString[T](resp.body))
      else Left(s"HTTP ${resp.code} GET $pathQuery: ${resp.body.take(300)}")
    catch case e: Throwable => Left(s"GET $pathQuery failed: ${e.getMessage}")

  private def signedPost[T: JsonValueCodec](c: BybitCredentials, path: String, body: String): Either[String, T] =
    try
      val ts = Instant.now().toEpochMilli.toString
      val prehash = ts + c.apiKey + BybitClient.RecvWindow + body
      val resp = basicRequest
        .post(Uri.unsafeParse(s"$base$path"))
        .header("X-BAPI-API-KEY", c.apiKey)
        .header("X-BAPI-SIGN", BybitClient.hmacSha256Hex(c.apiSecret, prehash))
        .header("X-BAPI-TIMESTAMP", ts)
        .header("X-BAPI-RECV-WINDOW", BybitClient.RecvWindow)
        .header("Content-Type", "application/json")
        .body(body)
        .response(asStringAlways)
        .send(backend)
      if resp.code.isSuccess then Right(readFromString[T](resp.body))
      else Left(s"HTTP ${resp.code} POST $path: ${resp.body.take(300)}")
    catch case e: Throwable => Left(s"POST $path failed: ${e.getMessage}")

object BybitOptionsClient:
  private def fmt(d: Double): String = if d == d.toLong.toDouble then d.toLong.toString else d.toString

  /** Bybit v5 统一响应外壳 */
  final case class Envelope[T](retCode: Int, retMsg: String, result: Option[T]):
    def asEither: Either[String, T] =
      if retCode == 0 then result.toRight("retCode=0 但 result 缺失")
      else Left(s"Bybit retCode=$retCode: $retMsg")

  final case class KlineResult(list: List[List[String]])
  final case class InstrumentItem(symbol: String, deliveryTime: String)
  final case class InstrumentsResult(list: List[InstrumentItem], nextPageCursor: Option[String])
  final case class TickerItem(symbol: String, bid1Price: String)
  final case class TickersResult(list: List[TickerItem])
  final case class OrderResult(orderId: String, orderLinkId: String)

  given klineCodec: JsonValueCodec[Envelope[KlineResult]] = JsonCodecMaker.make
  given instrumentsCodec: JsonValueCodec[Envelope[InstrumentsResult]] = JsonCodecMaker.make
  given tickersCodec: JsonValueCodec[Envelope[TickersResult]] = JsonCodecMaker.make
  given orderCodec: JsonValueCodec[Envelope[OrderResult]] = JsonCodecMaker.make
