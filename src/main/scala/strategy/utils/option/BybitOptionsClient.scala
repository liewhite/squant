package strategy.utils.option

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
  * **实盘**: sellOption 直接提交真实订单 (无 dry-run); testnet=true 走 api-testnet。**首次上真金白银前务必在 testnet 验证**
  * (符号格式/合约张数单位/最小下单量/价格精度), 本客户端未做精度对齐与撤单/持仓查询 (按需补)。
  */
final class BybitOptionsClient(
    backend: SyncBackend,
    credentials: Option[BybitCredentials],
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

  override def linearKlines(symbol: String, interval: String, bars: Int): Either[String, Vector[(Double, Double, Double)]] =
    // [startTime, open, high(2), low(3), close(4), volume, turnover], 倒序返回 -> 反转为最旧->最新
    val q = s"category=linear&symbol=$symbol&interval=$interval&limit=${math.min(1000, bars)}"
    publicGet[Envelope[KlineResult]](s"/v5/market/kline?$q").flatMap { env =>
      env.asEither.map { r =>
        r.list.reverse.flatMap { row =>
          for h <- row.lift(2).flatMap(_.toDoubleOption); l <- row.lift(3).flatMap(_.toDoubleOption); c <- row.lift(4).flatMap(_.toDoubleOption)
          yield (h, l, c)
        }.toVector
      }
    }

  override def optionAccountGreeks(): Either[String, (Double, Double)] =
    credentials match
      case None => Left("optionAccountGreeks 需 API key")
      case Some(c) =>
        signedGet[Envelope[PositionListResult]](c, "/v5/position/list", "category=option").flatMap { env =>
          env.asEither.map { r =>
            // 不可解析的行**不能丢**: 那会让账户 delta 少算一块, 而对冲正是按它下单。
            def num(raw: String, field: String, sym: String): Double =
              raw.toDoubleOption.getOrElse(
                throw IllegalStateException(s"Bybit position 的 $field 不是数字: symbol=$sym 原始值='$raw'")
              )
            (
              r.list.map(p => num(p.delta, "delta", p.symbol)).sum,
              r.list.map(p => num(p.gamma, "gamma", p.symbol)).sum,
            )
          }
        }

  override def optionChain(baseCoin: String): Either[String, Vector[OptionInstrument]] =
    def page(cursor: Option[String], acc: Vector[OptionInstrument]): Either[String, Vector[OptionInstrument]] =
      val q = s"category=option&baseCoin=$baseCoin&limit=1000" + cursor.fold("")(c => s"&cursor=$c")
      publicGet[Envelope[InstrumentsResult]](s"/v5/market/instruments-info?$q").flatMap { env =>
        env.asEither.flatMap { r =>
          val insts = r.list.flatMap { i =>
            OptionContract.parseSymbol(i.symbol).flatMap { case (_, strike, right) =>
              i.deliveryTime.toLongOption.filter(_ > 0).flatMap { exp =>
                // Bybit ETH/BTC 期权每张对应 1 单位标的 (数量本身就是币本位), 故 ctVal=1
                // 精度三件套缺一个就跳过整个合约: `getOrElse(0.0)` 会让 OptionQty.alignDown
                // 跳过对齐、minQty=0 放过任何量 —— 一行坏报文换来下单量校验整体失效。
                for
                  minQty <- i.lotSizeFilter.flatMap(_.minOrderQty.toDoubleOption).filter(_ > 0)
                  qtyStep <- i.lotSizeFilter.flatMap(_.qtyStep.toDoubleOption).filter(_ > 0)
                  tickSize <- i.priceFilter.flatMap(_.tickSize.toDoubleOption).filter(_ > 0)
                yield OptionInstrument(i.symbol, exp, strike, right, ctVal = 1.0, minQty, qtyStep, tickSize)
              }
            }
          }
          val merged = acc ++ insts
          r.nextPageCursor.filter(_.nonEmpty) match
            case Some(c) => page(Some(c), merged)
            case None    => Right(merged)
        }
      }
    page(None, Vector.empty)

  /** 历史隐含波动率指数 (年化小数, 小时级)，单窗口 ≤30 天 (Bybit 限制, 分页由 [[strategy.utils.backtest.BybitIvHistory]] 负责)。
    *
    * `category=option` -> 期权市场反推的 IV (非标的 RV); ETH 期权 USDT 结算 -> `quoteCoin=USDT` 才有数据。
    * `period` = 恒定期限分桶 (7/14/21/30/60/90/180/270 日)。返回 (ts_ms, iv) 升序去重。 */
  def historicalIv(baseCoin: String, quoteCoin: String, period: Int, startMs: Long, endMs: Long): Either[String, Vector[(Long, Double)]] =
    val q = s"category=option&baseCoin=$baseCoin&quoteCoin=$quoteCoin&period=$period&startTime=$startMs&endTime=$endMs"
    publicGet[Envelope[List[IvPoint]]](s"/v5/market/historical-volatility?$q").flatMap { env =>
      env.asEither.map { pts =>
        pts.flatMap(p => p.time.toLongOption.zip(p.value.toDoubleOption)).distinctBy(_._1).sortBy(_._1).toVector
      }
    }

  override def optionQuote(symbol: String): Either[String, Option[Quote]] =
    publicGet[Envelope[TickersResult]](s"/v5/market/tickers?category=option&symbol=$symbol").map { env =>
      env.result.flatMap(_.list.headOption).flatMap { t =>
        for bid <- t.bid1Price.toDoubleOption.filter(_ > 0); ask <- t.ask1Price.toDoubleOption.filter(_ > 0)
        yield Quote(bid, ask)
      }
    }

  override def sellOption(symbol: String, qty: Double, price: Double, postOnly: Boolean, orderLinkId: String): Either[String, String] =
    // postOnly -> 只做 maker (越价被拒); 否则 IOC 限价 (taker, 立即成交且限定最差价)
    val tif = if postOnly then "PostOnly" else "IOC"
    val body =
      s"""{"category":"option","symbol":"$symbol","side":"Sell","orderType":"Limit","qty":"${fmt(qty)}","price":"${fmt(price)}","timeInForce":"$tif","orderLinkId":"$orderLinkId"}"""
    credentials match // 实盘下单, 无 dry-run
      case None => Left("缺少 BYBIT_API_KEY/SECRET, 无法下单")
      case Some(c) =>
        logger.warn(s"[实盘] 提交: SELL $symbol qty=$qty @$price ${if postOnly then "PostOnly" else "IOC(taker)"} link=$orderLinkId")
        signedPost[Envelope[OrderResult]](c, "/v5/order/create", body).flatMap(_.asEither.map(_.orderId))

  // ---- HTTP ----
  private def publicGet[T: JsonValueCodec](pathQuery: String): Either[String, T] =
    try
      val resp = basicRequest.get(Uri.unsafeParse(s"$base$pathQuery")).response(asStringAlways).send(backend)
      if resp.code.isSuccess then Right(readFromString[T](resp.body))
      else Left(s"HTTP ${resp.code} GET $pathQuery: ${resp.body.take(300)}")
    catch case e: Throwable => Left(s"GET $pathQuery failed: ${e.getMessage}")

  private def signedGet[T: JsonValueCodec](c: BybitCredentials, path: String, query: String): Either[String, T] =
    try
      val ts = Instant.now().toEpochMilli.toString
      val prehash = ts + c.apiKey + BybitClient.RecvWindow + query
      val resp = basicRequest
        .get(Uri.unsafeParse(s"$base$path?$query"))
        .header("X-BAPI-API-KEY", c.apiKey)
        .header("X-BAPI-SIGN", BybitClient.hmacSha256Hex(c.apiSecret, prehash))
        .header("X-BAPI-TIMESTAMP", ts)
        .header("X-BAPI-RECV-WINDOW", BybitClient.RecvWindow)
        .response(asStringAlways)
        .send(backend)
      if resp.code.isSuccess then Right(readFromString[T](resp.body))
      else Left(s"HTTP ${resp.code} GET $path: ${resp.body.take(300)}")
    catch case e: Throwable => Left(s"GET $path failed: ${e.getMessage}")

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
  final case class LotSizeFilter(minOrderQty: String, qtyStep: String)
  final case class PriceFilter(tickSize: String)
  final case class InstrumentItem(symbol: String, deliveryTime: String, lotSizeFilter: Option[LotSizeFilter], priceFilter: Option[PriceFilter])
  final case class InstrumentsResult(list: List[InstrumentItem], nextPageCursor: Option[String])
  final case class TickerItem(symbol: String, bid1Price: String, ask1Price: String)
  final case class TickersResult(list: List[TickerItem])
  final case class OrderResult(orderId: String, orderLinkId: String)
  final case class PositionItem(symbol: String, delta: String, gamma: String)
  final case class PositionListResult(list: List[PositionItem])
  /** historical-volatility 行: period 为数字, value/time 为字符串 (年化小数 / ms epoch)。result 直接是数组。 */
  final case class IvPoint(period: Int, value: String, time: String)

  given klineCodec: JsonValueCodec[Envelope[KlineResult]] = JsonCodecMaker.make
  given ivCodec: JsonValueCodec[Envelope[List[IvPoint]]] = JsonCodecMaker.make
  given instrumentsCodec: JsonValueCodec[Envelope[InstrumentsResult]] = JsonCodecMaker.make
  given tickersCodec: JsonValueCodec[Envelope[TickersResult]] = JsonCodecMaker.make
  given orderCodec: JsonValueCodec[Envelope[OrderResult]] = JsonCodecMaker.make
  given positionsCodec: JsonValueCodec[Envelope[PositionListResult]] = JsonCodecMaker.make
