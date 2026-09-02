package hft.exchange.hyperliquid

import hft.domain.{Exchange, ExchangeError, Symbol}

import com.github.plokhotnyuk.jsoniter_scala.core.*
import sttp.client4.*
import sttp.model.{Method, Uri}

import scala.concurrent.duration.*

import HyperliquidCodec.*
import HyperliquidCodec.given

/** Hyperliquid 公共 REST 客户端 —— 目前只回答"这个 perp dex 上市了哪些资产"。
  *
  * ## 为什么不实现 [[hft.exchange.ExchangeClient]]
  *
  * 那个接口要求给出 [[hft.domain.SymbolMeta]]，而其中的 `tickSize` 在 Hyperliquid 上
  * **不是一个常数**：价格精度是"最多 5 位有效数字且不超过 szDecimals 决定的小数位"，
  * 随价格量级变化。填一个假的常数进去，症状不会出现在行情上，而是等到有人拿它对齐下单
  * 价格时被交易所拒单 —— 那时这个谎已经传了很远。
  *
  * 接入下单需要先把"价格精度"表达成交易所可以各自实现的东西 (见 hft-engine-rs 的
  * `SignificantFiguresFormatter`)，那是另一件事。在此之前，本客户端只提供公共行情所需的事实。
  *
  * @param dex perp dex 名称。`""` = 默认 dex (加密永续)，`"xyz"` = 股票/商品永续等 HIP-3 dex
  */
final class HyperliquidClient(
    backend: SyncBackend,
    val dex: String = "",
    restBase: String = HyperliquidClient.RestBaseUrl,
):
  def exchange: Exchange = Exchange.Hyperliquid

  /** 本 dex 当前上市的资产 (已剔除下架)，以框架 [[Symbol]] 给出 */
  def listedSymbols(): Either[ExchangeError, Vector[Symbol]] =
    val body = if dex.isEmpty then """{"type":"meta"}""" else s"""{"type":"meta","dex":"$dex"}"""
    post[MetaResp](body).map { resp =>
      resp.universe.iterator
        .filterNot(_.delisted)
        .flatMap(a => fromHyperliquid(a.name, dex))
        .toVector
    }

  private def post[T: JsonValueCodec](body: String): Either[ExchangeError, T] =
    try
      val response = basicRequest
        .method(Method.POST, Uri.unsafeParse(s"$restBase/info"))
        .header("Content-Type", "application/json")
        .body(body)
        .readTimeout(10.seconds)
        .response(asStringAlways)
        .send(backend)
      if !response.code.isSuccess then Left(ExchangeError.Http(response.code.code, response.body))
      else
        try Right(readFromString[T](response.body))
        catch case e: Exception => Left(ExchangeError.Parse(s"${e.getMessage}; body=${response.body}"))
    catch
      case e: Exception if isInterrupt(e) => throw e
      case e: Exception                   => Left(ExchangeError.Network(s"POST $restBase/info: ${e.getMessage}"))

  /** 异常 cause 链中是否包含线程中断 (ox 作用域取消的信号)，是则重抛而非误判为网络错误 */
  private def isInterrupt(t: Throwable): Boolean =
    Iterator.iterate(t)(_.getCause).takeWhile(_ != null).take(10).exists {
      case _: InterruptedException | _: java.io.InterruptedIOException => true
      case _                                                           => false
    }

object HyperliquidClient:
  val RestBaseUrl = "https://api.hyperliquid.xyz"
  val WsUrl = "wss://api.hyperliquid.xyz/ws"

  /** 股票/商品永续所在的 HIP-3 dex (builder 部署)。默认 dex 上没有这些标的。 */
  val StockDex = "xyz"
