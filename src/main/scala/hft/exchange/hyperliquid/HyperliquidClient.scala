package hft.exchange.hyperliquid

import hft.domain.{Exchange, ExchangeError, Symbol}
import hft.exchange.RestTransport

import com.github.plokhotnyuk.jsoniter_scala.core.*
import sttp.client4.*
import sttp.model.Method

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
    RestTransport
      .send(
        backend,
        Method.POST,
        s"$restBase/info",
        headers = Map("Content-Type" -> "application/json"),
        body = Some(body),
        // 资产清单响应大, 与下单路径的时限无关 (本客户端目前不下单)
        timeout = RestTransport.QueryTimeout,
      )
      .flatMap(RestTransport.parse[T])


object HyperliquidClient:
  val RestBaseUrl = "https://api.hyperliquid.xyz"
  val WsUrl = "wss://api.hyperliquid.xyz/ws"

  /** 股票/商品永续所在的 HIP-3 dex (builder 部署)。默认 dex 上没有这些标的。 */
  val StockDex = "xyz"
