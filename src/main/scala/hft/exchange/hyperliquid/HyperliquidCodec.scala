package hft.exchange.hyperliquid

import hft.domain.{Price, Symbol}

import com.github.plokhotnyuk.jsoniter_scala.core.*
import com.github.plokhotnyuk.jsoniter_scala.macros.*

/** Hyperliquid API 报文结构与 coin 命名转换。
  *
  * 所有字段都有默认值：jsoniter 对缺失字段取默认值、对未知字段直接跳过，
  * 因此可以先用信封探测 `channel` 再按具体类型解析。
  *
  * ## coin 与框架 Symbol
  *
  * Hyperliquid 的一个进程里同时存在多个 perp dex：默认 dex 的资产叫 `"BTC"`，
  * builder 部署的 dex (HIP-3) 叫 `"{dex}:{资产}"`，如股票永续所在的 `"xyz:AAPL"`。
  * 框架的 `Symbol` 与 OKX 一致取**基础资产**(`"AAPL"`)，dex 是接入层配置。
  *
  * 于是 dex 归属必须在边界上校验：默认 dex 的 `"AAPL"` 与 xyz dex 的 `"xyz:AAPL"`
  * 剥掉前缀后是同一个字符串，混进来就是两个不同标的的行情落进同一个 `Instrument`，
  * 而这没有任何外在症状 —— 只是价格莫名其妙地跳。
  */
private[hyperliquid] object HyperliquidCodec:

  // ===== WebSocket: 公共流 =====

  /** 消息类型探测: 只看 "channel" 字段 */
  final case class WsEnvelope(channel: String = "")

  /** 盘口一档。`bbo` 是 [买一, 卖一]，任一侧可能为 null (单边盘口) */
  final case class BboLevel(px: String = "0", sz: String = "0")
  final case class BboData(coin: String = "", time: Long = 0, bbo: List[Option[BboLevel]] = Nil)
  final case class BboPush(channel: String = "", data: BboData = BboData())

  /** 逐笔成交。`side` 是**主动方**方向: "B" = 主动买, "A" = 主动卖 */
  final case class TradeData(coin: String = "", side: String = "", px: String = "0", sz: String = "0", time: Long = 0)
  final case class TradesPush(channel: String = "", data: List[TradeData] = Nil)

  /** 资产上下文：标记价、预言机价与资金费率同在一条推送里 */
  final case class AssetCtx(markPx: String = "0", oraclePx: String = "0", funding: String = "0")
  final case class AssetCtxData(coin: String = "", ctx: AssetCtx = AssetCtx())
  final case class AssetCtxPush(channel: String = "", data: AssetCtxData = AssetCtxData())

  // ===== REST: /info =====

  final case class AssetInfo(name: String = "", szDecimals: Int = 0, isDelisted: Boolean = false)
  final case class MetaResp(universe: List[AssetInfo] = Nil)

  given JsonValueCodec[WsEnvelope] = JsonCodecMaker.make
  given JsonValueCodec[BboPush] = JsonCodecMaker.make
  given JsonValueCodec[TradesPush] = JsonCodecMaker.make
  given JsonValueCodec[AssetCtxPush] = JsonCodecMaker.make
  given JsonValueCodec[MetaResp] = JsonCodecMaker.make

  /** 框架 Symbol -> 线路 coin。默认 dex (`dex` 为空) 直接用基础资产名 */
  def toHyperliquid(symbol: Symbol, dex: String): String =
    if dex.isEmpty then symbol else s"$dex:$symbol"

  /** 线路 coin -> 框架 Symbol；**不属于本 dex 的一律 None**。
    *
    * 判定收在这里而不是留给调用方：漏检一处，别的 dex 的同名资产就会被当成本 dex 的行情
    * 发上总线 (见类文档)。
    */
  def fromHyperliquid(coin: String, dex: String): Option[Symbol] =
    coin.split(':') match
      case Array(base) if dex.isEmpty          => Some(base)
      case Array(prefix, base) if prefix == dex => Some(base)
      case _                                   => None

  /** API 返回的数字字符串。非法即抛错终止——静默归零会造成无法察觉的状态错误。 */
  extension (s: String)
    def asPrice: Price = Price(s.asDouble)
    def asDouble: Double =
      s.toDoubleOption.getOrElse(throw IllegalStateException(s"Invalid number from Hyperliquid API: '$s'"))
