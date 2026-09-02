package hft.exchange.hyperliquid

import hft.domain.{Price, Symbol}

import com.github.plokhotnyuk.jsoniter_scala.core.*
import com.github.plokhotnyuk.jsoniter_scala.macros.*

/** Hyperliquid API 报文结构与 coin 命名转换。
  *
  * ## 字段一律必填，缺失即抛
  *
  * 这些 case class **不给默认值**。jsoniter 对无默认值的字段要求必须出现，缺了就抛
  * `JsonReaderException` —— 这正是想要的：交易所少发一个 `px`，那是坏报文，不是价格 0。
  * 给默认值等于把"报文有问题"翻译成一个合法的零值，让它一路流进价差计算，
  * 而**没有任何外在症状**：均线慢慢被零值拖偏，报出来的偏离全是假的。
  *
  * 真正**合法可缺**的字段才用 `Option`，且只有一个：[[AssetInfo.isDelisted]] ——
  * Hyperliquid 只在已下架的资产上下发它。用 `Option` 说的是"接口可以不表态"，
  * 用 `= false` 说的是"没说就是没下架"，后者把一个观测不到的事实伪装成观测到的。
  *
  * 未知字段仍然直接跳过 (jsoniter 默认行为)，因此报文里多出来的东西不影响解析，
  * 也因此各 push 类型只声明自己要用的那部分。
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

  /** 消息类型探测: 只看 "channel" 字段。
    *
    * 服务端每一帧都带它 (数据推送、订阅应答、pong、错误)，所以它是必填的 ——
    * 没有 channel 的帧是我们不认得的东西，不该被当成"某个默认频道"继续处理。
    */
  final case class WsEnvelope(channel: String)

  /** 盘口一档 */
  final case class BboLevel(px: String, sz: String)

  /** `bbo` 是 [买一, 卖一]。**元素可为 null**：单边盘口是稀薄品种的合法市场状态，
    * 故这一层用 `Option` 而不是默认值 —— 缺的是"那一侧有没有报价"这个事实本身。 */
  final case class BboData(coin: String, time: Long, bbo: List[Option[BboLevel]])
  final case class BboPush(data: BboData)

  /** 逐笔成交。`side` 是**主动方**方向: "B" = 主动买, "A" = 主动卖 */
  final case class TradeData(coin: String, side: String, px: String, sz: String, time: Long)
  final case class TradesPush(data: List[TradeData])

  /** 资产上下文：标记价、预言机价与资金费率同在一条推送里 */
  final case class AssetCtx(markPx: String, oraclePx: String, funding: String)
  final case class AssetCtxData(coin: String, ctx: AssetCtx)
  final case class AssetCtxPush(data: AssetCtxData)

  // ===== REST: /info =====

  /** 一个资产。只声明用得上的字段，其余由 jsoniter 跳过。
    *
    * `isDelisted` 是本文件唯一的 `Option`：接口只在**已下架**的资产上下发它，
    * 不下发是它表达"在架"的方式 —— 那是合法缺失，不是坏报文。
    */
  final case class AssetInfo(name: String, isDelisted: Option[Boolean]):
    def delisted: Boolean = isDelisted.getOrElse(false)

  final case class MetaResp(universe: List[AssetInfo])

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
