package hft.exchange.okx

import com.github.plokhotnyuk.jsoniter_scala.core.*
import com.github.plokhotnyuk.jsoniter_scala.macros.*
import hft.domain.*

/** OKX API 报文结构 (字段名与官方 API 一致，数字以字符串传输)。
  *
  * 所有字段都有默认值：jsoniter 对缺失字段取默认值、未知字段直接跳过，因此同一报文可先用
  * [[OkxEnvelope]] 探测 event / channel，再按 channel 解析具体的 [[WsPush]]。
  */
private[okx] object OkxCodec:

  // ==================== Symbol ↔ instId 转换 ====================
  //
  // OKX 统一 symbol 即基础币 ("BTC")，与计价币 quote 拼成 instId。
  //   永续:   to_okx("BTC","USDT")        = "BTC-USDT-SWAP"
  //   指数:   to_okx_index("BTC","USDT")  = "BTC-USDT"

  def toOkx(symbol: Symbol, quote: String): String = s"$symbol-$quote-SWAP"
  def toOkxIndex(symbol: Symbol, quote: String): String = s"$symbol-$quote"

  /** `"BTC-USDT-SWAP"` -> `Some("BTC")`；**计价币不是 `quote` 的、非永续的，一律 None**。
    *
    * quote 必须在这里检查, 不能留给调用方。框架的 `Symbol` 只有基础币, 于是币本位的
    * `ETH-USD-SWAP` 与 `ETH-USDT-SWAP` 会收敛成同一个 `"ETH"` —— 而私有流按 instType
    * **全量**订阅、`/account/positions` 也返回**全部**计价币种的永续。漏检一处, 币本位的
    * 仓位与订单回报就会被当成 USDT 合约记账: 拿错的 ctVal 换张成币, 还会在对齐快照
    * `toMap` 时静默覆盖真正的那一行。
    *
    * 这条检查一旦散在各入口, 总有一个入口会忘 —— 本文件原先三个全量入口就漏了两个。
    */
  def fromOkx(instId: String, quote: String): Option[Symbol] =
    instId.split('-') match
      case Array(base, q, "SWAP") if q == quote => Some(base)
      case _                                    => None

  /** "BTC-USDT" -> Some("BTC") */
  def fromOkxIndex(instId: String): Option[Symbol] =
    instId.split('-') match
      case Array(base, _) => Some(base)
      case _              => None

  /** OKX 订单状态映射。按文档的**完整**枚举，只有文档之外的值才抛。
    *
    * 依据 OKX v5 文档: 终态是 `filled` / `canceled` / `mmp_canceled` (做市商保护撤单),
    * 未终结是 `live` / `partially_filled`。
    *
    * 归成 `OrderStatus.Rejected` 会让一张还活着的单被本地宣告死亡 (终态触发 markTerminal、
    * 清掉 pending 登记), 所以未知值必须抛而不是归成终态；而**文档里有的**状态必须映射到位,
    * 否则同样是启动对齐时见到一张 mmp_canceled 的历史单就崩。 */
  def mapOrderState(state: String, filled: Coin): OrderStatus = state match
    case "live"                                   => OrderStatus.Pending
    case "partially_filled"                       => OrderStatus.PartiallyFilled(filled)
    case "filled"                                 => OrderStatus.Filled
    case "canceled" | "cancelled" | "mmp_canceled" => OrderStatus.Cancelled
    case other => throw IllegalStateException(s"文档之外的 OKX 订单状态: '$other' (filled=${filled.value})")

  // ==================== WebSocket: 通用包络 ====================

  /** 订阅参数 (推送与订阅共用)：channel + instId */
  final case class WsArg(channel: String = "", instId: String = "")

  /** 控制消息 / 推送探测：event 非空即控制消息 (subscribe/unsubscribe/error/login/...)，
    * 否则按 arg.channel 分派数据解析
    */
  final case class OkxEnvelope(
      event: String = "",
      code: String = "",
      msg: String = "",
      arg: WsArg = WsArg(),
  )

  /** 通用推送：arg 标识频道/合约，data 为该频道的数据数组 */
  final case class WsPush[T](arg: WsArg = WsArg(), data: List[T] = Nil)

  // ==================== WebSocket: 公共频道数据 ====================

  final case class FundingRateData(
      instId: String = "",
      fundingRate: String = "0",
      fundingTime: String = "0", // 下次结算时间 (ms)
  )

  /** bbo-tbt: asks/bids 为 [[价格, 数量, ...], ...]，取首档 */
  final case class BboData(
      asks: List[List[String]] = Nil,
      bids: List[List[String]] = Nil,
      ts: String = "0",
  )

  final case class MarkPriceData(instId: String = "", markPx: String = "0", ts: String = "0")

  final case class IndexTickerData(instId: String = "", idxPx: String = "0", ts: String = "0")

  /** trades 频道：逐笔成交。side 为 taker 方向 ("buy"/"sell") */
  final case class TradeData(instId: String = "", px: String = "0", sz: String = "0", side: String = "", ts: String = "0")

  // ==================== WebSocket: 私有频道数据 ====================

  final case class PositionData(
      instId: String = "",
      pos: String = "0",   // 张数；正多负空
      avgPx: String = "",  // 空仓时为空字符串
      upl: String = "",    // 空仓时为空字符串
  )

  final case class AccountDetail(ccy: String = "", cashBal: String = "0")
  final case class AccountData(
      uTime: String = "0",
      totalEq: String = "0",
      notionalUsd: String = "0",
      details: List[AccountDetail] = Nil,
  )

  final case class OrderPushData(
      instId: String = "",
      ordId: String = "",
      clOrdId: String = "",
      side: String = "",
      state: String = "",
      px: String = "",      // 市价单为空
      sz: String = "0",     // 张
      fillSz: String = "0", // 本次成交 (张)
      fillPx: String = "0",
      accFillSz: String = "0", // 累计成交 (张)
      avgPx: String = "0",     // 累计成交均价；未成交时为空
  )

  // ==================== REST 响应 ====================

  /** 合约规格。
    *
    * `state` 是**必须读的**：未上市的合约 (`preopen`) 会带着一整排空字符串下发规格字段，
    * 而空串不是 "0"，`asDouble` 对它抛错 —— 于是交易所预告一个新合约就能让所有 OKX 客户端
    * 在启动拉规格时崩掉，与本进程交易什么毫不相干。规格得等它真正上市才有意义。
    *
    * 它**无默认值**，缺了就抛：给 `""` 兜底的话，
    * [[OkxPublicClient.fetchAllSymbolMetas]] 的状态过滤会把**每一条**都判为不合格，
    * 于是进程带着一张空的规格表启动 —— 下不了单、换不了算，而启动本身是"成功"的。
    */
  final case class InstrumentData(
      instId: String,
      state: String,
      tickSz: String,
      lotSz: String,
      minSz: String,
      ctVal: String,
  ):
    /** 尚未上市 —— 规格字段此刻为空串，读它就是崩溃。
      *
      * 判据取"尚未上市"而不是"是否 live"：`suspend` (临时停牌) 的合约规格是齐全的，
      * 而且**账户上可能正持有它的仓位**。把它一并剔出规格表，`fetchPositions` 就会
      * 静默丢掉那条持仓、启动对齐得出"已平仓"的结论 —— 比崩溃危险得多。
      */
    def notYetListed: Boolean = state == "preopen"

  final case class InstrumentsResp(code: String = "", msg: String = "", data: List[InstrumentData] = Nil)

  final case class BalanceRespData(totalEq: String = "0")
  final case class BalanceResp(code: String = "", msg: String = "", data: List[BalanceRespData] = Nil)

  final case class GreeksData(
      ccy: String = "",
      deltaBS: String = "0",
      gammaBS: String = "0",
      thetaBS: String = "0",
      vegaBS: String = "0",
      ts: String = "0",
  )
  final case class GreeksResp(code: String = "", msg: String = "", data: List[GreeksData] = Nil)

  final case class PlaceOrderData(ordId: String = "", sCode: String = "0", sMsg: String = "")
  final case class PlaceOrderResp(code: String = "", msg: String = "", data: List[PlaceOrderData] = Nil)

  final case class CancelData(sCode: String = "0", sMsg: String = "")
  final case class CancelResp(code: String = "", msg: String = "", data: List[CancelData] = Nil)

  final case class PendingData(
      instId: String = "",
      ordId: String = "",
      clOrdId: String = "",
      side: String = "",
      state: String = "",
      px: String = "0",
      sz: String = "0",
      accFillSz: String = "0",
      /** 交易所侧的最后更新时刻 (ms)。用它而不是本地钟：柜台把它当 exchangeTs 用作延迟基准。 */
      uTime: String = "",
  )
  final case class PendingResp(code: String = "", msg: String = "", data: List[PendingData] = Nil)

  /** GET /api/v5/account/positions 的响应。行的形状与私有 WS positions 频道一致,
    * 故复用 [[PositionData]] —— 同一个事实两处解析, 迟早会有一处漏掉某个字段。 */
  final case class PositionsResp(code: String = "", msg: String = "", data: List[PositionData] = Nil)

  /** 仅含 code/msg 的简单响应 (set-leverage 等) */
  final case class SimpleResp(code: String = "", msg: String = "")

  // ==================== codecs ====================

  given JsonValueCodec[OkxEnvelope] = JsonCodecMaker.make
  given fundingPush: JsonValueCodec[WsPush[FundingRateData]] = JsonCodecMaker.make
  given bboPush: JsonValueCodec[WsPush[BboData]] = JsonCodecMaker.make
  given markPush: JsonValueCodec[WsPush[MarkPriceData]] = JsonCodecMaker.make
  given indexPush: JsonValueCodec[WsPush[IndexTickerData]] = JsonCodecMaker.make
  given tradePush: JsonValueCodec[WsPush[TradeData]] = JsonCodecMaker.make
  given positionPush: JsonValueCodec[WsPush[PositionData]] = JsonCodecMaker.make
  given accountPush: JsonValueCodec[WsPush[AccountData]] = JsonCodecMaker.make
  given orderPush: JsonValueCodec[WsPush[OrderPushData]] = JsonCodecMaker.make
  given JsonValueCodec[InstrumentsResp] = JsonCodecMaker.make
  given JsonValueCodec[BalanceResp] = JsonCodecMaker.make
  given JsonValueCodec[GreeksResp] = JsonCodecMaker.make
  given JsonValueCodec[PlaceOrderResp] = JsonCodecMaker.make
  given JsonValueCodec[CancelResp] = JsonCodecMaker.make
  given JsonValueCodec[PendingResp] = JsonCodecMaker.make
  given JsonValueCodec[PositionsResp] = JsonCodecMaker.make
  given JsonValueCodec[SimpleResp] = JsonCodecMaker.make

  /** API 返回的数字字符串。非法即抛错终止——静默归零会造成无法察觉的状态错误 */
  extension (s: String)
    /** 解析成价格 —— 交易所报文是 Price 进入框架的唯一入口 */
    def asPrice: Price = Price(s.asDouble)
    def asDouble: Double =
      s.toDoubleOption.getOrElse(throw IllegalStateException(s"Invalid number from OKX API: '$s'"))

    /** 可空数字：空字符串视为 0.0 (OKX 空仓的 avgPx/upl、市价单的 px) */
    def asDoubleOrZero: Double =
      if s.isEmpty then 0.0 else s.asDouble
