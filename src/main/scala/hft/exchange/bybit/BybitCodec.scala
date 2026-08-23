package hft.exchange.bybit

import com.github.plokhotnyuk.jsoniter_scala.core.*
import com.github.plokhotnyuk.jsoniter_scala.macros.*
import hft.domain.*

/** Bybit v5 API 报文结构 (USDT linear 永续)。字段名与官方 API 一致，数字以字符串传输。
  *
  * 所有字段都有默认值：jsoniter 对缺失字段取默认值、未知字段直接跳过，因此——
  *   - WS 同一报文先用 [[BybitWsMsg]] 探测 op (控制帧) / topic (数据帧)，再按 topic 解析具体 push;
  *   - tickers 为 snapshot+delta，delta 仅含变化字段，缺失字段保持默认空串 ("") —— 解析层据此跳过未变字段。
  *
  * 单位约定 (与 OKX 的关键差异)：Bybit linear 永续 qty 以**币本位**计价、无合约乘数，
  * 故 contractSize=1.0，盘口/成交数量直接即币本位，无需张<->币换算。symbol 原生即统一格式 ("BTCUSDT")。
  */
private[bybit] object BybitCodec:

  // ==================== Symbol / 状态映射 ====================

  /** Bybit symbol 即框架统一 Symbol ("BTCUSDT")，恒等转换，仅做非空校验 */
  def fromBybit(symbol: String): Option[Symbol] =
    if symbol.nonEmpty then Some(symbol) else None

  /** Bybit 订单状态映射。cum 为累计成交量 (币本位)。未知状态归为 Rejected，由上层决定是否致命。
    * PartiallyFilledCanceled/Deactivated 为终态撤单 (部分成交后撤 / 条件单失效)。
    */
  def mapOrderStatus(status: String, cum: Coin): OrderStatus = status match
    case "New"                                          => OrderStatus.Pending
    case "PartiallyFilled"                              => OrderStatus.PartiallyFilled(cum)
    case "Filled"                                       => OrderStatus.Filled
    case "Cancelled" | "PartiallyFilledCanceled" | "Deactivated" => OrderStatus.Cancelled
    case "Rejected"                                     => OrderStatus.Rejected("Bybit rejected")
    case other                                          => OrderStatus.Rejected(s"Unknown Bybit status: $other")

  /** 统一方向 -> Bybit 下单方向 */
  def sideToParam(side: Side): String = side match
    case Side.Long  => "Buy"
    case Side.Short => "Sell"

  /** Bybit 方向 -> 统一方向。非 Buy/Sell (如空仓的空串) 由调用方自行处理，此处仅用于已知有向语境 */
  def sideFromBybit(side: String): Side = side match
    case "Buy"  => Side.Long
    case "Sell" => Side.Short
    case other  => throw IllegalStateException(s"Unknown Bybit side: '$other'")

  /** 限价单 TimeInForce -> Bybit 参数 */
  def tifToParam(tif: TimeInForce): String = tif match
    case TimeInForce.GTC      => "GTC"
    case TimeInForce.IOC      => "IOC"
    case TimeInForce.FOK      => "FOK"
    case TimeInForce.PostOnly => "PostOnly"

  // ==================== WebSocket: 通用包络 ====================

  /** 控制帧 (op: auth/subscribe/ping/pong) 与数据帧 (topic 非空) 的探测包络。
    * success/ret_msg 为控制帧的结果 (WS 用 snake_case，区别于 REST 的 retCode/retMsg)。
    */
  final case class BybitWsMsg(
      op: String = "",
      topic: String = "",
      success: Boolean = true,
      ret_msg: String = "",
  )

  /** data 为单对象的推送 (orderbook / tickers) */
  final case class WsObj[T](topic: String = "", ts: Long = 0L, data: T)

  /** data 为数组的推送 (publicTrade / order / execution / wallet) */
  final case class WsList[T](topic: String = "", ts: Long = 0L, data: List[T] = Nil)

  // ==================== WebSocket: 公共频道数据 ====================

  /** orderbook.1: b/a 为 [[价格, 数量], ...]，level1 仅 snapshot，取首档。数量单位为币本位 */
  final case class OrderbookData(
      s: String = "",
      b: List[List[String]] = Nil,
      a: List[List[String]] = Nil,
  )

  /** tickers: snapshot+delta，缺失字段 (空串) 表示未变化，解析层逐字段跳过 */
  final case class TickerData(
      symbol: String = "",
      markPrice: String = "",
      indexPrice: String = "",
      fundingRate: String = "",
      nextFundingTime: String = "",
  )

  /** publicTrade: S 为 taker 方向 ("Buy"/"Sell")，T 为成交时间 (ms) */
  final case class PublicTradeData(s: String = "", p: String = "0", v: String = "0", S: String = "", T: Long = 0L)

  // ==================== WebSocket: 私有频道数据 ====================

  final case class OrderData(
      symbol: String = "",
      orderId: String = "",
      orderLinkId: String = "",
      side: String = "",       // Buy/Sell
      orderStatus: String = "",
      price: String = "",      // 市价单为空
      qty: String = "0",
      cumExecQty: String = "0", // 累计成交 (币本位)
  )

  final case class ExecutionData(
      symbol: String = "",
      side: String = "",       // Buy/Sell
      execQty: String = "0",   // 本次成交 (币本位)
      execPrice: String = "0",
      execTime: String = "0",
  )

  final case class WalletCoin(coin: String = "", walletBalance: String = "0")
  final case class WalletData(
      accountType: String = "",
      totalEquity: String = "0",
      coin: List[WalletCoin] = Nil,
  )

  // ==================== REST 响应 (顶层 retCode 数字, result 包裹) ====================

  final case class PriceFilter(tickSize: String = "0")
  final case class LotSizeFilter(qtyStep: String = "0", minOrderQty: String = "0")
  final case class InstrumentData(
      symbol: String = "",
      priceFilter: PriceFilter = PriceFilter(),
      lotSizeFilter: LotSizeFilter = LotSizeFilter(),
  )
  final case class InstrumentsResult(list: List[InstrumentData] = Nil, nextPageCursor: String = "")
  final case class InstrumentsResp(retCode: Int = -1, retMsg: String = "", result: InstrumentsResult = InstrumentsResult())

  final case class OrderCreateResult(orderId: String = "", orderLinkId: String = "")
  final case class OrderCreateResp(retCode: Int = -1, retMsg: String = "", result: OrderCreateResult = OrderCreateResult())

  /** cancel 只关心 retCode/retMsg (订单不存在归一为 OrderNotFound) */
  final case class CancelResp(retCode: Int = -1, retMsg: String = "")

  final case class OpenOrderData(
      symbol: String = "",
      orderId: String = "",
      orderLinkId: String = "",
      side: String = "",
      orderStatus: String = "",
      price: String = "0",
      qty: String = "0",
      cumExecQty: String = "0",
  )
  final case class OpenOrdersResult(list: List[OpenOrderData] = Nil, nextPageCursor: String = "")
  final case class OpenOrdersResp(retCode: Int = -1, retMsg: String = "", result: OpenOrdersResult = OpenOrdersResult())

  final case class PositionData(
      symbol: String = "",
      side: String = "",          // Buy/Sell；空仓为 ""
      size: String = "0",         // 恒为正，方向由 side 决定
      avgPrice: String = "0",     // 空仓可能为 ""
      unrealisedPnl: String = "0",
  )
  final case class PositionListResult(list: List[PositionData] = Nil)
  final case class PositionListResp(retCode: Int = -1, retMsg: String = "", result: PositionListResult = PositionListResult())

  /** wallet-balance REST：result.list[0] 为账户聚合 (复用 WalletData) */
  final case class WalletResult(list: List[WalletData] = Nil)
  final case class WalletResp(retCode: Int = -1, retMsg: String = "", result: WalletResult = WalletResult())

  /** 仅含 retCode/retMsg 的简单响应 (set-leverage 等) */
  final case class SimpleResp(retCode: Int = -1, retMsg: String = "")

  // ==================== codecs ====================

  given JsonValueCodec[BybitWsMsg] = JsonCodecMaker.make
  given orderbookPush: JsonValueCodec[WsObj[OrderbookData]] = JsonCodecMaker.make
  given tickerPush: JsonValueCodec[WsObj[TickerData]] = JsonCodecMaker.make
  given tradePush: JsonValueCodec[WsList[PublicTradeData]] = JsonCodecMaker.make
  given orderPush: JsonValueCodec[WsList[OrderData]] = JsonCodecMaker.make
  given execPush: JsonValueCodec[WsList[ExecutionData]] = JsonCodecMaker.make
  given walletPush: JsonValueCodec[WsList[WalletData]] = JsonCodecMaker.make
  given JsonValueCodec[InstrumentsResp] = JsonCodecMaker.make
  given JsonValueCodec[OrderCreateResp] = JsonCodecMaker.make
  given JsonValueCodec[CancelResp] = JsonCodecMaker.make
  given JsonValueCodec[OpenOrdersResp] = JsonCodecMaker.make
  given JsonValueCodec[PositionListResp] = JsonCodecMaker.make
  given JsonValueCodec[WalletResp] = JsonCodecMaker.make
  given JsonValueCodec[SimpleResp] = JsonCodecMaker.make

  /** API 返回的数字字符串。非法即抛错终止——静默归零会造成无法察觉的状态错误 */
  extension (s: String)
    def asDouble: Double =
      s.toDoubleOption.getOrElse(throw IllegalStateException(s"Invalid number from Bybit API: '$s'"))

    /** 可空数字：空字符串视为 0.0 (空仓 avgPx、市价单 px) */
    def asDoubleOrZero: Double =
      if s.isEmpty then 0.0 else s.asDouble
