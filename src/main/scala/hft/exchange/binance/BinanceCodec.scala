package hft.exchange.binance

import com.github.plokhotnyuk.jsoniter_scala.core.*
import com.github.plokhotnyuk.jsoniter_scala.macros.*

/** Binance USDⓈ-M 合约 API 报文结构 (字段名与官方 API 一致，数字以字符串传输)。
  *
  * 所有字段都有默认值：jsoniter 对缺失字段取默认值，对未知字段直接跳过，
  * 因此同一 envelope 可安全地探测消息类型后再做具体解析。
  */
private[binance] object BinanceCodec:

  // ===== WebSocket: 公共流 =====

  /** 消息类型探测: 只看 "e" 字段；订阅 ack 等无 e 字段的消息得到空串 */
  final case class WsEnvelope(e: String = "")

  final case class BookTickerMsg(
      s: String = "",  // symbol
      b: String = "0", // best bid price
      B: String = "0", // best bid qty
      a: String = "0", // best ask price
      A: String = "0", // best ask qty
      E: Long = 0,     // event time
  )

  final case class MarkPriceMsg(
      s: String = "",  // symbol
      p: String = "0", // mark price
      i: String = "0", // index price
      r: String = "0", // funding rate
      T: Long = 0,     // next funding time
      E: Long = 0,     // event time
  )

  // ===== WebSocket: 私有流 (user data stream) =====

  final case class OrderTradeUpdateMsg(E: Long = 0, o: OrderData = OrderData())

  final case class OrderData(
      s: String = "",  // symbol
      c: String = "",  // client order id
      S: String = "",  // side: BUY/SELL
      q: String = "0", // original quantity
      p: String = "0", // original price
      X: String = "",  // order status
      i: Long = 0,     // order id
      l: String = "0", // last filled qty
      z: String = "0", // cumulative filled qty
      L: String = "0", // last filled price
      T: Long = 0,     // transaction time
  )

  final case class AccountUpdateMsg(E: Long = 0, a: AccountData = AccountData())
  final case class AccountData(B: List[WsBalance] = Nil, P: List[WsPosition] = Nil)
  final case class WsBalance(a: String = "", wb: String = "0") // asset, wallet balance
  final case class WsPosition(
      s: String = "",   // symbol
      pa: String = "0", // position amount
      ep: String = "0", // entry price
      up: String = "0", // unrealized pnl
      ps: String = "BOTH",
  )

  // ===== REST =====

  final case class ExchangeInfo(symbols: List[SymbolInfo] = Nil)
  final case class SymbolInfo(
      symbol: String = "",
      status: String = "",
      contractType: String = "",
      filters: List[FilterInfo] = Nil,
  )
  final case class FilterInfo(
      filterType: String = "",
      tickSize: String = "0",
      stepSize: String = "0",
      minQty: String = "0",
  )

  final case class NewOrderResp(orderId: Long = 0)

  final case class OpenOrder(
      orderId: Long = 0,
      clientOrderId: String = "",
      price: String = "0",
      origQty: String = "0",
      executedQty: String = "0",
      side: String = "",
      status: String = "",
      symbol: String = "",
      time: Long = 0,
  )

  final case class AccountResp(
      totalMarginBalance: String = "0",
      positions: List[AccountPosition] = Nil,
  )
  final case class AccountPosition(symbol: String = "", notional: String = "0")

  final case class PositionRisk(
      symbol: String = "",
      positionAmt: String = "0",
      entryPrice: String = "0",
      unRealizedProfit: String = "0",
  )

  final case class ListenKeyResp(listenKey: String = "")

  given JsonValueCodec[WsEnvelope] = JsonCodecMaker.make
  given JsonValueCodec[BookTickerMsg] = JsonCodecMaker.make
  given JsonValueCodec[MarkPriceMsg] = JsonCodecMaker.make
  given JsonValueCodec[OrderTradeUpdateMsg] = JsonCodecMaker.make
  given JsonValueCodec[AccountUpdateMsg] = JsonCodecMaker.make
  given JsonValueCodec[ExchangeInfo] = JsonCodecMaker.make
  given JsonValueCodec[NewOrderResp] = JsonCodecMaker.make
  given openOrdersCodec: JsonValueCodec[List[OpenOrder]] = JsonCodecMaker.make
  given JsonValueCodec[AccountResp] = JsonCodecMaker.make
  given positionRisksCodec: JsonValueCodec[List[PositionRisk]] = JsonCodecMaker.make
  given JsonValueCodec[ListenKeyResp] = JsonCodecMaker.make

  /** API 返回的数字字符串。非法即抛错终止——静默归零会造成无法察觉的状态错误。
    * (字段缺失时 codec 默认值为 "0"，解析为 0.0，语义即零值)
    */
  extension (s: String)
    def asDouble: Double =
      s.toDoubleOption.getOrElse(throw IllegalStateException(s"Invalid number from Binance API: '$s'"))
