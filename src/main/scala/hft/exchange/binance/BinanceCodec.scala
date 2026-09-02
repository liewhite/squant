package hft.exchange.binance

import hft.domain.{Price, Side}

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
  /** 公共流帧的探测包络：数据帧带事件类型 `e`，请求应答不带 —— 成功是 `result:null`、
    * 失败是 `error:{code,msg}`。两者都必须能被区分出来，见 [[BinanceMarketFeed]]。 */
  final case class WsEnvelope(e: String = "", id: Long = 0L, error: Option[WsError] = None)

  final case class WsError(code: Int, msg: String)

  /** `/fapi/v1/positionSide/dual`: true = 双向持仓(hedge)，false = 单向持仓。 */
  final case class PositionSideDual(dualSidePosition: Boolean = true)

  /** Binance 方向 -> 统一方向。穷举 BUY/SELL，其余抛。
    *
    * 从前是 `if side == "BUY" then Long else Short`：任何非 "BUY" 的值 (含 codec 默认的空串)
    * 都变成 Short —— **方向静默取反**。Bybit/OKX 侧对未知方向都是抛的，这里也必须一致。 */
  def sideFromBinance(side: String): Side = side match
    case "BUY"  => Side.Long
    case "SELL" => Side.Short
    case other  => throw IllegalStateException(s"未知的 Binance 方向: '$other'")

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

  /** 归集成交 (aggTrade)：逐笔成交印记 */
  final case class AggTradeMsg(
      s: String = "",       // symbol
      p: String = "0",      // price
      q: String = "0",      // quantity
      m: Boolean = false,   // 买方是否为挂单方 (true -> 主动卖出)
      T: Long = 0,          // trade time
      E: Long = 0,          // event time
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
      ap: String = "0", // average filled price —— 记账用它, 不能用 p (市价单为 0)
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
      /** 标的资产代码 —— 传统资产永续里它就是股票/商品代码 (AAPLUSDT -> "AAPL")。
        *
        * **无默认值**: 缺了它就抛。给 `""` 兜底的话，[[BinancePublicClient.fetchTradFiPerps]]
        * 的 `baseAsset.nonEmpty` 会把这一条静默丢掉 —— 少监控一个标的，没有任何症状。 */
      baseAsset: String,
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

  given JsonValueCodec[PositionSideDual] = JsonCodecMaker.make
  given JsonValueCodec[WsEnvelope] = JsonCodecMaker.make
  given JsonValueCodec[BookTickerMsg] = JsonCodecMaker.make
  given JsonValueCodec[MarkPriceMsg] = JsonCodecMaker.make
  given JsonValueCodec[AggTradeMsg] = JsonCodecMaker.make
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
    /** 解析成价格 —— 交易所报文是 Price 进入框架的唯一入口 */
    def asPrice: Price = Price(s.asDouble)
    def asDouble: Double =
      s.toDoubleOption.getOrElse(throw IllegalStateException(s"Invalid number from Binance API: '$s'"))
