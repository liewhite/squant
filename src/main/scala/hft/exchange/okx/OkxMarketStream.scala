package hft.exchange.okx

import com.github.plokhotnyuk.jsoniter_scala.core.*
import hft.domain.*
import hft.exchange.{MarketDataStream, SubscriptionKind, WsLoop}
import hft.messaging.{EventBus, EventData, IncomeEvent}
import org.slf4j.LoggerFactory
import ox.Ox
import ox.channels.Channel
import sttp.client4.WebSocketSyncBackend
import sttp.ws.WebSocketFrame

import OkxCodec.*
import OkxCodec.given

/** OKX 永续合约公共行情连接器。
  *
  * OKX 所有公共频道复用单条 `/ws/v5/public` 连接，订阅以 `{"op":"subscribe","args":[...]}` 增量下发：
  *   - bbo-tbt      -> BBO
  *   - funding-rate -> FundingRate
  *   - mark-price   -> MarkPrice
  *   - index-tickers-> IndexPrice (用指数 instId, 如 BTC-USDT)
  *
  * Fail-fast：连接断开、解析失败、错误事件 (event=error) 一律抛异常终止引擎作用域。
  */
final class OkxMarketStream(
    backend: WebSocketSyncBackend,
    quote: String = "USDT",
    wsUrl: String = OkxClient.WsPublicUrl,
) extends MarketDataStream:
  private val logger = LoggerFactory.getLogger(classOf[OkxMarketStream])

  override def exchange: Exchange = Exchange.Okx

  private val outgoing = Channel.unlimited[WebSocketFrame]
  private var bus: EventBus[IncomeEvent] = scala.compiletime.uninitialized

  override def start(incomeBus: EventBus[IncomeEvent])(using Ox): Unit =
    bus = incomeBus
    WsLoop.run("okx/public", backend, () => wsUrl, outgoing, onPublicText)

  override def subscribe(kinds: Set[SubscriptionKind]): Unit =
    if kinds.nonEmpty then
      val args = kinds.map(argJson).mkString(",")
      val message = s"""{"op":"subscribe","args":[$args]}"""
      logger.info(s"subscribing on okx/public: ${kinds.map(_.subscribedSymbol)}")
      outgoing.send(WebSocketFrame.text(message))

  private def argJson(kind: SubscriptionKind): String = kind match
    case SubscriptionKind.BBO(s)         => s"""{"channel":"bbo-tbt","instId":"${toOkx(s, quote)}"}"""
    case SubscriptionKind.FundingRate(s) => s"""{"channel":"funding-rate","instId":"${toOkx(s, quote)}"}"""
    case SubscriptionKind.MarkPrice(s)   => s"""{"channel":"mark-price","instId":"${toOkx(s, quote)}"}"""
    case SubscriptionKind.IndexPrice(s)  => s"""{"channel":"index-tickers","instId":"${toOkxIndex(s, quote)}"}"""

  // ==================== 公共流解析 (解析失败/错误事件 -> 异常上抛终止) ====================

  private def onPublicText(text: String): Unit =
    val env = readFromString[OkxEnvelope](text)
    if env.event.nonEmpty then handleControl(env, text)
    else
      env.arg.channel match
        case "bbo-tbt"      => readFromString[WsPush[BboData]](text).data.foreach(d => publishBbo(env.arg.instId, d))
        case "funding-rate" => readFromString[WsPush[FundingRateData]](text).data.foreach(publishFunding)
        case "mark-price"   => readFromString[WsPush[MarkPriceData]](text).data.foreach(publishMark)
        case "index-tickers" => readFromString[WsPush[IndexTickerData]](text).data.foreach(publishIndex)
        case other          => throw IllegalStateException(s"Unexpected OKX public channel '$other': $text")

  private def handleControl(env: OkxEnvelope, text: String): Unit = env.event match
    case "subscribe" | "unsubscribe" | "channel-conn-count" => ()
    case "error" => throw IllegalStateException(s"OKX public WS error: code=${env.code} msg=${env.msg}")
    case other   => logger.warn(s"ignoring OKX public event '$other': $text")

  private def requireSymbol(instId: String): Symbol =
    fromOkx(instId).getOrElse(throw IllegalStateException(s"Unknown OKX instId: '$instId'"))

  // 注意: OKX bbo-tbt 的盘口数量单位为**合约张数**，此处未转币本位 (Binance BBO 为币本位)。
  // 当前无消费者读取 bidQty/askQty；若策略按盘口深度定 size，须先用 SymbolMeta.qtyToCoin 换算。
  private def publishBbo(instId: String, d: BboData): Unit =
    val sym = requireSymbol(instId)
    val ask = d.asks.headOption.getOrElse(throw IllegalStateException(s"OKX bbo empty asks: $instId"))
    val bid = d.bids.headOption.getOrElse(throw IllegalStateException(s"OKX bbo empty bids: $instId"))
    val ts = d.ts.toLong
    val bbo = BBO(Exchange.Okx, sym, bid.head.asDouble, bid(1).asDouble, ask.head.asDouble, ask(1).asDouble, ts)
    bus.publish(IncomeEvent.at(ts, EventData.BboUpdate(bbo)))

  private def publishMark(d: MarkPriceData): Unit =
    val ts = d.ts.toLong
    bus.publish(IncomeEvent.at(ts, EventData.MarkPriceUpdate(MarkPrice(Exchange.Okx, requireSymbol(d.instId), d.markPx.asDouble, ts))))

  private def publishIndex(d: IndexTickerData): Unit =
    val sym = fromOkxIndex(d.instId).getOrElse(throw IllegalStateException(s"Unknown OKX index instId: '${d.instId}'"))
    val ts = d.ts.toLong
    bus.publish(IncomeEvent.at(ts, EventData.IndexPriceUpdate(IndexPrice(Exchange.Okx, sym, d.idxPx.asDouble, ts))))

  private def publishFunding(d: FundingRateData): Unit =
    val ts = nowMs
    val fr = FundingRate(
      exchange = Exchange.Okx,
      symbol = requireSymbol(d.instId),
      rate = d.fundingRate.asDouble,
      nextSettleTime = d.fundingTime.toLongOption.getOrElse(0L),
      timestamp = ts,
    )
    bus.publish(IncomeEvent.at(ts, EventData.FundingRateUpdate(fr)))
