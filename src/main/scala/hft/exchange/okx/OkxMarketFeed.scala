package hft.exchange.okx

import com.github.plokhotnyuk.jsoniter_scala.core.*
import hft.domain.*
import hft.exchange.{MarketFeed, WsLoop}
import hft.event.{Event, Topics}
import org.slf4j.LoggerFactory
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
final class OkxMarketFeed(
    client: OkxClient,
    backend: WebSocketSyncBackend,
    wsUrl: String = OkxClient.WsPublicUrl,
) extends MarketFeed:
  private val logger = LoggerFactory.getLogger(classOf[OkxMarketFeed])

  /** 计价币与张<->币换算的 SymbolMeta 均取自 client (instruments 为公共端点，免凭证) */
  private val quote: String = client.quote

  override def exchange: Exchange = Exchange.Okx

  private val outgoing = Channel.unlimited[WebSocketFrame]
  /** symbol -> meta，用于把盘口数量从合约张数换算为币本位 (装上总线时一次性拉取) */
  private var metas: Map[Symbol, SymbolMeta] = Map.empty

  override protected def connect(): Unit =
    metas = client.fetchAllSymbolMetas() match
      case Right(ms) => ms.map(m => m.symbol -> m).toMap
      case Left(e)   => throw IllegalStateException(s"OKX fetch symbol metas failed: ${e.message}")
    WsLoop.run("okx/public", backend, () => wsUrl, outgoing, onPublicText, body => fork(body))

  /** 基类已去重，这里收到的都是尚未订阅过的流 */
  override protected def subscribeToExchange(kinds: Set[SubscriptionKind]): Unit =
    val args = kinds.map(argJson).mkString(",")
    outgoing.send(WebSocketFrame.text(s"""{"op":"subscribe","args":[$args]}"""))

  private def argJson(kind: SubscriptionKind): String = kind match
    case SubscriptionKind.BBO(s)         => s"""{"channel":"bbo-tbt","instId":"${toOkx(s, quote)}"}"""
    case SubscriptionKind.FundingRate(s) => s"""{"channel":"funding-rate","instId":"${toOkx(s, quote)}"}"""
    case SubscriptionKind.MarkPrice(s)   => s"""{"channel":"mark-price","instId":"${toOkx(s, quote)}"}"""
    case SubscriptionKind.IndexPrice(s)  => s"""{"channel":"index-tickers","instId":"${toOkxIndex(s, quote)}"}"""
    case SubscriptionKind.Trade(s)       => s"""{"channel":"trades","instId":"${toOkx(s, quote)}"}"""

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
        case "trades"        => readFromString[WsPush[TradeData]](text).data.foreach(publishTrade)
        case other          => throw IllegalStateException(s"Unexpected OKX public channel '$other': $text")

  private def handleControl(env: OkxEnvelope, text: String): Unit = env.event match
    case "subscribe" | "unsubscribe" | "channel-conn-count" => ()
    case "error" => throw IllegalStateException(s"OKX public WS error: code=${env.code} msg=${env.msg}")
    case other   => logger.warn(s"ignoring OKX public event '$other': $text")

  private def requireSymbol(instId: String): Symbol =
    fromOkx(instId).getOrElse(throw IllegalStateException(s"Unknown OKX instId: '$instId'"))

  // OKX bbo-tbt 盘口数量单位为合约张数，统一换算为币本位 (策略层永远看币本位，与 Binance BBO 一致)
  private def publishBbo(instId: String, d: BboData): Unit =
    val sym = requireSymbol(instId)
    val meta = metas.getOrElse(sym, throw IllegalStateException(s"No SymbolMeta for OKX bbo symbol: $sym"))
    val ask = d.asks.headOption.getOrElse(throw IllegalStateException(s"OKX bbo empty asks: $instId"))
    val bid = d.bids.headOption.getOrElse(throw IllegalStateException(s"OKX bbo empty bids: $instId"))
    val ts = d.ts.toLong
    val bbo = BBO(
      exchange = Exchange.Okx,
      symbol = sym,
      bidPrice = bid.head.asPrice,
      bidQty = meta.toCoin(Contracts(bid(1).asDouble)),
      askPrice = ask.head.asPrice,
      askQty = meta.toCoin(Contracts(ask(1).asDouble)),
      timestamp = ts,
    )
    publish(Event.at(Topics.Bbo, bbo, ts))

  private def publishMark(d: MarkPriceData): Unit =
    val ts = d.ts.toLong
    publish(Event.at(Topics.MarkPrice, MarkPrice(Exchange.Okx, requireSymbol(d.instId), d.markPx.asPrice, ts), ts))

  private def publishIndex(d: IndexTickerData): Unit =
    val sym = fromOkxIndex(d.instId).getOrElse(throw IllegalStateException(s"Unknown OKX index instId: '${d.instId}'"))
    val ts = d.ts.toLong
    publish(Event.at(Topics.IndexPrice, IndexPrice(Exchange.Okx, sym, d.idxPx.asPrice, ts), ts))

  private def publishTrade(d: TradeData): Unit =
    val ts = d.ts.toLong
    // OKX side = taker 方向: side=sell -> 买方是挂单方 (isBuyerMaker=true)
    val sym = requireSymbol(d.instId)
    val trade = MarketTrade(Exchange.Okx, sym, d.px.asPrice, metas.getOrElse(sym, throw IllegalStateException(s"No SymbolMeta for OKX trade symbol: $sym")).toCoin(Contracts(d.sz.asDouble)), d.side == "sell", ts)
    publish(Event.at(Topics.Trade, trade, ts))

  private def publishFunding(d: FundingRateData): Unit =
    val ts = nowMs
    val fr = FundingRate(
      exchange = Exchange.Okx,
      symbol = requireSymbol(d.instId),
      rate = d.fundingRate.asDouble,
      nextSettleTime = d.fundingTime.toLongOption.getOrElse(0L),
      timestamp = ts,
    )
    publish(Event.at(Topics.FundingRate, fr, ts))
