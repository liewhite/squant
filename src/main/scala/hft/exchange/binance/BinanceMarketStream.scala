package hft.exchange.binance

import com.github.plokhotnyuk.jsoniter_scala.core.*
import hft.domain.*
import hft.exchange.{MarketDataStream, SubscriptionKind, WsLoop}
import hft.event.{Event, EventBus, Topics}
import org.slf4j.LoggerFactory
import ox.Ox
import ox.channels.Channel
import sttp.client4.WebSocketSyncBackend
import sttp.ws.WebSocketFrame

import java.util.concurrent.atomic.AtomicInteger

import BinanceCodec.*
import BinanceCodec.given

/** Binance USDⓈ-M 合约公共行情连接器。
  *
  * Binance 按路由端点拆分推送，连接器为每个用到的路由维护一条独立连接：
  *   - `/public/ws` 高频公共数据: bookTicker -> BBO
  *   - `/market/ws` 常规行情: markPrice\@1s -> MarkPrice + IndexPrice + FundingRate
  *
  * Fail-fast：连接断开、消息解析失败、未知事件类型，一律抛出异常终止引擎作用域。
  */
final class BinanceMarketStream(
    backend: WebSocketSyncBackend,
    wsBaseUrl: String = BinanceClient.WsBaseUrl,
) extends MarketDataStream:
  private val logger = LoggerFactory.getLogger(classOf[BinanceMarketStream])

  override def exchange: Exchange = Exchange.Binance

  /** 公共流路由端点 */
  private enum Route(val path: String):
    case Public extends Route("/public/ws") // 高频公共市场数据
    case Market extends Route("/market/ws") // 常规行情数据

  /** 每个路由一条出站 channel (即一条连接)；建连前入队的订阅帧会在建连后发送 */
  private val outgoing: Map[Route, Channel[WebSocketFrame]] =
    Route.values.map(_ -> Channel.unlimited[WebSocketFrame]).toMap
  private val requestId = AtomicInteger(0)
  private var bus: EventBus = scala.compiletime.uninitialized

  override def start(eventBus: EventBus)(using Ox): Unit =
    bus = eventBus
    Route.values.foreach { route =>
      WsLoop.run(s"binance${route.path.stripSuffix("/ws")}", backend, () => s"$wsBaseUrl${route.path}", outgoing(route), onPublicText)
    }

  override def subscribe(kinds: Set[SubscriptionKind]): Unit =
    kinds.groupBy(routeOf).foreach { (route, routeKinds) =>
      sendSubscribe(route, routeKinds.map(streamName))
    }

  private def routeOf(kind: SubscriptionKind): Route = kind match
    case _: SubscriptionKind.BBO | _: SubscriptionKind.Trade => Route.Public // 高频流
    case _                                                   => Route.Market

  private def streamName(kind: SubscriptionKind): String =
    val symbol = kind.subscribedSymbol.toLowerCase
    kind match
      case _: SubscriptionKind.BBO   => s"$symbol@bookTicker"
      case _: SubscriptionKind.Trade => s"$symbol@aggTrade"
      // FundingRate / MarkPrice / IndexPrice 共用 markPrice 流
      case _ => s"$symbol@markPrice@1s"

  private def sendSubscribe(route: Route, streams: Set[String]): Unit =
    if streams.nonEmpty then
      val params = streams.map(s => s"\"$s\"").mkString(",")
      val message = s"""{"method":"SUBSCRIBE","params":[$params],"id":${requestId.incrementAndGet()}}"""
      logger.info(s"subscribing on ${route.path}: $streams")
      outgoing(route).send(WebSocketFrame.text(message))

  // ==================== 公共流解析 (解析失败/未知事件 -> 异常上抛终止) ====================

  private def onPublicText(text: String): Unit =
    readFromString[WsEnvelope](text).e match
      case "bookTicker"      => publishBookTicker(readFromString[BookTickerMsg](text))
      case "markPriceUpdate" => publishMarkPrice(readFromString[MarkPriceMsg](text))
      case "aggTrade"        => publishTrade(readFromString[AggTradeMsg](text))
      case ""                => () // SUBSCRIBE ack: {"result":null,"id":N}，确定可忽略
      case other             => throw IllegalStateException(s"Unexpected public event '$other': $text")

  private def publishTrade(msg: AggTradeMsg): Unit =
    val trade = MarketTrade(Exchange.Binance, msg.s, msg.p.asDouble, msg.q.asDouble, msg.m, msg.T)
    bus.publish(Event.at(Topics.Trade, trade, msg.T))

  private def publishBookTicker(msg: BookTickerMsg): Unit =
    val bbo = BBO(
      exchange = Exchange.Binance,
      symbol = msg.s,
      bidPrice = msg.b.asDouble,
      bidQty = msg.B.asDouble,
      askPrice = msg.a.asDouble,
      askQty = msg.A.asDouble,
      timestamp = msg.E,
    )
    bus.publish(Event.at(Topics.Bbo, bbo, msg.E))

  /** markPrice 流一次携带标记价格/指数价格/资金费率，拆为三个事件发布 */
  private def publishMarkPrice(msg: MarkPriceMsg): Unit =
    bus.publish(
      Event.at(Topics.MarkPrice, MarkPrice(Exchange.Binance, msg.s, msg.p.asDouble, msg.E), msg.E)
    )
    bus.publish(
      Event.at(Topics.IndexPrice, IndexPrice(Exchange.Binance, msg.s, msg.i.asDouble, msg.E), msg.E)
    )
    bus.publish(
      Event.at(Topics.FundingRate, FundingRate(Exchange.Binance, msg.s, msg.r.asDouble, nextSettleTime = msg.T, timestamp = msg.E), msg.E)
    )
