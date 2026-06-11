package hft.exchange.binance

import com.github.plokhotnyuk.jsoniter_scala.core.*
import hft.domain.*
import hft.exchange.{ExchangeConnector, SubscriptionKind, WsLoop}
import hft.messaging.{EventBus, EventData, IncomeEvent}
import org.slf4j.LoggerFactory
import ox.{Ox, fork}
import ox.channels.Channel
import sttp.client4.WebSocketSyncBackend
import sttp.ws.WebSocketFrame

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable

import BinanceCodec.*
import BinanceCodec.given

/** Binance USDⓈ-M 合约 WebSocket 连接器。
  *
  * Binance 按路由端点拆分推送，连接器为每个用到的路由维护一条独立连接：
  *   - `/public/ws` 高频公共数据: bookTicker -> BBO
  *   - `/market/ws` 常规行情: markPrice\@1s -> MarkPrice + IndexPrice + FundingRate
  *   - `/private/ws/<listenKey>` 用户数据流 (配置凭证时): ORDER_TRADE_UPDATE ->
  *     OrderUpdate/Fill，ACCOUNT_UPDATE -> Balance/Position；listenKey 每 30 分钟
  *     自动续期，重连时重新获取
  */
final class BinanceConnector(
    client: BinanceClient,
    backend: WebSocketSyncBackend,
    wsBaseUrl: String = BinanceClient.WsBaseUrl,
) extends ExchangeConnector:
  private val logger = LoggerFactory.getLogger(classOf[BinanceConnector])

  override def exchange: Exchange = Exchange.Binance

  /** 公共流路由端点 */
  private enum Route(val path: String):
    case Public extends Route("/public/ws") // 高频公共市场数据
    case Market extends Route("/market/ws") // 常规行情数据

  /** 每个公共路由一条出站 channel (即一条连接) */
  private val outgoing: Map[Route, Channel[WebSocketFrame]] =
    Route.values.map(_ -> Channel.unlimited[WebSocketFrame]).toMap
  private val privateOut = Channel.unlimited[WebSocketFrame]
  /** 已订阅集合，synchronized 保护 (subscribe 与重连恢复可能并发) */
  private val subscribed = mutable.Set.empty[SubscriptionKind]
  private val requestId = AtomicInteger(0)
  private var bus: EventBus[IncomeEvent] = scala.compiletime.uninitialized

  override def start(incomeBus: EventBus[IncomeEvent])(using Ox): Unit =
    bus = incomeBus
    Route.values.foreach { route =>
      WsLoop.run(
        s"binance${route.path.stripSuffix("/ws")}",
        backend,
        () => s"$wsBaseUrl${route.path}",
        outgoing(route),
        onPublicText,
        () => restoreSubscriptions(route),
      )
    }
    if client.hasCredentials then
      WsLoop.run("binance/private", backend, privateStreamUrl, privateOut, onPrivateText, () => ())
      fork {
        while true do
          Thread.sleep(30 * 60 * 1000)
          client.keepAliveListenKey().left.foreach(e => logger.warn(s"listenKey keepalive failed: ${e.message}"))
      }
      ()

  override def subscribe(kinds: Set[SubscriptionKind]): Unit = synchronized {
    subscribed ++= kinds
    kinds.groupBy(routeOf).foreach { (route, routeKinds) =>
      sendSubscribe(route, routeKinds.map(streamName))
    }
  }

  /** 重连后恢复该路由的全部订阅 */
  private def restoreSubscriptions(route: Route): Unit = synchronized {
    val streams = subscribed.toSet.filter(routeOf(_) == route).map(streamName)
    sendSubscribe(route, streams)
  }

  /** 每次 (重) 连接私有流前重新获取 listenKey；失败抛异常由 WsLoop 退避重试 */
  private def privateStreamUrl(): String =
    client.createListenKey() match
      case Right(key) => s"$wsBaseUrl/private/ws/$key"
      case Left(e)    => throw IllegalStateException(s"Failed to create listenKey: ${e.message}")

  private def routeOf(kind: SubscriptionKind): Route = kind match
    case _: SubscriptionKind.BBO => Route.Public
    case _                       => Route.Market

  private def streamName(kind: SubscriptionKind): String =
    val symbol = kind.subscribedSymbol.toLowerCase
    kind match
      case _: SubscriptionKind.BBO => s"$symbol@bookTicker"
      // FundingRate / MarkPrice / IndexPrice 共用 markPrice 流
      case _ => s"$symbol@markPrice@1s"

  private def sendSubscribe(route: Route, streams: Set[String]): Unit =
    if streams.nonEmpty then
      val params = streams.map(s => s"\"$s\"").mkString(",")
      val message = s"""{"method":"SUBSCRIBE","params":[$params],"id":${requestId.incrementAndGet()}}"""
      logger.info(s"subscribing on ${route.path}: $streams")
      outgoing(route).send(WebSocketFrame.text(message))

  // ==================== 公共流解析 ====================

  private def onPublicText(text: String): Unit =
    try
      readFromString[WsEnvelope](text).e match
        case "bookTicker"      => publishBookTicker(readFromString[BookTickerMsg](text))
        case "markPriceUpdate" => publishMarkPrice(readFromString[MarkPriceMsg](text))
        case _                 => () // 订阅 ack 等控制消息
    catch
      case e: JsonReaderException => logger.warn(s"Failed to parse public message: ${e.getMessage}; text=$text")

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
    bus.publish(IncomeEvent.at(msg.E, EventData.BboUpdate(bbo)))

  /** markPrice 流一次携带标记价格/指数价格/资金费率，拆为三个事件发布 */
  private def publishMarkPrice(msg: MarkPriceMsg): Unit =
    bus.publish(
      IncomeEvent.at(msg.E, EventData.MarkPriceUpdate(MarkPrice(Exchange.Binance, msg.s, msg.p.asDouble, msg.E)))
    )
    bus.publish(
      IncomeEvent.at(msg.E, EventData.IndexPriceUpdate(IndexPrice(Exchange.Binance, msg.s, msg.i.asDouble, msg.E)))
    )
    bus.publish(
      IncomeEvent.at(
        msg.E,
        EventData.FundingRateUpdate(
          FundingRate(Exchange.Binance, msg.s, msg.r.asDouble, nextSettleTime = msg.T, timestamp = msg.E)
        ),
      )
    )

  // ==================== 私有流解析 ====================

  private def onPrivateText(text: String): Unit =
    try
      readFromString[WsEnvelope](text).e match
        case "ORDER_TRADE_UPDATE" => publishOrderUpdate(readFromString[OrderTradeUpdateMsg](text))
        case "ACCOUNT_UPDATE"     => publishAccountUpdate(readFromString[AccountUpdateMsg](text))
        case "listenKeyExpired" =>
          // 抛出异常令接收循环退出，WsLoop 重连时会重新获取 listenKey
          throw IllegalStateException("listenKey expired")
        case _ => ()
    catch
      case e: JsonReaderException => logger.warn(s"Failed to parse private message: ${e.getMessage}; text=$text")

  private def publishOrderUpdate(msg: OrderTradeUpdateMsg): Unit =
    val o = msg.o
    val side = if o.S == "BUY" then Side.Long else Side.Short
    val status = o.X match
      case "NEW"              => OrderStatus.Pending
      case "PARTIALLY_FILLED" => OrderStatus.PartiallyFilled(o.z.asDouble)
      case "FILLED"           => OrderStatus.Filled
      case "CANCELED" | "EXPIRED" => OrderStatus.Cancelled
      case "REJECTED"         => OrderStatus.Rejected("rejected by exchange")
      case other              => OrderStatus.Error(s"unknown status: $other")
    val update = OrderUpdate(
      orderId = o.i.toString,
      clientOrderId = Some(o.c),
      exchange = Exchange.Binance,
      symbol = o.s,
      side = side,
      status = status,
      price = o.p.asDouble,
      quantity = o.q.asDouble,
      filledQuantity = o.z.asDouble,
      fillSize = o.l.asDouble,
      timestamp = o.T,
    )
    bus.publish(IncomeEvent.at(msg.E, EventData.OrderUpdated(update)))
    // 本次有成交 -> 同步发布 Fill 事件，乐观更新仓位
    if o.l.asDouble > 0 then
      val fill = Fill(Exchange.Binance, o.s, side, price = o.L.asDouble, size = o.l.asDouble, timestamp = o.T)
      bus.publish(IncomeEvent.at(msg.E, EventData.FillUpdate(fill)))

  private def publishAccountUpdate(msg: AccountUpdateMsg): Unit =
    msg.a.B.foreach { b =>
      bus.publish(
        IncomeEvent.at(msg.E, EventData.BalanceUpdate(Balance(Exchange.Binance, b.a, b.wb.asDouble, msg.E)))
      )
    }
    msg.a.P.filter(_.ps == "BOTH").foreach { p =>
      val position = Position(Exchange.Binance, p.s, p.pa.asDouble, p.ep.asDouble, p.up.asDouble)
      bus.publish(IncomeEvent.at(msg.E, EventData.PositionUpdate(position)))
    }
