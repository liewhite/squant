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

import BinanceCodec.*
import BinanceCodec.given

/** Binance USDⓈ-M 合约 WebSocket 连接器。
  *
  * Binance 按路由端点拆分推送，连接器为每个用到的路由维护一条独立连接：
  *   - `/public/ws` 高频公共数据: bookTicker -> BBO
  *   - `/market/ws` 常规行情: markPrice\@1s -> MarkPrice + IndexPrice + FundingRate
  *   - `/private/ws/<listenKey>` 用户数据流 (配置凭证时): ORDER_TRADE_UPDATE ->
  *     OrderUpdate/Fill，ACCOUNT_UPDATE -> Balance/Position
  *
  * Fail-fast：连接断开、消息解析失败、未知事件类型/订单状态、listenKey 续期失败，
  * 一律抛出异常终止引擎作用域——不重连、不丢弃，避免任何静默的状态发散。
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

  /** 每个公共路由一条出站 channel (即一条连接)；建连前入队的订阅帧会在建连后发送 */
  private val outgoing: Map[Route, Channel[WebSocketFrame]] =
    Route.values.map(_ -> Channel.unlimited[WebSocketFrame]).toMap
  private val privateOut = Channel.unlimited[WebSocketFrame]
  private val requestId = AtomicInteger(0)
  private var bus: EventBus[IncomeEvent] = scala.compiletime.uninitialized

  override def start(incomeBus: EventBus[IncomeEvent])(using Ox): Unit =
    bus = incomeBus
    Route.values.foreach { route =>
      WsLoop.run(s"binance${route.path.stripSuffix("/ws")}", backend, () => s"$wsBaseUrl${route.path}", outgoing(route), onPublicText)
    }
    if client.hasCredentials then
      WsLoop.run("binance/private", backend, privateStreamUrl, privateOut, onPrivateText)
      fork {
        while true do
          Thread.sleep(30 * 60 * 1000)
          client.keepAliveListenKey() match
            case Right(()) => logger.info("listenKey keepalive ok")
            case Left(e) =>
              // 续期失败意味着私有流将在 60 分钟内被服务端断开 (推送丢失)，立即终止
              throw IllegalStateException(s"listenKey keepalive failed: ${e.message}")
      }
      ()

  override def subscribe(kinds: Set[SubscriptionKind]): Unit =
    kinds.groupBy(routeOf).foreach { (route, routeKinds) =>
      sendSubscribe(route, routeKinds.map(streamName))
    }

  /** 连接私有流前获取 listenKey；失败抛异常终止 */
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

  // ==================== 公共流解析 (解析失败/未知事件 -> 异常上抛终止) ====================

  private def onPublicText(text: String): Unit =
    readFromString[WsEnvelope](text).e match
      case "bookTicker"      => publishBookTicker(readFromString[BookTickerMsg](text))
      case "markPriceUpdate" => publishMarkPrice(readFromString[MarkPriceMsg](text))
      case ""                => () // SUBSCRIBE ack: {"result":null,"id":N}，确定可忽略
      case other             => throw IllegalStateException(s"Unexpected public event '$other': $text")

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

  // ==================== 私有流解析 (任何不理解的消息 -> 异常上抛终止) ====================

  private def onPrivateText(text: String): Unit =
    readFromString[WsEnvelope](text).e match
      case "ORDER_TRADE_UPDATE" => publishOrderUpdate(readFromString[OrderTradeUpdateMsg](text))
      case "ACCOUNT_UPDATE"     => publishAccountUpdate(readFromString[AccountUpdateMsg](text))
      // 确定可安全忽略的事件:
      // - TRADE_LITE: 成交信息与 ORDER_TRADE_UPDATE 重复推送
      // - ACCOUNT_CONFIG_UPDATE: 杠杆/保证金模式变更，不影响订单与仓位核算
      case "TRADE_LITE" | "ACCOUNT_CONFIG_UPDATE" => ()
      case "listenKeyExpired" =>
        throw IllegalStateException("listenKey expired, private stream about to drop pushes")
      case other => throw IllegalStateException(s"Unhandled private event '$other': $text")

  private def publishOrderUpdate(msg: OrderTradeUpdateMsg): Unit =
    val o = msg.o
    val side = if o.S == "BUY" then Side.Long else Side.Short
    val status = o.X match
      case "NEW"              => OrderStatus.Pending
      case "PARTIALLY_FILLED" => OrderStatus.PartiallyFilled(o.z.asDouble)
      case "FILLED"           => OrderStatus.Filled
      case "CANCELED" | "EXPIRED" | "EXPIRED_IN_MATCH" => OrderStatus.Cancelled
      case "REJECTED"         => OrderStatus.Rejected("rejected by exchange")
      // 未知状态意味着无法解释交易所的订单状态机，继续运行只会静默发散
      case other => throw IllegalStateException(s"Unknown order status '$other': $o")
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
