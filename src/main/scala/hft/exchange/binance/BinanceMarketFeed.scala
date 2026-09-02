package hft.exchange.binance

import com.github.plokhotnyuk.jsoniter_scala.core.*
import hft.domain.*
import hft.exchange.{MarketFeed, WsLoop}
import hft.event.{Event, Topics}
import org.slf4j.LoggerFactory
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
final class BinanceMarketFeed(
    backend: WebSocketSyncBackend,
    wsBaseUrl: String = BinanceClient.WsBaseUrl,
) extends MarketFeed:
  private val logger = LoggerFactory.getLogger(classOf[BinanceMarketFeed])

  override def exchange: Exchange = Exchange.Binance

  /** 公共流路由端点 */
  private enum Route(val path: String):
    case Public extends Route("/public/ws") // 高频公共市场数据
    case Market extends Route("/market/ws") // 常规行情数据

  /** 每个路由一条出站 channel (即一条连接)；建连前入队的订阅帧会在建连后发送 */
  private val outgoing: Map[Route, Channel[WebSocketFrame]] =
    Route.values.map(_ -> Channel.unlimited[WebSocketFrame]).toMap
  private val requestId = AtomicInteger(0)

  override protected def connect(): Unit =
    Route.values.foreach { route =>
      WsLoop.run(
        s"binance${route.path.stripSuffix("/ws")}", backend, () => s"$wsBaseUrl${route.path}",
        outgoing(route), onPublicText, body => fork(body),
      )
    }

  /** 基类已去重，这里收到的都是尚未订阅过的流 */
  override protected def subscribeToExchange(kinds: Set[SubscriptionKind]): Unit =
    kinds.groupBy(routeOf).foreach { (route, routeKinds) =>
      sendSubscribe(route, routeKinds.map(streamName))
    }

  // 穷举而不用 `case _ =>` 兜底：新增一种 SubscriptionKind 时，这里要编译报错逼人来教它
  // 该走哪条连接。兜底会把没教过的流悄悄塞进 Market 路由，订上一条不相干的流还不报错。
  private def routeOf(kind: SubscriptionKind): Route = kind match
    case _: SubscriptionKind.BBO | _: SubscriptionKind.Trade                                     => Route.Public // 高频流
    case _: SubscriptionKind.MarkPrice | _: SubscriptionKind.IndexPrice | _: SubscriptionKind.FundingRate => Route.Market

  private def streamName(kind: SubscriptionKind): String =
    val symbol = kind.subscribedSymbol.toLowerCase
    kind match
      case _: SubscriptionKind.BBO   => s"$symbol@bookTicker"
      case _: SubscriptionKind.Trade => s"$symbol@aggTrade"
      // 三者共用 markPrice 流 (一条消息拆三个事件), 但仍逐个写出 —— 见 routeOf 的说明
      case _: SubscriptionKind.MarkPrice | _: SubscriptionKind.IndexPrice | _: SubscriptionKind.FundingRate =>
        s"$symbol@markPrice@1s"

  /** 单条 SUBSCRIBE 消息最多带多少个流名。
    *
    * 实测 (2026-08, USDⓈ-M)：200 个流一条消息正常受理；527 个 (全部 USDT 永续) 塞进一条会被
    * 服务端以 `1008 policy violation: Payload too long` **直接断开连接**。分批发则单连接可持有
    * 全部 527 条 —— 限制在**单条消息体积**，不是单连接的流数量，故无需拆连接。取 100 留足余量。
    */
  private val MaxStreamsPerMessage = 100

  private def sendSubscribe(route: Route, streams: Set[String]): Unit =
    if streams.nonEmpty then
      // 币安对入站消息限频 10 条/秒。全市场 527 个 aggTrade 分 6 条, 在限内;
      // 若某次订阅的流数超过 1000 (即 >10 条消息), 得在发送侧限速, 故此处告警而不是静默发。
      val chunks = streams.grouped(MaxStreamsPerMessage).toVector
      if chunks.sizeIs > 10 then
        logger.warn(
          s"subscribing ${streams.size} streams in ${chunks.size} messages on ${route.path}; " +
            "币安入站限 10 条/秒, 可能被限频断开"
        )
      logger.info(s"subscribing ${streams.size} streams on ${route.path} in ${chunks.size} message(s)")
      chunks.foreach { chunk =>
        val params = chunk.map(s => s"\"$s\"").mkString(",")
        val message = s"""{"method":"SUBSCRIBE","params":[$params],"id":${requestId.incrementAndGet()}}"""
        outgoing(route).send(WebSocketFrame.text(message))
      }

  // ==================== 公共流解析 (解析失败/未知事件 -> 异常上抛终止) ====================

  private def onPublicText(text: String): Unit =
    val envelope = readFromString[WsEnvelope](text)
    envelope.e match
      case "bookTicker"      => publishBookTicker(readFromString[BookTickerMsg](text))
      case "markPriceUpdate" => publishMarkPrice(readFromString[MarkPriceMsg](text))
      case "aggTrade"        => publishTrade(readFromString[AggTradeMsg](text))
      // 没有事件类型 `e` 的帧是请求应答。成功是 {"result":null,"id":N}，失败是
      // {"id":N,"error":{"code":..,"msg":..}} —— 两者都没有 `e`。从前一律当成 ack 忽略，
      // 于是一次非法订阅被静默吞掉: 订阅没生效、行情永不到达、没有任何症状。
      case "" =>
        envelope.error.foreach(err =>
          throw IllegalStateException(s"Binance 公共流请求失败: code=${err.code} msg=${err.msg} (id=${envelope.id}): $text")
        )
      case other => throw IllegalStateException(s"Unexpected public event '$other': $text")

  private def publishTrade(msg: AggTradeMsg): Unit =
    val trade = MarketTrade(Exchange.Binance, msg.s, msg.p.asPrice, Coin(msg.q.asDouble), msg.m, msg.T)
    publish(Event.at(Topics.Trade, trade, msg.T))

  private def publishBookTicker(msg: BookTickerMsg): Unit =
    val bbo = BBO(
      exchange = Exchange.Binance,
      symbol = msg.s,
      bidPrice = msg.b.asPrice,
      bidQty = Coin(msg.B.asDouble),
      askPrice = msg.a.asPrice,
      askQty = Coin(msg.A.asDouble),
      timestamp = msg.E,
    )
    publish(Event.at(Topics.Bbo, bbo, msg.E))

  /** markPrice 流一次携带标记价格/指数价格/资金费率，拆为三个事件发布 */
  private def publishMarkPrice(msg: MarkPriceMsg): Unit =
    publish(
      Event.at(Topics.MarkPrice, MarkPrice(Exchange.Binance, msg.s, msg.p.asPrice, msg.E), msg.E)
    )
    publish(
      Event.at(Topics.IndexPrice, IndexPrice(Exchange.Binance, msg.s, msg.i.asPrice, msg.E), msg.E)
    )
    publish(
      Event.at(Topics.FundingRate, FundingRate(Exchange.Binance, msg.s, msg.r.asDouble, nextSettleTime = msg.T, timestamp = msg.E), msg.E)
    )
