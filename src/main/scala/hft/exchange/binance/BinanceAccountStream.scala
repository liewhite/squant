package hft.exchange.binance

import com.github.plokhotnyuk.jsoniter_scala.core.*
import hft.domain.*
import hft.exchange.{AccountStream, WsLoop}
import hft.event.{Event, EventBus, Topics}
import org.slf4j.LoggerFactory
import ox.{Ox, fork}
import ox.channels.Channel
import sttp.client4.WebSocketSyncBackend
import sttp.ws.WebSocketFrame

import BinanceCodec.*
import BinanceCodec.given

/** Binance USDⓈ-M 合约私有账户连接器 (user data stream)。
  *
  * 连接 `/private/ws/<listenKey>`，解析：
  *   - ORDER_TRADE_UPDATE -> OrderUpdate / Fill
  *   - ACCOUNT_UPDATE -> Balance / Position
  *
  * 维护 listenKey 续期 (Binance 要求每 60 分钟内至少一次)。
  *
  * Fail-fast：连接断开、消息解析失败、未知事件类型/订单状态、listenKey 续期失败，
  * 一律抛出异常终止引擎作用域——不重连、不丢弃，避免任何静默的状态发散。
  */
final class BinanceAccountStream(
    client: BinanceClient,
    backend: WebSocketSyncBackend,
    wsBaseUrl: String = BinanceClient.WsBaseUrl,
) extends AccountStream:
  require(client.hasCredentials, "BinanceAccountStream requires credentials")
  private val logger = LoggerFactory.getLogger(classOf[BinanceAccountStream])

  override def exchange: Exchange = Exchange.Binance

  private val outgoing = Channel.unlimited[WebSocketFrame]
  private var bus: EventBus = scala.compiletime.uninitialized

  override def start(eventBus: EventBus)(using Ox): Unit =
    bus = eventBus
    WsLoop.run("binance/private", backend, privateStreamUrl, outgoing, onPrivateText)
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

  /** 连接私有流前获取 listenKey；失败抛异常终止 */
  private def privateStreamUrl(): String =
    client.createListenKey() match
      case Right(key) => s"$wsBaseUrl/private/ws/$key"
      case Left(e)    => throw IllegalStateException(s"Failed to create listenKey: ${e.message}")

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
      account = AccountId.Live,
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
    bus.publish(Event.at(Topics.OrderUpdate, update, msg.E))
    // 本次有成交 -> 同步发布 Fill 事件，乐观更新仓位
    if o.l.asDouble > 0 then
      val fill = Fill(AccountId.Live, Exchange.Binance, o.s, side, price = o.L.asDouble, size = o.l.asDouble, timestamp = o.T)
      bus.publish(Event.at(Topics.Fill, fill, msg.E))

  private def publishAccountUpdate(msg: AccountUpdateMsg): Unit =
    msg.a.B.foreach { b =>
      bus.publish(
        Event.at(Topics.Balance, Balance(AccountId.Live, Exchange.Binance, b.a, b.wb.asDouble, msg.E), msg.E)
      )
    }
    msg.a.P.filter(_.ps == "BOTH").foreach { p =>
      val position = Position(AccountId.Live, Exchange.Binance, p.s, p.pa.asDouble, p.ep.asDouble, p.up.asDouble)
      bus.publish(Event.at(Topics.Position, position, msg.E))
    }
