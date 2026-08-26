package hft.exchange.binance

import com.github.plokhotnyuk.jsoniter_scala.core.*
import hft.domain.*
import hft.exchange.{AccountFeed, AccountReport, WsLoop}
import org.slf4j.LoggerFactory
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
final class BinanceAccountFeed(
    client: BinanceClient,
    backend: WebSocketSyncBackend,
    wsBaseUrl: String = BinanceClient.WsBaseUrl,
) extends AccountFeed:
  private val logger = LoggerFactory.getLogger(classOf[BinanceAccountFeed])

  override def exchange: Exchange = Exchange.Binance

  private val outgoing = Channel.unlimited[WebSocketFrame]

  /** 解析结果的去处，由柜台在 [[connect]] 时注入，之后只读。
    * 本流不知道账户是谁 —— 那是柜台盖的章 */
  @volatile private var report: AccountReport => Unit = scala.compiletime.uninitialized

  override def connect(sink: AccountReport => Unit, spawn: (=> Unit) => Unit): Unit =
    report = sink
    WsLoop.run("binance/private", backend, privateStreamUrl, outgoing, onPrivateText, spawn)
    spawn {
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
      case "PARTIALLY_FILLED" => OrderStatus.PartiallyFilled(Coin(o.z.asDouble))
      case "FILLED"           => OrderStatus.Filled
      case "CANCELED" | "EXPIRED" | "EXPIRED_IN_MATCH" => OrderStatus.Cancelled
      case "REJECTED"         => OrderStatus.Rejected("rejected by exchange")
      // 未知状态意味着无法解释交易所的订单状态机，继续运行只会静默发散
      case other => throw IllegalStateException(s"Unknown order status '$other': $o")
    val filledQty = Coin(o.z.asDouble) // 累计成交
    // 成交先报: 柜台据此记账并先发仓位快照, 订单状态随后。同一条推送里两件事,
    // 顺序由这里决定 —— 不必依赖交易所各频道的到达次序。
    if o.l.asDouble > 0 then
      report(AccountReport.Executed(o.i.toString, o.s, side, o.L.asPrice, filledQty, o.T))
    report(AccountReport.OrderStatusChanged(
      orderId = o.i.toString,
      clientOrderId = Some(o.c),
      symbol = o.s,
      side = side,
      status = status,
      price = o.p.asPrice,
      quantity = Coin(o.q.asDouble),
      filledQuantity = filledQty,
      timestamp = o.T,
    ))

  private def publishAccountUpdate(msg: AccountUpdateMsg): Unit =
    msg.a.B.foreach(b => report(AccountReport.BalanceChanged(b.a, b.wb.asDouble, msg.E)))
    // 交易所报的仓位 —— 交给柜台对账, 不进总线。推的是整个账户, 里面有本柜台不管的标的,
    // 由柜台按"已对齐过的标的"过滤 (见 RestTradingGateway.reconcile)。
    msg.a.P.filter(_.ps == "BOTH").foreach { p =>
      report(AccountReport.PositionReported(p.s, Coin(p.pa.asDouble), msg.E))
    }
