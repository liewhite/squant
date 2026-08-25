package hft.exchange.bybit

import com.github.plokhotnyuk.jsoniter_scala.core.*
import hft.domain.*
import hft.exchange.{AccountStream, WsLoop}
import hft.event.{Event, EventBus, Topics}
import org.slf4j.LoggerFactory
import ox.{Ox, fork}
import ox.channels.Channel
import sttp.client4.WebSocketSyncBackend
import sttp.ws.WebSocketFrame

import java.time.Instant

import BybitCodec.*
import BybitCodec.given

object BybitAccountStream:
  /** auth 帧 expires 相对当前时间的前移量 (ms)，给握手留出窗口 */
  val AuthExpiresBufferMs: Long = 10_000

/** Bybit v5 USDT linear 永续私有账户连接器。
  *
  * 连接 `/v5/private`，带内握手 (WsLoop 无建连钩子，故事件驱动)：
  *   1. start 时把 auth 帧入队 (连上即发)
  *   2. 收到 auth 成功 (op=auth,success=true) 后，入队订阅 execution/order/wallet
  *
  * 解析 (linear 数量即币本位，无张<->币换算)：
  *   - execution -> Fill (本次成交，维护仓位)。Bybit order 频道只带累计成交 cumExecQty 不带单笔增量，
  *     故仓位维护交由 execution 频道，与 order 频道职责分离，避免重复计数
  *   - order     -> OrderUpdate (挂单状态追踪；fillSize=0，仓位不在此维护)
  *   - wallet    -> AccountInfo(净值) + 各币种 Balance
  *
  * Fail-fast：连接断开、解析失败、auth/订阅失败一律抛异常终止引擎作用域。
  */
final class BybitAccountStream(
    client: BybitClient,
    backend: WebSocketSyncBackend,
    wsUrl: String = BybitClient.WsPrivateUrl,
) extends AccountStream:
  private val logger = LoggerFactory.getLogger(classOf[BybitAccountStream])
  private val credentials = client.wsCredentials

  override def exchange: Exchange = Exchange.Bybit

  private val outgoing = Channel.unlimited[WebSocketFrame]
  private var bus: EventBus = scala.compiletime.uninitialized

  override def start(eventBus: EventBus)(using Ox): Unit =
    bus = eventBus
    WsLoop.run("bybit/private", backend, () => wsUrl, outgoing, onPrivateText)
    // auth 帧入队，连接建立后立即发送 (expires 在此刻生成，留 10s 窗口)
    outgoing.send(WebSocketFrame.text(authFrame()))
    startHeartbeat()

  private def authFrame(): String =
    val expires = Instant.now().toEpochMilli + BybitAccountStream.AuthExpiresBufferMs
    val sign = credentials.signWsAuth(expires)
    s"""{"op":"auth","args":["${credentials.apiKey}",$expires,"$sign"]}"""

  private val subscribeFrame =
    """{"op":"subscribe","args":["execution","order","wallet"]}"""

  // ==================== 私有流解析 (任何不理解的消息 -> 异常上抛终止) ====================

  private def onPrivateText(text: String): Unit =
    val msg = readFromString[BybitWsMsg](text)
    if msg.op.nonEmpty then handleControl(msg, text)
    else
      msg.topic match
        case "execution" => readFromString[WsList[ExecutionData]](text).data.foreach(publishExecution)
        case "order"     => readFromString[WsList[OrderData]](text).data.foreach(publishOrder)
        case "wallet"    => readFromString[WsList[WalletData]](text).data.foreach(publishWallet)
        case other       => throw IllegalStateException(s"Unexpected Bybit private topic '$other': $text")

  private def handleControl(msg: BybitWsMsg, text: String): Unit = msg.op match
    case "auth" =>
      if msg.success then
        logger.info("Bybit private auth success, subscribing private channels")
        outgoing.send(WebSocketFrame.text(subscribeFrame))
      else throw IllegalStateException(s"Bybit auth failed: ${msg.ret_msg}")
    case "subscribe" =>
      if !msg.success then throw IllegalStateException(s"Bybit private subscribe failed: ${msg.ret_msg}")
    case "ping" | "pong" => () // 心跳响应
    case other           => logger.warn(s"ignoring Bybit private op '$other': $text")

  /** 单笔成交 -> Fill，即时维护仓位 (无论策略单还是手动单) */
  private def publishExecution(d: ExecutionData): Unit =
    val sym = fromBybit(d.symbol).getOrElse(throw IllegalStateException(s"Unknown Bybit symbol in execution: '${d.symbol}'"))
    val fill = Fill(
      account = AccountId.Live,
      exchange = Exchange.Bybit,
      symbol = sym,
      side = sideFromBybit(d.side),
      price = d.execPrice.asPrice,
      size = Coin(d.execQty.asDouble),
      timestamp = d.execTime.toLongOption.getOrElse(nowMs),
    )
    bus.publish(Event.local(Topics.Fill, fill))

  /** 订单状态 -> OrderUpdate，仅追踪挂单生命周期；fillSize=0，仓位由 execution 维护 */
  private def publishOrder(d: OrderData): Unit =
    val sym = fromBybit(d.symbol).getOrElse(throw IllegalStateException(s"Unknown Bybit symbol in order: '${d.symbol}'"))
    val filled = Coin(d.cumExecQty.asDouble)
    val update = OrderUpdate(
      account = AccountId.Live,
      orderId = d.orderId,
      clientOrderId = if d.orderLinkId.nonEmpty then Some(d.orderLinkId) else None,
      exchange = Exchange.Bybit,
      symbol = sym,
      side = sideFromBybit(d.side),
      status = mapOrderStatus(d.orderStatus, filled),
      price = Price(d.price.asDoubleOrZero),
      quantity = Coin(d.qty.asDouble),
      filledQuantity = filled,
      fillSize = Coin.Zero,
      timestamp = nowMs,
    )
    bus.publish(Event.local(Topics.OrderUpdate, update))

  /** 钱包快照 -> 账户净值 + 各币种现金余额 */
  private def publishWallet(d: WalletData): Unit =
    val ts = nowMs
    bus.publish(
      Event.at(Topics.AccountInfo, AccountInfo(AccountId.Live, Exchange.Bybit, d.totalEquity.asDouble, notional = 0.0), ts)
    )
    d.coin.foreach { c =>
      bus.publish(Event.at(Topics.Balance, Balance(AccountId.Live, Exchange.Bybit, c.coin, c.walletBalance.asDoubleOrZero, ts), ts))
    }

  /** 心跳发送线程：定期入队 ping 帧，维持私有连接 (无成交时也不致空闲被断) */
  private def startHeartbeat()(using Ox): Unit =
    fork {
      while true do
        Thread.sleep(BybitMarketStream.HeartbeatIntervalMs)
        outgoing.send(WebSocketFrame.text("""{"op":"ping"}"""))
    }
    ()
