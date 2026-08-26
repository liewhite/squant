package hft.exchange.bybit

import com.github.plokhotnyuk.jsoniter_scala.core.*
import hft.domain.*
import hft.exchange.{AccountFeed, AccountReport, WsLoop}
import org.slf4j.LoggerFactory
import ox.channels.Channel
import sttp.client4.WebSocketSyncBackend
import sttp.ws.WebSocketFrame

import java.time.Instant
import scala.collection.mutable

import BybitCodec.*
import BybitCodec.given

object BybitAccountFeed:
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
  *   - order     -> OrderUpdate (仅挂单状态追踪；本频道不带单笔成交增量)
  *   - wallet    -> AccountInfo(净值) + 各币种 Balance
  *
  * Fail-fast：连接断开、解析失败、auth/订阅失败一律抛异常终止引擎作用域。
  */
final class BybitAccountFeed(
    client: BybitClient,
    backend: WebSocketSyncBackend,
    wsUrl: String = BybitClient.WsPrivateUrl,
) extends AccountFeed:
  private val logger = LoggerFactory.getLogger(classOf[BybitAccountFeed])
  private val credentials = client.wsCredentials

  override def exchange: Exchange = Exchange.Bybit

  private val outgoing = Channel.unlimited[WebSocketFrame]

  /** 解析结果的去处，由柜台在 [[connect]] 时注入，之后只读。
    * 本流不知道账户是谁 —— 那是柜台盖的章 */
  @volatile private var report: AccountReport => Unit = scala.compiletime.uninitialized
  @volatile private var spawnThread: (=> Unit) => Unit = scala.compiletime.uninitialized

  /** 每张订单的累计成交量。
    *
    * Bybit 的 execution 频道只给**本次**成交量，而柜台的记账依据是累计量 (那样才对重复推送
    * 与跨频道乱序免疫)，所以在这里把它累起来。只有 WS 接收线程一个访问者。
    *
    * 断线不重连、进程重启走启动对齐，因此这份累计不会跨越断线残留成错值。
    */
  private val executedByOrder = mutable.Map.empty[String, Double]

  override def connect(sink: AccountReport => Unit, spawn: (=> Unit) => Unit): Unit =
    report = sink
    spawnThread = spawn
    WsLoop.run("bybit/private", backend, () => wsUrl, outgoing, onPrivateText, spawn)
    // auth 帧入队，连接建立后立即发送 (expires 在此刻生成，留 10s 窗口)
    outgoing.send(WebSocketFrame.text(authFrame()))
    startHeartbeat()

  private def authFrame(): String =
    val expires = Instant.now().toEpochMilli + BybitAccountFeed.AuthExpiresBufferMs
    val sign = credentials.signWsAuth(expires)
    s"""{"op":"auth","args":["${credentials.apiKey}",$expires,"$sign"]}"""

  private val subscribeFrame =
    """{"op":"subscribe","args":["execution","order","wallet","position"]}"""

  // ==================== 私有流解析 (任何不理解的消息 -> 异常上抛终止) ====================

  private def onPrivateText(text: String): Unit =
    val msg = readFromString[BybitWsMsg](text)
    if msg.op.nonEmpty then handleControl(msg, text)
    else
      msg.topic match
        case "execution" => readFromString[WsList[ExecutionData]](text).data.foreach(publishExecution)
        case "order"     => readFromString[WsList[OrderData]](text).data.foreach(publishOrder)
        case "wallet"    => readFromString[WsList[WalletData]](text).data.foreach(publishWallet)
        case "position"  => readFromString[WsList[PositionData]](text).data.foreach(publishPosition)
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

  /** 单笔成交 -> 累计成交量。本频道只给本次量，累计在这里攒 */
  private def publishExecution(d: ExecutionData): Unit =
    val sym = fromBybit(d.symbol).getOrElse(throw IllegalStateException(s"Unknown Bybit symbol in execution: '${d.symbol}'"))
    val cumulative = executedByOrder.updateWith(d.orderId)(prev => Some(prev.getOrElse(0.0) + d.execQty.asDouble)).get
    report(AccountReport.Executed(
      orderId = d.orderId,
      symbol = sym,
      side = sideFromBybit(d.side),
      price = d.execPrice.asPrice,
      cumulativeQty = Coin(cumulative),
      timestamp = d.execTime.toLongOption.getOrElse(nowMs),
    ))

  /** 订单状态。本频道带**累计**成交量 —— 柜台拿它补记账 (execution 先到时增量为零)，
    * 于是两条频道谁先到都不影响账本。 */
  private def publishOrder(d: OrderData): Unit =
    val sym = fromBybit(d.symbol).getOrElse(throw IllegalStateException(s"Unknown Bybit symbol in order: '${d.symbol}'"))
    val filled = Coin(d.cumExecQty.asDouble)
    val status = mapOrderStatus(d.orderStatus, filled)
    // 终态之后不会再有新成交, 清掉本地累加器。**晚到的那条 execution 由柜台兜住**:
    // 它的记账进度在终态后还留一分钟墓碑, 认得出"这笔已经记过了" (见 RestTradingGateway.settled)。
    if status.isTerminal then executedByOrder.remove(d.orderId)
    report(AccountReport.OrderStatusChanged(
      orderId = d.orderId,
      clientOrderId = if d.orderLinkId.nonEmpty then Some(d.orderLinkId) else None,
      symbol = sym,
      side = sideFromBybit(d.side),
      status = status,
      price = Price(d.price.asDoubleOrZero),
      quantity = Coin(d.qty.asDouble),
      filledQuantity = filled,
      timestamp = nowMs,
    ))

  /** 交易所报的仓位 -> 交给柜台对账。size 是绝对值, 方向在 side 里 */
  private def publishPosition(d: PositionData): Unit =
    val sym = fromBybit(d.symbol).getOrElse(throw IllegalStateException(s"Unknown Bybit symbol in position: '${d.symbol}'"))
    val magnitude = d.size.asDoubleOrZero
    val signed = d.side match
      case "Sell" => -magnitude
      case _      => magnitude // Buy, 或空仓时的 "None"
    report(AccountReport.PositionReported(sym, Coin(signed), nowMs))

  /** 钱包快照 -> 账户净值 + 各币种现金余额 */
  private def publishWallet(d: WalletData): Unit =
    val ts = nowMs
    report(AccountReport.EquityChanged(d.totalEquity.asDouble, notional = 0.0, ts))
    d.coin.foreach(c => report(AccountReport.BalanceChanged(c.coin, c.walletBalance.asDoubleOrZero, ts)))

  /** 心跳发送线程：定期入队 ping 帧，维持私有连接 (无成交时也不致空闲被断) */
  private def startHeartbeat(): Unit =
    spawnThread {
      while true do
        Thread.sleep(BybitMarketFeed.HeartbeatIntervalMs)
        outgoing.send(WebSocketFrame.text("""{"op":"ping"}"""))
    }
    ()
