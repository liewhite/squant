package hft.exchange.okx

import com.github.plokhotnyuk.jsoniter_scala.core.*
import hft.domain.*
import hft.exchange.{AccountFeed, WsLoop}
import hft.event.{AnyEvent, Event, Topics}
import org.slf4j.LoggerFactory
import ox.channels.Channel
import sttp.client4.WebSocketSyncBackend
import sttp.ws.WebSocketFrame

import java.time.Instant
import scala.collection.mutable

import OkxCodec.*
import OkxCodec.given

object OkxAccountFeed:
  /** Greeks REST 轮询间隔。OKX account-greeks WS 推送频率过低，改用 REST 轮询 (官方限速 10/2s) */
  val GreeksPollIntervalMs: Long = 1000

/** OKX 永续合约私有账户连接器。
  *
  * 连接 `/ws/v5/private`，带内握手 (WsLoop 无建连钩子，故用事件驱动)：
  *   1. start 时把 login 帧入队 (连上即发)
  *   2. 收到 login 成功 (event=login,code=0) 后，入队订阅 positions/account/orders
  *
  * 解析：
  *   - positions -> Position (张->币)；只发布推送中出现的仓位 (平仓时 OKX 推 pos=0)，
  *     初始 0 仓由 Engine 启动对齐负责
  *   - account   -> AccountInfo(净值/名义价值) + 各币种 Balance(cashBal，供 greeks delta 修正)
  *   - orders    -> Fill (fillSz>0 时, 先于 OrderUpdate) + OrderUpdate (张->币)
  *
  * 另起一个 fork 以 REST 轮询账户级希腊字母，去重后发布 [[Topics.Greeks]] 事件。
  *
  * Fail-fast：连接断开、解析失败、登录失败、错误事件一律抛异常终止引擎作用域。
  * Greeks 轮询失败是唯一例外 (记 warn 后下次重试)，不影响私有流主链路。
  */
final class OkxAccountFeed(
    client: OkxClient,
    backend: WebSocketSyncBackend,
    wsUrl: String = OkxClient.WsPrivateUrl,
) extends AccountFeed:
  private val logger = LoggerFactory.getLogger(classOf[OkxAccountFeed])
  private val credentials = client.wsCredentials

  override def exchange: Exchange = Exchange.Okx

  private val outgoing = Channel.unlimited[WebSocketFrame]
  /** symbol -> meta，用于张<->币换算 (连接时一次性拉取) */
  private var metas: Map[Symbol, SymbolMeta] = Map.empty
  /** greeks 去重：ccy -> 上次 timestamp */
  private val lastGreeksTs: mutable.Map[String, Timestamp] = mutable.Map.empty

  /** 回报归属的账户与发布通道，由柜台在 [[connect]] 时注入，之后只读 */
  @volatile private var account: AccountId = scala.compiletime.uninitialized
  @volatile private var publish: AnyEvent => Unit = scala.compiletime.uninitialized

  override def connect(acct: AccountId, sink: AnyEvent => Unit, spawn: (=> Unit) => Unit): Unit =
    account = acct
    publish = sink
    metas = client.symbolMetas // 进程内只拉一次, 与柜台读的是同一份

    WsLoop.run("okx/private", backend, () => wsUrl, outgoing, onPrivateText, spawn)
    // login 帧入队，连接建立后立即发送 (timestamp 在此刻生成；连接通常亚秒级，OKX 允许 ~30s 偏差)
    outgoing.send(WebSocketFrame.text(loginFrame()))

    // Greeks REST 轮询 (独立虚拟线程，与私有 WS 并行)
    spawn {
      while true do
        Thread.sleep(OkxAccountFeed.GreeksPollIntervalMs)
        pollGreeks()
    }

  private def loginFrame(): String =
    val ts = Instant.now().getEpochSecond.toString
    val sign = credentials.signWsLogin(ts)
    s"""{"op":"login","args":[{"apiKey":"${credentials.apiKey}","passphrase":"${credentials.passphrase}","timestamp":"$ts","sign":"$sign"}]}"""

  private val subscribeFrame =
    """{"op":"subscribe","args":[{"channel":"positions","instType":"SWAP"},{"channel":"account"},{"channel":"orders","instType":"SWAP"}]}"""

  /** 轮询账户希腊字母，去重 (ts 未变跳过) 后发布。失败仅 warn，下次重试 (不致命) */
  private def pollGreeks(): Unit =
    client.fetchGreeks() match
      case Right(list) =>
        list.foreach { g =>
          if lastGreeksTs.getOrElse(g.ccy, -1L) != g.timestamp then
            lastGreeksTs(g.ccy) = g.timestamp
            logger.debug(s"OKX greeks ${g.ccy}: delta=${g.delta} gamma=${g.gamma} theta=${g.theta} vega=${g.vega} ts=${g.timestamp}")
            publish(Event.at(Topics.Greeks, g, g.timestamp))
        }
      case Left(e) =>
        logger.warn(s"OKX fetch greeks failed (will retry): ${e.message}")

  // ==================== 私有流解析 (任何不理解的消息 -> 异常上抛终止) ====================

  private def onPrivateText(text: String): Unit =
    val env = readFromString[OkxEnvelope](text)
    if env.event.nonEmpty then handleControl(env, text)
    else
      env.arg.channel match
        case "positions" => readFromString[WsPush[PositionData]](text).data.foreach(publishPosition)
        case "account"   => readFromString[WsPush[AccountData]](text).data.foreach(publishAccount)
        case "orders"    => readFromString[WsPush[OrderPushData]](text).data.foreach(publishOrder)
        // account-greeks 走 REST 轮询，WS 同名频道 (若订阅) 直接忽略
        case "account-greeks" => ()
        case other            => throw IllegalStateException(s"Unexpected OKX private channel '$other': $text")

  private def handleControl(env: OkxEnvelope, text: String): Unit = env.event match
    case "login" =>
      if env.code == "0" then
        logger.info("OKX private login success, subscribing private channels")
        outgoing.send(WebSocketFrame.text(subscribeFrame))
      else throw IllegalStateException(s"OKX login failed: code=${env.code} msg=${env.msg}")
    case "subscribe" | "unsubscribe" | "channel-conn-count" => ()
    case "error" => throw IllegalStateException(s"OKX private WS error: code=${env.code} msg=${env.msg}")
    case other   => logger.warn(s"ignoring OKX private event '$other': $text")

  /** 缺少 meta 的 symbol 无法做张->币换算，跳过 (非配置 quote 的品种) */
  private def metaOf(symbol: Symbol): Option[SymbolMeta] = metas.get(symbol)

  private def publishPosition(d: PositionData): Unit =
    for
      sym <- fromOkx(d.instId)
      meta <- metaOf(sym)
    do
      val position = Position(
        account = account,
        exchange = Exchange.Okx,
        symbol = sym,
        size = meta.toCoin(Contracts(d.pos.asDouble)),
        entryPrice = Price(d.avgPx.asDoubleOrZero),
        unrealizedPnl = d.upl.asDoubleOrZero,
      )
      publish(Event.local(Topics.Position, position))

  private def publishAccount(d: AccountData): Unit =
    val ts = d.uTime.toLongOption.getOrElse(nowMs)
    publish(
      Event.at(Topics.AccountInfo, AccountInfo(account, Exchange.Okx, d.totalEq.asDouble, d.notionalUsd.asDouble), ts)
    )
    // 各币种现金余额：供 StateManager 修正 greeks delta 的现货敞口
    d.details.foreach { detail =>
      publish(Event.at(Topics.Balance, Balance(account, Exchange.Okx, detail.ccy, detail.cashBal.asDouble, ts), ts))
    }

  private def publishOrder(d: OrderPushData): Unit =
    val sym = fromOkx(d.instId).getOrElse(throw IllegalStateException(s"Unknown OKX instId in order: '${d.instId}'"))
    val meta = metaOf(sym).getOrElse(throw IllegalStateException(s"No SymbolMeta for OKX order symbol: $sym"))
    val side = d.side match
      case "buy"  => Side.Long
      case "sell" => Side.Short
      case other  => throw IllegalStateException(s"Unknown OKX side: '$other'")
    val fillSz = meta.toCoin(Contracts(d.fillSz.asDouble))
    val filledQty = meta.toCoin(Contracts(d.accFillSz.asDouble))
    // Fill 先于 OrderUpdate (确保乐观更新 position 后再处理订单终态)
    if fillSz.nonZero then
      val fill = Fill(account, Exchange.Okx, sym, side, price = d.fillPx.asPrice, size = fillSz, timestamp = nowMs)
      publish(Event.local(Topics.Fill, fill))
    val update = OrderUpdate(
      account = account,
      orderId = d.ordId,
      clientOrderId = if d.clOrdId.nonEmpty then Some(d.clOrdId) else None,
      exchange = Exchange.Okx,
      symbol = sym,
      side = side,
      status = mapOrderState(d.state, filledQty),
      price = Price(d.px.asDoubleOrZero),
      quantity = meta.toCoin(Contracts(d.sz.asDouble)),
      filledQuantity = filledQty,
      timestamp = nowMs,
    )
    publish(Event.local(Topics.OrderUpdate, update))
