package hft.exchange.binance

import com.github.plokhotnyuk.jsoniter_scala.core.*
import hft.domain.*
import hft.exchange.{AccountFeed, AccountReport, RestTransport, WsLoop}
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

  override def connect(
      sink: AccountReport => Unit,
      spawn: (=> Unit) => Unit,
      sleepUnlessStopped: Long => Boolean,
  ): Unit =
    report = sink
    // 装配期校验持仓模式: 本框架的账本只建模单向持仓。双向持仓模式下 ACCOUNT_UPDATE 会推
    // LONG/SHORT 行, 而它们无法用一个带符号的净仓表达 —— 这是账户级配置错误, 该在这里终止。
    client.isOneWayPositionMode() match
      case Right(true) => ()
      case Right(false) =>
        throw IllegalStateException(
          "Binance 账户处于双向持仓模式 (dualSidePosition=true), 本框架只支持单向持仓 —— " +
            "请在交易所把持仓模式改为单向, 或先实现双向持仓的账本模型"
        )
      case Left(e) =>
        throw IllegalStateException(s"无法确认 Binance 持仓模式, 拒绝启动: ${e.message}")
    WsLoop.run("binance/private", backend, privateStreamUrl, outgoing, onPrivateText, spawn)
    spawn {
      // 协作式睡眠: 停机请求立即唤醒并退出循环, 不必等 30 分钟或靠中断打断
      while !sleepUnlessStopped(BinanceAccountFeed.ListenKeyKeepAliveMs) do
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
    val filledQty = Coin(o.z.asDouble) // 累计成交 —— 记账的依据
    // **先报订单状态、后报成交明细**: 前者驱动记账与仓位快照, 后者只是明细。
    // 契约并不承诺 Fill 相对 Position 的位置 (Bybit 那是两条独立频道, 没法承诺),
    // 但同一条消息里的两件事顺序在我们手里 —— 能排就排, 少一处虚实差异。
    report(AccountReport.OrderStatusChanged(
      orderId = o.i.toString,
      clientOrderId = Some(o.c),
      symbol = o.s,
      side = side,
      status = status,
      price = o.p.asPrice,
      avgFillPrice = o.ap.asPrice, // 记账用它 —— 市价单的 o.p 是 0
      quantity = Coin(o.q.asDouble),
      reduceOnly = RestTransport.requireFlag(o.R, "Binance", "R(reduceOnly)", s"orderId=${o.i}"),
      filledQuantity = filledQty,
      timestamp = o.T,
    ))
    if o.l.asDouble > 0 then
      report(AccountReport.Executed(o.s, side, o.L.asPrice, Coin(o.l.asDouble), o.T))

  private def publishAccountUpdate(msg: AccountUpdateMsg): Unit =
    msg.a.B.foreach(b => report(AccountReport.BalanceChanged(b.a, b.wb.asDouble, msg.E)))
    // 交易所报的仓位 —— 交给柜台对账, 不进总线。推的是整个账户, 里面有本柜台不管的标的,
    // 由柜台按"已对齐过的标的"过滤 (见 RestTradingGateway.reconcile)。
    // 单向持仓模式下 positionSide 恒为 BOTH (Binance 文档: 单向模式只返回 BOTH)，而该模式
    // 已在 connect 的装配期校验过。走到这里说明模式在运行期被改了 —— 从前这里 filter 掉非 BOTH
    // 的行, 于是对账读数被整段丢掉、对账从此永远"一致"。
    msg.a.P.foreach { p =>
      if p.ps != "BOTH" then
        throw IllegalStateException(
          s"Binance 推来 positionSide=${p.ps} (symbol=${p.s}): 持仓模式在运行期被改成了双向, " +
            "本框架的账本只建模单向持仓"
        )
      report(AccountReport.PositionReported(p.s, Coin(p.pa.asDouble), msg.E))
    }

object BinanceAccountFeed:
  /** listenKey 续期间隔。Binance 的 listenKey 60 分钟过期，30 分钟续一次留足余量：
    * 续期失败即私有流将被断开、推送丢失，因此这条循环的失败是致命的。 */
  val ListenKeyKeepAliveMs: Long = 30 * 60 * 1000
