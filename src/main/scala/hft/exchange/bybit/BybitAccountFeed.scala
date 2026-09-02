package hft.exchange.bybit

import com.github.plokhotnyuk.jsoniter_scala.core.*
import hft.domain.*
import hft.exchange.{AccountFeed, AccountReport, RestTransport, WsLoop}
import org.slf4j.LoggerFactory
import ox.channels.Channel
import sttp.client4.WebSocketSyncBackend
import sttp.ws.WebSocketFrame

import java.time.Instant
import scala.collection.mutable

import BybitCodec.*
import BybitCodec.given

object BybitAccountFeed:
  /** 算作真实成交的执行类型。其余 (资金费、结算) 不进账本。
    *
    * 空串**不在**这里：Bybit 的 execution 帧一定带 execType，空串只可能是 codec 默认值顶上来的
    * (即报文缺字段)。把它算成成交等于给缺失字段填了默认值。 */
  private val TradeExecTypes: Set[String] = Set("Trade", "AdlTrade", "BustTrade", "Delivery", "BlockTrade")

  /** 明确不是成交的执行类型 —— 见到它们跳过，不必告警。
    *
    * 依据 Bybit v5 `docs/v5/enum` 的 execType 全表: Funding (资金费)、Settle (反向合约结算)、
    * MovePosition (转仓)、FutureSpread (价差腿)、ForwardSplitSettle / ReverseSplitSettle
    * (股票拆分的零股结算)、Dividend (分红)。`SessionSettlePnl` 保留 (UTA 的日结)。
    *
    * 与 [[TradeExecTypes]] 合起来覆盖文档全表；剩下的只有 `UNKNOWN`，见 publishExecution。 */
  private val NonTradeExecTypes: Set[String] = Set(
    "Funding",
    "Settle",
    "SessionSettlePnl",
    "MovePosition",
    "FutureSpread",
    "ForwardSplitSettle",
    "ReverseSplitSettle",
    "Dividend",
  )

  /** 文档标注为 "May be returned by a classic account. Cannot query by this type"。
    *
    * 本客户端只用统一账户 (accountType=UNIFIED)，因此它的出现意味着账户类型的前提被打破 ——
    * 而"这条执行到底算不算成交"无从判断，静默丢弃会让对账报出一个无从定位的差额。 */
  private val ClassicAccountExecType: String = "UNKNOWN"

  /** auth 帧 expires 相对当前时间的前移量 (ms)，给握手留出窗口 */
  val AuthExpiresBufferMs: Long = 10_000

  /** `wallet` 推送 -> 净值 + **列出的各币种当前余额**。
    *
    * 纯函数: 这里是本连接器对余额的全部业务判断所在, 而它决定的是 delta 对冲把多少现货算进敞口。
    *
    * **不报"整份钱包"**: Bybit 文档只写了"订阅成功时不给 snapshot"
    * ("There is no snapshot event given at the time when the subscription is successful"),
    * 从未声明 `coin[]` 覆盖全部币种 —— 官方示例里 `totalWalletBalance` 远大于唯一列出的那条的
    * `usdValue`, 自己就反证了。全量只信启动对齐的 REST 钱包。
    *
    * 当成全量做整表替换的代价比 OKX 更重: Bybit **没有**周期性全量推送, 于是一次只有 USDT
    * 变动的推送会把 ETH 现货抹成 0, 并一直保持到 ETH 余额自己再变一次。
    *
    * @param ts 本地接收时刻 —— Bybit 的 wallet 帧不带交易所时间戳 */
  private[bybit] def walletReports(d: WalletData, ts: Timestamp): Vector[AccountReport] =
    AccountReport.EquityChanged(d.totalEquity.asDouble, ts) +:
      d.coin.map(c => AccountReport.BalanceChanged(c.coin, c.walletBalance.asDouble, ts)).toVector

/** Bybit v5 USDT linear 永续私有账户连接器。
  *
  * 连接 `/v5/private`，带内握手 (WsLoop 无建连钩子，故事件驱动)：
  *   1. start 时把 auth 帧入队 (连上即发)
  *   2. 收到 auth 成功 (op=auth,success=true) 后，入队订阅 execution/order/wallet
  *
  * 解析 (linear 数量即币本位，无张<->币换算)：
  *   - execution -> 成交明细 (本次成交量与成交价)。**不参与记账**
  *   - order     -> 订单状态 + **累计成交量**, 柜台据它记账 (见 hft.exchange.AccountReport)
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
  @volatile private var cooperativeSleep: Long => Boolean = scala.compiletime.uninitialized

  override def connect(
      sink: AccountReport => Unit,
      spawn: (=> Unit) => Unit,
      sleepUnlessStopped: Long => Boolean,
  ): Unit =
    report = sink
    spawnThread = spawn
    cooperativeSleep = sleepUnlessStopped
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

  /** 私有流报文入口。`private[bybit]` 而非 `private`: 报文 -> [[AccountReport]] 的映射是本连接器
    * 全部业务含义所在, 而它从前没有任何测试能碰到 —— "把增量当全量" 这个 bug 就藏在这一层。 */
  private[bybit] def onPrivateText(text: String): Unit =
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

  /** 单笔成交 -> 成交明细。**不参与记账** —— 仓位由 order 频道的累计成交量驱动。
    *
    * **先按 execType 筛**：本频道推的不只是成交 —— 资金费结算 (Funding) 与交割结算 (Settle)
    * 也走这里，而它们的 execQty 是仓位量不是成交量。把它们当成交报出去，
    * 成交记录与绩效统计里就会多出一整笔仓位大小的"成交"。
    */
  private def publishExecution(d: ExecutionData): Unit =
    if !BybitAccountFeed.TradeExecTypes.contains(d.execType) then
      // 明确不是成交的类型: 跳过 (有依据, 见 NonTradeExecTypes)。
      // 其余一律抛 —— 丢弃一条不认识的执行帧会让对账报出"我们这边的 bug"却无从定位,
      // 而空 execType 说明报文本身缺字段。
      if d.execType == BybitAccountFeed.ClassicAccountExecType then
        throw IllegalStateException(
          s"Bybit 返回 execType=UNKNOWN (order=${d.orderId} symbol=${d.symbol} qty=${d.execQty}) —— " +
            "该值文档标注只出现在**经典账户**, 而本客户端按统一账户接入; 这条执行算不算成交无从判断"
        )
      if !BybitAccountFeed.NonTradeExecTypes.contains(d.execType) then
        throw IllegalStateException(
          s"文档之外的 Bybit execType: '${d.execType}' (order=${d.orderId} symbol=${d.symbol} qty=${d.execQty})"
        )
      return
    val sym = fromBybit(d.symbol).getOrElse(throw IllegalStateException(s"Unknown Bybit symbol in execution: '${d.symbol}'"))
    report(AccountReport.Executed(
      symbol = sym,
      side = sideFromBybit(d.side),
      price = d.execPrice.asPrice,
      qty = Coin(d.execQty.asDouble),
      // 交易所时间戳不可解析 = 坏报文。退回本地钟会让延迟统计恒为零, 且掩盖了报文问题。
      timestamp = d.execTime.toLongOption.getOrElse(
        throw IllegalStateException(s"Bybit execution execTime 不是时间戳: '${d.execTime}' (order=${d.orderId})")
      ),
    ))

  /** 订单状态。本频道带**累计**成交量 —— 柜台拿它补记账 (execution 先到时增量为零)，
    * 于是两条频道谁先到都不影响账本。 */
  private def publishOrder(d: OrderData): Unit =
    val sym = fromBybit(d.symbol).getOrElse(throw IllegalStateException(s"Unknown Bybit symbol in order: '${d.symbol}'"))
    val filled = Coin(d.cumExecQty.asDouble)
    val status = mapOrderStatus(d.orderStatus, filled)
    // 晚到的那条 execution 由柜台兜住: 记账进度在终态后还留一分钟墓碑,
    // 认得出"这笔已经记过了" (见 PositionBook 的墓碑说明)。
    report(AccountReport.OrderStatusChanged(
      orderId = d.orderId,
      clientOrderId = if d.orderLinkId.nonEmpty then Some(d.orderLinkId) else None,
      symbol = sym,
      side = sideFromBybit(d.side),
      status = status,
      price = Price(d.price.asDoubleOrZero),
      avgFillPrice = Price(d.avgPrice.asDoubleOrZero), // 记账用它 —— 市价单的 price 为空
      reduceOnly = RestTransport.requireFlag(d.reduceOnly, "Bybit", "reduceOnly", s"orderId=${d.orderId}"),
      quantity = Coin(d.qty.asDouble),
      filledQuantity = filled,
      timestamp = d.updatedTime.toLongOption.getOrElse(
        throw IllegalStateException(s"Bybit order 推送缺 updatedTime: orderId=${d.orderId} 原始值='${d.updatedTime}'")
      ),
    ))

  /** 交易所报的仓位 -> 交给柜台对账。size 是绝对值, 方向在 side 里 */
  private def publishPosition(d: PositionData): Unit =
    val sym = fromBybit(d.symbol).getOrElse(throw IllegalStateException(s"Unknown Bybit symbol in position: '${d.symbol}'"))
    val magnitude = d.size.asDouble
    // 穷举方向。从前是 `case _ => magnitude`: 任何未知 side 都被记成**多头**,
    // 而同一件事在 REST 路径上写的是 `case _ => Coin.Zero` (归零) —— 同一事实两个答案。
    val signed = d.side match
      case "Buy"           => magnitude
      case "Sell"          => -magnitude
      // 空仓: Bybit v5 position 的 side 为**空串** (docs/v5/position 的 WS 示例即 side:"" size:"0")
      case "" if magnitude == 0.0 => 0.0
      case other =>
        throw IllegalStateException(s"未知的 Bybit 持仓方向: '$other' (symbol=${d.symbol} size=${d.size})")
    report(AccountReport.PositionReported(sym, Coin(signed), nowMs))

  private def publishWallet(d: WalletData): Unit =
    BybitAccountFeed.walletReports(d, nowMs).foreach(report)

  /** 心跳发送线程：定期入队 ping 帧，维持私有连接 (无成交时也不致空闲被断) */
  private def startHeartbeat(): Unit =
    spawnThread {
      // 协作式睡眠: 停机请求立即唤醒并退出, 不必靠中断打断 (见 AccountFeed.connect 契约)
      while !cooperativeSleep(BybitMarketFeed.HeartbeatIntervalMs) do
        outgoing.send(WebSocketFrame.text("""{"op":"ping"}"""))
    }
    ()
