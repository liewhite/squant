package hft.exchange.okx

import com.github.plokhotnyuk.jsoniter_scala.core.*
import hft.domain.*
import hft.exchange.{AccountFeed, AccountReport, WsLoop}
import org.slf4j.LoggerFactory
import ox.channels.Channel
import sttp.client4.WebSocketSyncBackend
import sttp.ws.WebSocketFrame

import java.time.Instant
import scala.collection.mutable

import OkxCodec.*
import OkxCodec.given

object OkxAccountFeed:
  /** Greeks REST 轮询间隔的**默认值**。OKX account-greeks WS 推送频率过低，改用 REST 轮询。
    *
    * 可由构造参数覆盖 —— 因为下游的"读数陈旧即暂停对冲"闸门是按这个间隔的倍数定的
    * (见 `MakerHedgeStrategy.maxGreeksStaleMs`)：写死在这里的话，闸门与真实发布节奏各说各话,
    * 把闸门调紧到小于实际轮询周期就会让对冲永久暂停, 而没有任何一处会报错。 */
  val GreeksPollIntervalMs: Long = 1000

  /** OKX `/account/greeks` 的限速是 10 次 / 2 秒 (官方文档), 即最快 200ms 一次。 */
  val MinGreeksPollIntervalMs: Long = 200

  /** `account` 推送 -> 净值 + **逐币种当前余额**。
    *
    * 纯函数: 这里是本连接器对余额的全部业务判断所在, 而它决定的是 delta 对冲把多少现货算进敞口。
    *
    * **不报"整份钱包"**: OKX 文档写明只有 initial/regular snapshot 是全量, 变动触发的
    * `event_update` 只带那一个币种, 且全量快照本身可能按 `curPage`/`lastPage` 分页。
    * 报文里没有 `eventType`/分页字段可读 (见 [[OkxCodec.WsPush]]), 因此这条通道给不出"全量"
    * 这个事实 —— 那份全量来自启动对齐的 REST 钱包 (`TradingGateway.currentWallet`)。
    *
    * 当成全量做整表替换的代价: 一次只有 USDT 变动的推送会把 ETH 现货抹成 0, 对冲随即按
    * 少算了整份现货的 delta 下单, 直到下一次 regular snapshot 才自愈。 */
  private[okx] def accountReports(d: AccountData): Vector[AccountReport] =
    val ts = d.uTime.toLongOption.getOrElse(
      throw IllegalStateException(s"OKX account 推送缺 uTime: 原始值='${d.uTime}'")
    )
    AccountReport.EquityChanged(d.totalEq.asDouble, ts) +:
      d.details.map(detail => AccountReport.BalanceChanged(detail.ccy, detail.cashBal.asDouble, ts)).toVector

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
    greeksPollMs: Long = OkxAccountFeed.GreeksPollIntervalMs,
    wsUrl: String = OkxClient.WsPrivateUrl,
) extends AccountFeed:
  require(
    greeksPollMs >= OkxAccountFeed.MinGreeksPollIntervalMs,
    s"greeks 轮询间隔 ${greeksPollMs}ms 快于 OKX 限速 (10 次/2 秒 -> 最快 ${OkxAccountFeed.MinGreeksPollIntervalMs}ms)",
  )
  private val logger = LoggerFactory.getLogger(classOf[OkxAccountFeed])
  private val credentials = client.wsCredentials

  override def exchange: Exchange = Exchange.Okx

  private val outgoing = Channel.unlimited[WebSocketFrame]
  /** symbol -> meta，用于张<->币换算 (连接时一次性拉取) */
  private var metas: Map[Symbol, SymbolMeta] = Map.empty
  /** greeks 去重：ccy -> 上次 timestamp */
  private val lastGreeksTs: mutable.Map[String, Timestamp] = mutable.Map.empty

  /** 解析结果的去处，由柜台在 [[connect]] 时注入，之后只读。
    * 本流不知道账户是谁 —— 那是柜台盖的章 */
  @volatile private var report: AccountReport => Unit = scala.compiletime.uninitialized

  override def connect(
      sink: AccountReport => Unit,
      spawn: (=> Unit) => Unit,
      sleepUnlessStopped: Long => Boolean,
  ): Unit =
    report = sink
    metas = client.symbolMetas // 进程内只拉一次, 与柜台读的是同一份

    WsLoop.run("okx/private", backend, () => wsUrl, outgoing, onPrivateText, spawn)
    // login 帧入队，连接建立后立即发送 (timestamp 在此刻生成；连接通常亚秒级，OKX 允许 ~30s 偏差)
    outgoing.send(WebSocketFrame.text(loginFrame()))

    // Greeks REST 轮询 (受管任务, 与私有 WS 并行)。协作式睡眠: 停机即退出。
    spawn {
      while !sleepUnlessStopped(greeksPollMs) do pollGreeks()
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
            report(AccountReport.GreeksChanged(g.ccy, g.delta, g.gamma, g.theta, g.vega, g.timestamp))
        }
      case Left(e) =>
        logger.warn(s"OKX fetch greeks failed (will retry): ${e.message}")

  // ==================== 私有流解析 (任何不理解的消息 -> 异常上抛终止) ====================

  /** 私有流报文入口。`private[okx]` 而非 `private`: 报文 -> [[AccountReport]] 的映射是本连接器
    * 全部业务含义所在 (增量还是全量、方向、单位换算), 而它从前没有任何测试能碰到 ——
    * "把增量当全量" 这个 bug 就藏在这一层。 */
  private[okx] def onPrivateText(text: String): Unit =
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

  /** 张->币换算所需的合约规格。
    *
    * **缺了就抛**：`metas` 是装配期一次性加载的全量表 (见 `ExchangeClient.symbolMetas`)，
    * 一个已被 `fromOkx` 认作本 quote 的 symbol 却查不到规格，只能是规格表没加载全或
    * 新上市合约还没进表 —— 而这条仓位读数是对账的输入，静默跳过等于让对账永远"一致"。
    *
    * 从前同一个条件在本文件里有两种处理: 仓位路径用 for-comprehension 静默跳过、
    * 订单路径直接抛。同一事实必须只有一个答案。 */
  private def metaOf(symbol: Symbol): SymbolMeta =
    metas.getOrElse(symbol, throw IllegalStateException(s"OKX 缺 $symbol 的合约规格 (装配期未加载?)"))

  /** 交易所报的仓位 —— 交给柜台对账, 不进总线 */
  private def publishPosition(d: PositionData): Unit =
    // 非本 quote 的品种 (币本位等) 不属于本柜台, fromOkx 返回 None 即跳过 —— 这条有依据。
    fromOkx(d.instId, client.quote).foreach { sym =>
      report(AccountReport.PositionReported(
        Instrument.perp(Exchange.Okx, sym),
        metaOf(sym).toCoin(Contracts(d.pos.asDouble)),
        nowMs,
      ))
    }

  private def publishAccount(d: AccountData): Unit =
    OkxAccountFeed.accountReports(d).foreach(report)

  private def publishOrder(d: OrderPushData): Unit =
    val sym = fromOkx(d.instId, client.quote).getOrElse(throw IllegalStateException(s"Unknown OKX instId in order: '${d.instId}'"))
    val meta = metaOf(sym)
    val side = d.side match
      case "buy"  => Side.Long
      case "sell" => Side.Short
      case other  => throw IllegalStateException(s"Unknown OKX side: '$other'")
    val fillSz = meta.toCoin(Contracts(d.fillSz.asDouble))
    val filledQty = meta.toCoin(Contracts(d.accFillSz.asDouble))
    val ts = nowMs
    // **先报订单状态、后报成交明细**: 前者驱动记账与仓位快照, 后者只是明细。
    // 契约并不承诺 Fill 相对 Position 的位置, 但同一条消息里的顺序在我们手里 ——
    // 能排就排, 少一处虚实差异。
    report(AccountReport.OrderStatusChanged(
      orderId = d.ordId,
      clientOrderId = if d.clOrdId.nonEmpty then Some(d.clOrdId) else None,
      // fromOkx 只认本 quote 的永续 (见它的说明), 所以到这里的必然是 U 本位永续。
      // 接期权时这里要按 instId 解析出品种。
      instrument = Instrument.perp(Exchange.Okx, sym),
      side = side,
      status = mapOrderState(d.state, filledQty),
      price = Price(d.px.asDoubleOrZero),
      avgFillPrice = Price(d.avgPx.asDoubleOrZero), // 记账用它 —— 市价单的 px 为空
      reduceOnly = OkxCodec.booleanFrom(d.reduceOnly, "reduceOnly"),
      quantity = meta.toCoin(Contracts(d.sz.asDouble)),
      filledQuantity = filledQty,
      timestamp = ts,
    ))
    if fillSz.nonZero then
      report(AccountReport.Executed(Instrument.perp(Exchange.Okx, sym), side, d.fillPx.asPrice, fillSz, ts))
