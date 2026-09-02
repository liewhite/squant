package hft.exchange.bybit

import com.github.plokhotnyuk.jsoniter_scala.core.*
import hft.domain.*
import hft.exchange.{MarketFeed, WsLoop}
import hft.event.{Event, Topics}
import org.slf4j.LoggerFactory
import ox.channels.Channel
import sttp.client4.WebSocketSyncBackend
import sttp.ws.WebSocketFrame

import BybitCodec.*
import BybitCodec.given

object BybitMarketFeed:
  /** 应用层心跳间隔。Bybit 要求 ~20s 内发 ping，否则连接被服务端判为失活而关闭 */
  val HeartbeatIntervalMs: Long = 20_000

/** Bybit v5 USDT linear 永续公共行情连接器。
  *
  * 所有公共频道复用单条 `/v5/public/linear` 连接，订阅以 `{"op":"subscribe","args":[...]}` 增量下发：
  *   - orderbook.1.{symbol} -> BBO (level1 仅 snapshot，取首档；数量即币本位)
  *   - tickers.{symbol}     -> MarkPrice + IndexPrice + FundingRate (snapshot+delta，按非空字段发布)
  *   - publicTrade.{symbol} -> MarketTrade
  *
  * 与 OKX 的差异：Bybit linear 数量即币本位 (contractSize=1)，故**无需拉取 SymbolMeta 做换算**；
  * symbol 原生即统一格式，从 topic 末段提取 (delta 帧 data 可能省略 symbol)。
  *
  * Fail-fast：连接断开、解析失败、订阅失败 (success=false) 一律抛异常终止引擎作用域。
  */
final class BybitMarketFeed(
    backend: WebSocketSyncBackend,
    wsUrl: String = BybitClient.WsPublicLinearUrl,
) extends MarketFeed:
  private val logger = LoggerFactory.getLogger(classOf[BybitMarketFeed])

  override def exchange: Exchange = Exchange.Bybit

  private val outgoing = Channel.unlimited[WebSocketFrame]

  override protected def connect(): Unit =
    WsLoop.run("bybit/public", backend, () => wsUrl, outgoing, onPublicText, body => fork(body))
    startHeartbeat(outgoing)

  /** 基类已去重，这里收到的都是尚未订阅过的流 */
  override protected def subscribeToExchange(kinds: Set[SubscriptionKind]): Unit =
    // tickers 频道被 Mark/Index/Funding 共用，topic 集合天然去重
    val args = kinds.map(topicOf).map(t => s"\"$t\"").mkString(",")
    outgoing.send(WebSocketFrame.text(s"""{"op":"subscribe","args":[$args]}"""))

  private def topicOf(kind: SubscriptionKind): String = kind match
    case SubscriptionKind.BBO(s)         => s"orderbook.1.$s"
    case SubscriptionKind.MarkPrice(s)   => s"tickers.$s"
    case SubscriptionKind.IndexPrice(s)  => s"tickers.$s"
    case SubscriptionKind.FundingRate(s) => s"tickers.$s"
    case SubscriptionKind.Trade(s)       => s"publicTrade.$s"

  // ==================== 公共流解析 (解析失败/订阅失败 -> 异常上抛终止) ====================

  private def onPublicText(text: String): Unit =
    val msg = readFromString[BybitWsMsg](text)
    if msg.op.nonEmpty then handleControl(msg, text)
    else if msg.topic.startsWith("orderbook.") then
      val push = readFromString[WsObj[OrderbookData]](text)
      publishBbo(symbolFromTopic(push.topic), push.data, push.ts)
    else if msg.topic.startsWith("tickers.") then
      val push = readFromString[WsObj[TickerData]](text)
      publishTicker(symbolFromTopic(push.topic), push.data, push.ts)
    else if msg.topic.startsWith("publicTrade.") then
      val sym = symbolFromTopic(msg.topic)
      readFromString[WsList[PublicTradeData]](text).data.foreach(d => publishTrade(sym, d))
    else throw IllegalStateException(s"Unexpected Bybit public topic '${msg.topic}': $text")

  private def handleControl(msg: BybitWsMsg, text: String): Unit = msg.op match
    case "subscribe" =>
      if !msg.success then throw IllegalStateException(s"Bybit public subscribe failed: ${msg.ret_msg}")
    case "ping" | "pong" => () // 心跳响应
    case other           => logger.warn(s"ignoring Bybit public op '$other': $text")

  /** topic 末段即 symbol: "orderbook.1.BTCUSDT" / "tickers.BTCUSDT" / "publicTrade.BTCUSDT" -> "BTCUSDT" */
  private def symbolFromTopic(topic: String): Symbol =
    val sym = topic.substring(topic.lastIndexOf('.') + 1)
    if sym.nonEmpty then sym else throw IllegalStateException(s"Cannot extract symbol from Bybit topic: '$topic'")

  // orderbook.1 (linear) 官方为 snapshot-only 全量推送，正常情况下双侧首档必存；
  // 某侧为空属异常 (非 delta 缺省)，按 fail-fast 上抛并带上原始档位便于诊断
  private def publishBbo(sym: Symbol, d: OrderbookData, ts: Long): Unit =
    val bid = d.b.headOption.getOrElse(throw IllegalStateException(s"Bybit orderbook empty bids: $sym (b=${d.b} a=${d.a})"))
    val ask = d.a.headOption.getOrElse(throw IllegalStateException(s"Bybit orderbook empty asks: $sym (b=${d.b} a=${d.a})"))
    val bbo = BBO(
      exchange = Exchange.Bybit,
      symbol = sym,
      bidPrice = bid.head.asPrice,
      bidQty = Coin(bid(1).asDouble), // 已是币本位
      askPrice = ask.head.asPrice,
      askQty = Coin(ask(1).asDouble),
      timestamp = ts,
    )
    publish(Event.at(Topics.Bbo, bbo, ts))

  /** 各标的最近一次见到的 `nextFundingTime`。
    *
    * ## 为什么必须存这一份状态
    *
    * Bybit 的 tickers 是 **snapshot + delta**：delta 帧只带发生变化的字段。预测资金费率
    * (`fundingRate`) 频繁变动，而 `nextFundingTime` 每 8 小时才变一次 —— 于是**绝大多数**
    * delta 帧带着 fundingRate 却不带 nextFundingTime。
    *
    * 从前这里写 `nextFundingTime.toLongOption.getOrElse(0L)`，把"这一帧没带"当成了"结算时刻是 0"。
    * 下游 [[FundingRate.dailyRate]] 见 `currentTime >= 0` 直接返回 0.0 —— 资金费信号被静默归零，
    * 没有任何症状。delta 协议要求接收方保存上一帧的状态，用默认值替代状态就是替上游的协议买单。
    *
    * 只由公共流的接收线程读写 (单条连接、单线程解析)，无需同步。
    */
  private var lastFundingTime: Map[Symbol, Timestamp] = Map.empty

  /** tickers 一帧 (snapshot 或 delta) 携带 mark/index/funding，仅发布本帧实际出现 (非空) 的字段 */
  private def publishTicker(sym: Symbol, d: TickerData, ts: Long): Unit =
    if d.markPrice.nonEmpty then
      publish(Event.at(Topics.MarkPrice, MarkPrice(Exchange.Bybit, sym, d.markPrice.asPrice, ts), ts))
    if d.indexPrice.nonEmpty then
      publish(Event.at(Topics.IndexPrice, IndexPrice(Exchange.Bybit, sym, d.indexPrice.asPrice, ts), ts))
    // 本帧带了结算时刻就更新状态；不可解析则是坏报文, 抛出而不是当成"没带"
    if d.nextFundingTime.nonEmpty then
      val settle = d.nextFundingTime.toLongOption.getOrElse(
        throw IllegalStateException(s"Bybit tickers nextFundingTime 不是时间戳: '${d.nextFundingTime}' ($sym)")
      )
      lastFundingTime = lastFundingTime.updated(sym, settle)
    if d.fundingRate.nonEmpty then
      // 没有结算时刻就发不出一条有意义的资金费 —— snapshot 之前不发布, 而不是发一条 0。
      lastFundingTime.get(sym) match
        case Some(settle) =>
          val fr = FundingRate(
            exchange = Exchange.Bybit,
            symbol = sym,
            rate = d.fundingRate.asDouble,
            nextSettleTime = settle,
            timestamp = ts,
          )
          publish(Event.at(Topics.FundingRate, fr, ts))
        case None =>
          logger.warn(s"Bybit $sym 收到 fundingRate 但还没见过 nextFundingTime (snapshot 之前), 本帧不发布")

  private def publishTrade(sym: Symbol, d: PublicTradeData): Unit =
    // Bybit S = taker 方向: S=Sell -> 买方是挂单方 (isBuyerMaker=true)
    val trade = MarketTrade(Exchange.Bybit, sym, d.p.asPrice, Coin(d.v.asDouble), isBuyerMaker = d.S == "Sell", d.T)
    publish(Event.at(Topics.Trade, trade, d.T))

  /** 心跳发送任务：定期入队 ping 帧，维持连接 (服务端回 pong 同时刷新 WsLoop 空闲计时)。
    *
    * 用 [[MarketFeed.sleepUnlessStopped]] 而非裸 `Thread.sleep` —— 后者只能靠中断打断，
    * 于是每次停机都多一次"能不能按时退出"的不确定，超时就会让组件被隔离。 */
  private def startHeartbeat(out: Channel[WebSocketFrame]): Unit =
    fork {
      while !sleepUnlessStopped(BybitMarketFeed.HeartbeatIntervalMs) do
        out.send(WebSocketFrame.text("""{"op":"ping"}"""))
    }
    ()
