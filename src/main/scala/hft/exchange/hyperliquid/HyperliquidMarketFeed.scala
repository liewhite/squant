package hft.exchange.hyperliquid

import com.github.plokhotnyuk.jsoniter_scala.core.*
import hft.domain.*
import hft.exchange.{MarketFeed, WsLoop}
import hft.event.{Event, Topics}
import org.slf4j.LoggerFactory
import ox.channels.Channel
import sttp.client4.WebSocketSyncBackend
import sttp.ws.WebSocketFrame

import HyperliquidCodec.*
import HyperliquidCodec.given

/** Hyperliquid 公共行情连接器。
  *
  * 全部公共频道复用单条 `/ws` 连接，订阅以 `{"method":"subscribe","subscription":{...}}` 增量下发：
  *   - bbo            -> BBO
  *   - trades         -> MarketTrade
  *   - activeAssetCtx -> MarkPrice + IndexPrice + FundingRate (一条推送拆三个事件)
  *
  * ## 一个实例服务一个 perp dex
  *
  * `dex` 决定线路上的 coin 前缀，也决定本实例认领哪些行情 (见 [[HyperliquidCodec]])。
  * 要同时看默认 dex 的加密永续和 xyz 的股票永续，就装两个实例 —— 而**不是**让一个实例
  * 两边都收：框架 `Symbol` 取基础资产名，两个 dex 的同名资产会落进同一个 `Instrument`。
  * 两个实例各自认领 `MarketSubscription` 指令 (基数 `AtLeastOne`)，但那样一来同一条订阅
  * 指令会被两个实例都执行，其中一个必然订不到 —— 故**同进程只应装一个**，dex 归属由
  * 装配方决定。
  *
  * ## 心跳
  *
  * Hyperliquid 服务端在 60 秒没收到任何消息时断开连接，且不主动发 WS ping。因此本实现
  * 周期性发送应用层 `{"method":"ping"}`；服务端回 `{"channel":"pong"}`，这条回帧同时
  * 喂饱了 [[WsLoop]] 的空闲看门狗。
  *
  * Fail-fast：连接断开、解析失败、服务端 error 报文 (含订阅被拒) 一律抛异常终止引擎作用域。
  */
final class HyperliquidMarketFeed(
    backend: WebSocketSyncBackend,
    val dex: String = "",
    wsUrl: String = HyperliquidClient.WsUrl,
    pingIntervalMs: Long = 30_000,
) extends MarketFeed:
  private val logger = LoggerFactory.getLogger(classOf[HyperliquidMarketFeed])

  override def exchange: Exchange = Exchange.Hyperliquid

  override def name: String = if dex.isEmpty then s"market-feed@$exchange" else s"market-feed@$exchange:$dex"

  private val outgoing = Channel.unlimited[WebSocketFrame]

  /** 已下发的**底层流**。基类按 [[SubscriptionKind]] 去重，但标记价/指数价/资金费率
    * 共用一条 `activeAssetCtx` 流 —— 不在这一层再去一次重，同一条流会被订阅两三次，
    * 服务端以 error 报文拒绝，而那在本实现里是致命的。
    *
    * 只由 actor 事件循环这一个线程读写 (订阅指令经邮箱串行到达)，无需同步。
    */
  private var subscribedStreams: Set[String] = Set.empty

  override protected def connect(): Unit =
    WsLoop.run("hyperliquid/public", backend, () => wsUrl, outgoing, onPublicText, body => fork(body))
    fork {
      while !sleepUnlessStopped(pingIntervalMs) do outgoing.send(WebSocketFrame.text("""{"method":"ping"}"""))
    }

  override protected def subscribeToExchange(kinds: Set[SubscriptionKind]): Unit =
    val fresh = kinds.map(streamOf).filterNot(subscribedStreams.contains)
    if fresh.nonEmpty then
      subscribedStreams ++= fresh
      logger.info(s"subscribing ${fresh.size} streams on $name")
      fresh.foreach(stream => outgoing.send(WebSocketFrame.text(s"""{"method":"subscribe","subscription":$stream}""")))

  /** 订阅报文即流标识：同一条报文就是同一条流，去重与下发因此不可能各说各话 */
  private def streamOf(kind: SubscriptionKind): String =
    val coin = toHyperliquid(kind.subscribedSymbol, dex)
    kind match
      case _: SubscriptionKind.BBO   => s"""{"type":"bbo","coin":"$coin"}"""
      case _: SubscriptionKind.Trade => s"""{"type":"trades","coin":"$coin"}"""
      // 三者共用 activeAssetCtx (一条消息拆三个事件)
      case _: SubscriptionKind.MarkPrice | _: SubscriptionKind.IndexPrice | _: SubscriptionKind.FundingRate =>
        s"""{"type":"activeAssetCtx","coin":"$coin"}"""

  // ==================== 公共流解析 (解析失败/错误报文 -> 异常上抛终止) ====================

  private def onPublicText(text: String): Unit =
    readFromString[WsEnvelope](text).channel match
      case "bbo"            => publishBbo(readFromString[BboPush](text).data)
      case "trades"         => readFromString[TradesPush](text).data.foreach(publishTrade)
      case "activeAssetCtx" => publishAssetCtx(readFromString[AssetCtxPush](text).data)
      case "subscriptionResponse" | "pong" => ()
      // 订阅被拒也走这条路：连接仍然健康、ping/pong 正常，而那个标的的行情再也不来
      case "error" => throw IllegalStateException(s"Hyperliquid public WS error: $text")
      case other   => throw IllegalStateException(s"Unexpected Hyperliquid public channel '$other': $text")

  private def requireSymbol(coin: String): Symbol =
    fromHyperliquid(coin, dex).getOrElse(
      throw IllegalStateException(s"Hyperliquid coin '$coin' 不属于本实例的 dex '$dex'")
    )

  /** 单边盘口 (某一侧为 null) 是稀薄品种的**合法市场状态**，不是坏报文：跳过即可。
    * 按解析错误处理会让一次挂单真空级联停掉整个引擎。 */
  private def publishBbo(d: BboData): Unit =
    (d.bbo.lift(0).flatten, d.bbo.lift(1).flatten) match
      case (Some(bid), Some(ask)) =>
        val bbo = BBO(
          exchange = Exchange.Hyperliquid,
          symbol = requireSymbol(d.coin),
          bidPrice = bid.px.asPrice,
          bidQty = Coin(bid.sz.asDouble),
          askPrice = ask.px.asPrice,
          askQty = Coin(ask.sz.asDouble),
          timestamp = d.time,
        )
        publish(Event.at(Topics.Bbo, bbo, d.time))
      case _ => logger.debug(s"Hyperliquid 单边盘口, 跳过: ${d.coin}")

  /** HL 线路逐笔下发 (同一主动单吃穿多档会拆成多条)，与 Binance aggTrade / OKX trades 的
    * 归集口径不同。此处不做归集：使用方是价差/信号，多一条拆单不改变结论。 */
  private def publishTrade(d: TradeData): Unit =
    val trade = MarketTrade(
      exchange = Exchange.Hyperliquid,
      symbol = requireSymbol(d.coin),
      price = d.px.asPrice,
      qty = Coin(d.sz.asDouble),
      // side 是主动方: "A" = 主动卖 -> 买方是挂单方
      isBuyerMaker = d.side == "A",
      timestamp = d.time,
    )
    publish(Event.at(Topics.Trade, trade, d.time))

  /** activeAssetCtx 一次携带标记价/预言机价/资金费率，拆为三个事件发布。
    *
    * 报文不带时间戳，故取本地时刻 —— 这是**推送时刻的读数**，不是交易所标注的时间。
    * 资金费率按小时结算，日化口径由 `nextSettleTime` 表达 (见 [[FundingRate.dailyRate]])。
    */
  private def publishAssetCtx(d: AssetCtxData): Unit =
    val symbol = requireSymbol(d.coin)
    val ts = nowMs
    publish(Event.at(Topics.MarkPrice, MarkPrice(Exchange.Hyperliquid, symbol, d.ctx.markPx.asPrice, ts), ts))
    publish(Event.at(Topics.IndexPrice, IndexPrice(Exchange.Hyperliquid, symbol, d.ctx.oraclePx.asPrice, ts), ts))
    publish(
      Event.at(
        Topics.FundingRate,
        FundingRate(
          exchange = Exchange.Hyperliquid,
          symbol = symbol,
          rate = d.ctx.funding.asDouble,
          nextSettleTime = HyperliquidMarketFeed.nextHourlySettle(ts),
          timestamp = ts,
        ),
        ts,
      )
    )

object HyperliquidMarketFeed:
  private val HourMs = 3_600_000L

  /** Hyperliquid 每小时整点结算资金费 */
  def nextHourlySettle(now: Timestamp): Timestamp = (now / HourMs + 1) * HourMs
