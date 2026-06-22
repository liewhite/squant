package voltrade

import hft.domain.Exchange
import hft.engine.{Engine, ExchangeGateway}
import hft.exchange.bybit.{BybitAccountStream, BybitClient, BybitCredentials, BybitMarketStream}
import strategy.live.{MaAsymHedgeBand, MakerHedgeStrategy}
import org.slf4j.LoggerFactory
import ox.supervised
import sttp.client4.DefaultSyncBackend

/** 永续 delta 对冲实盘启动器 (引擎集成版)。**复用实盘引擎**: BBO 走引擎已有的 BybitMarketStream (不二次订阅),
  * 期权净 greeks 由 [[OptionGreeksStream]] 注入同一条 income 总线; 对冲用引擎原生的 [[MakerHedgeStrategy]]
  * (MaAsym 带 maker 挂单 + 5s 重挂), 订单走引擎 outcomeBus (StrategyRunner 自动按 SymbolMeta 对齐精度)。
  * gammaAdjust=true: 两次 greeks 轮询间用引擎 BBO + gamma 一阶刷新 delta -> tick 级新鲜。
  *
  * 启动时用历史 K 线 prewarm ATR/均线, 避免冷启动等数十小时。**无 dry-run, 启动即真实对冲下单** —— 用小资金测试。
  * 护栏 (替代 dry-run): 必须有 key; `VOLHEDGE_MAX_QTY` 单笔对冲张数硬上限 (超出不下单+告警); greeks 缺失/陈旧暂停。
  * `VOLHEDGE_TESTNET=1` 走 testnet (real order)。
  *
  * 运行: BYBIT_API_KEY=.. BYBIT_API_SECRET=.. sbt "runMain voltrade.PerpHedgeEngineLauncher"
  */
@main def PerpHedgeEngineLauncher(): Unit =
  val logger = LoggerFactory.getLogger("PerpHedgeEngineLauncher")
  val symbol = sys.env.getOrElse("VOLHEDGE_SYMBOL", "ETHUSDT")
  val ccy = sys.env.getOrElse("VOLHEDGE_CCY", "ETH")
  val greeksPollMs = sys.env.get("VOLHEDGE_GREEKS_POLL_MS").map(_.toLong).getOrElse(3000L)
  val offset = sys.env.get("VOLHEDGE_OFFSET").map(_.toDouble).getOrElse(0.0002)
  val requoteMs = sys.env.get("VOLHEDGE_REQUOTE_MS").map(_.toLong).getOrElse(5000L)
  val tightAtr = sys.env.get("VOLHEDGE_TIGHT_ATR").map(_.toDouble).getOrElse(1.0)
  val looseAtr = sys.env.get("VOLHEDGE_LOOSE_ATR").map(_.toDouble).getOrElse(2.0)
  val maxHedgeQty = sys.env.get("VOLHEDGE_MAX_QTY").map(_.toDouble).getOrElse(5.0) // 单笔对冲硬上限 (sanity, 小资金测试)
  val testnet = sys.env.get("VOLHEDGE_TESTNET").contains("1")

  val credentials = for k <- sys.env.get("BYBIT_API_KEY"); s <- sys.env.get("BYBIT_API_SECRET") yield BybitCredentials(k, s)
  if credentials.isEmpty then
    logger.error("缺 BYBIT_API_KEY/SECRET (读期权/永续持仓 + 下单需签名), 退出")
    sys.exit(1)

  logger.warn(s"PerpHedge(引擎集成) *** 实盘 LIVE *** (无 dry-run): symbol=$symbol ccy=$ccy greeks轮询=${greeksPollMs}ms 带=均线上 上${tightAtr}ATR/下${looseAtr}ATR 单笔上限=$maxHedgeQty")
  logger.warn(s"${if testnet then "testnet" else "mainnet"}")

  supervised:
    val backend = DefaultSyncBackend()
    val perp = BybitClient(backend, credentials) // 永续 (linear) 下单/查仓
    val opt = BybitOptionsClient(backend, credentials, testnet = testnet)
    val market = BybitMarketStream(backend)
    // 一个 accountStream = 期权 greeks 注入流 (先, 同步发 ccy 余额兜底) + Bybit 永续账户流 (持仓/订单回报)
    val account = CompositeAccountStream(Exchange.Bybit, Seq(OptionGreeksStream(opt, Exchange.Bybit, ccy, greeksPollMs), BybitAccountStream(perp, backend)))

    val engine = Engine.start(gateways = Vector(ExchangeGateway(perp, market, Some(account)))) // 实盘 (dryRun 默认 false)

    // greeks 陈旧阈值 = 4× 轮询间隔 (连续几次拉取失败即暂停对冲, 不按过期 delta 乱挂)
    val strategy = MakerHedgeStrategy(Exchange.Bybit, symbol, ccy, MaAsymHedgeBand(tightAtr, looseAtr),
      offsetPct = offset, requoteMs = requoteMs, gammaAdjust = true, maxGreeksStaleMs = greeksPollMs * 4, maxHedgeQty = maxHedgeQty)
    // 历史 K 线预热 ATR/均线 (开机即就绪)
    opt.linearKlines(symbol, "60", 64) match
      case Right(bars) => strategy.prewarm(bars); logger.warn(s"prewarm ${bars.size} 根 1h K线 -> ATR/均线就绪")
      case Left(e)     => logger.error(s"prewarm 取 K 线失败 (ATR 将靠实时 BBO 慢热): $e")
    engine.addStrategy(strategy)

    logger.warn("对冲腿运行中 (BBO 复用引擎行情流, 期权 greeks 每 %dms 注入). Ctrl+C 退出".format(greeksPollMs))
    Thread.sleep(Long.MaxValue)
