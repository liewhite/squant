package app.live

import hft.domain.Exchange
import hft.engine.{Engine, ExchangeGateway}
import hft.exchange.okx.{OkxAccountStream, OkxClient, OkxCredentials, OkxMarketStream}
import strategy.live.{MaAsymHedgeBand, MakerHedgeStrategy}
import org.slf4j.LoggerFactory
import ox.supervised
import sttp.client4.DefaultSyncBackend

/** 永续 delta 对冲实盘启动器 (OKX, 引擎集成版)。与 [[PerpHedgeEngineLauncher]] (Bybit) 同构, 仅交易所实现不同:
  * BBO 走引擎的 [[OkxMarketStream]] (不二次订阅), 永续持仓/订单回报走 [[OkxAccountStream]], 期权净 greeks 由
  * [[OptionGreeksStream]] 经 [[OkxOptionsClient]] 注入同一条 income 总线; 对冲用引擎原生 [[MakerHedgeStrategy]]。
  *
  * **OKX 特性**: [[OkxAccountStream]] 已原生轮询账户级 greeks, 这里的 [[OptionGreeksStream]] 主要保证
  * **ccy 余额兜底** (否则 StateManager.greeks 恒为 None -> 静默不对冲 -> 期权裸敞口), 并以 [[OptionsExchange]]
  * 抽象与 Bybit 路径保持一致; 二者 greeks 同源 (account/greeks), last-write-wins, 冗余轮询成本可忽略。
  *
  * 启动时用历史 K 线 prewarm ATR/均线。**无 dry-run, 启动即真实对冲下单** —— 用小资金测试。
  * 护栏: 必须有 key+passphrase; `VOLHEDGE_MAX_QTY` 单笔对冲张数硬上限; greeks 缺失/陈旧暂停。
  * `VOLHEDGE_SIMULATED=1` 走 OKX 模拟盘 (real order)。
  *
  * **OKX 符号约定**: symbol 用基础币 (VOLHEDGE_SYMBOL=ETH, 框架 OKX 统一 symbol 即基础币), 永续 instId 由
  * 框架按 `ETH-<quote>-SWAP` 转换; 对冲与现货修正的 ccy 同为基础币。
  *
  * 运行: OKX_API_KEY=.. OKX_API_SECRET=.. OKX_PASSPHRASE=.. sbt "runMain app.live.OkxPerpHedgeEngineLauncher"
  */
@main def OkxPerpHedgeEngineLauncher(): Unit =
  val logger = LoggerFactory.getLogger("OkxPerpHedgeEngineLauncher")
  val symbol = sys.env.getOrElse("VOLHEDGE_SYMBOL", "ETH") // OKX 统一 symbol=基础币
  val ccy = sys.env.getOrElse("VOLHEDGE_CCY", "ETH")
  val quote = sys.env.getOrElse("VOLHEDGE_QUOTE", "USDT")
  val greeksPollMs = sys.env.get("VOLHEDGE_GREEKS_POLL_MS").map(_.toLong).getOrElse(3000L)
  val offset = sys.env.get("VOLHEDGE_OFFSET").map(_.toDouble).getOrElse(0.0002)
  val requoteMs = sys.env.get("VOLHEDGE_REQUOTE_MS").map(_.toLong).getOrElse(5000L)
  val tightAtr = sys.env.get("VOLHEDGE_TIGHT_ATR").map(_.toDouble).getOrElse(1.0)
  val looseAtr = sys.env.get("VOLHEDGE_LOOSE_ATR").map(_.toDouble).getOrElse(2.0)
  val maxHedgeQty = sys.env.get("VOLHEDGE_MAX_QTY").map(_.toDouble).getOrElse(5.0) // 单笔对冲硬上限 (sanity, 小资金测试)
  val klineBar = sys.env.getOrElse("VOLHEDGE_KLINE_BAR", "1H") // OKX K 线粒度 (ATR/均线预热)
  val simulated = sys.env.get("VOLHEDGE_SIMULATED").contains("1")

  val credentials =
    for k <- sys.env.get("OKX_API_KEY"); s <- sys.env.get("OKX_API_SECRET"); p <- sys.env.get("OKX_PASSPHRASE")
    yield OkxCredentials(k, s, p)
  if credentials.isEmpty then
    logger.error("缺 OKX_API_KEY/SECRET/PASSPHRASE (读期权/永续持仓 + 下单需签名), 退出")
    sys.exit(1)

  logger.warn(s"OkxPerpHedge(引擎集成) *** 实盘 LIVE *** (无 dry-run): symbol=$symbol ccy=$ccy greeks轮询=${greeksPollMs}ms 带=均线上 上${tightAtr}ATR/下${looseAtr}ATR 单笔上限=$maxHedgeQty")
  logger.warn(s"${if simulated then "模拟盘(simulated)" else "主网(mainnet)"}")

  supervised:
    val backend = DefaultSyncBackend()
    val perp = OkxClient(backend, credentials, quote = quote) // 永续 (SWAP) 下单/查仓
    val opt = OkxOptionsClient(backend, credentials, quote = quote, optionCcy = Some(ccy), simulated = simulated)
    val market = OkxMarketStream(perp, backend)
    // accountStream = 期权 greeks 注入流 (先, 同步发 ccy 余额兜底) + OKX 永续账户流 (持仓/订单回报/账户)
    val account = CompositeAccountStream(Exchange.Okx, Seq(OptionGreeksStream(opt, Exchange.Okx, ccy, greeksPollMs), OkxAccountStream(perp, backend)))

    val engine = Engine.start(gateways = Vector(ExchangeGateway(perp, market, Some(account)))) // 实盘 (dryRun 默认 false)

    // greeks 陈旧阈值 = 4× 轮询间隔 (连续几次拉取失败即暂停对冲, 不按过期 delta 乱挂)
    val strategy = MakerHedgeStrategy(Exchange.Okx, symbol, ccy, MaAsymHedgeBand(tightAtr, looseAtr),
      offsetPct = offset, requoteMs = requoteMs, gammaAdjust = true, maxGreeksStaleMs = greeksPollMs * 4, maxHedgeQty = maxHedgeQty)
    // 历史 K 线预热 ATR/均线 (开机即就绪)
    opt.linearKlines(symbol, klineBar, 64) match
      case Right(bars) => strategy.prewarm(bars); logger.warn(s"prewarm ${bars.size} 根 $klineBar K线 -> ATR/均线就绪")
      case Left(e)     => logger.error(s"prewarm 取 K 线失败 (ATR 将靠实时 BBO 慢热): $e")
    engine.addStrategy(strategy)

    logger.warn("对冲腿运行中 (BBO 复用引擎行情流, 期权 greeks 每 %dms 注入). Ctrl+C 退出".format(greeksPollMs))
    Thread.sleep(Long.MaxValue)
