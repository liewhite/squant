package app.live.hedge
import app.live.option.*

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
  * 护栏 (替代 dry-run): 必须有 key; `maxHedgeQty` 单笔对冲张数硬上限 (超出不下单+告警); greeks 缺失/陈旧暂停。
  *
  * **配置**: 全部参数 (含 API 密钥) 走 JSON 文件 [[BybitHedgeConfig]], 路径由第一个命令行参数指定
  * (默认 `conf/perp-hedge-bybit.json`)。模板见 `conf/perp-hedge-bybit.example.json`。
  * **密钥在文件中明文 -> chmod 600 且勿入库** (真实配置已 .gitignore)。
  *
  * 运行: sbt "runMain app.live.PerpHedgeEngineLauncher [conf/perp-hedge-bybit.json]"
  */
@main def PerpHedgeEngineLauncher(args: String*): Unit =
  val logger = LoggerFactory.getLogger("PerpHedgeEngineLauncher")
  val confPath = args.headOption.getOrElse("conf/perp-hedge-bybit.json")
  val conf = HedgeConfig.loadBybit(confPath) match
    case Right(c) => c
    case Left(e)  => logger.error(s"加载配置失败 ($confPath): $e"); sys.exit(1)
  if conf.apiKey.isEmpty || conf.apiSecret.isEmpty then
    logger.error(s"配置 $confPath 缺 apiKey/apiSecret (读期权/永续持仓 + 下单需签名), 退出"); sys.exit(1)
  val t = conf.tuning
  val credentials = Some(BybitCredentials(conf.apiKey, conf.apiSecret))

  logger.warn(s"PerpHedge(引擎集成) *** 实盘 LIVE *** (无 dry-run): symbol=${t.symbol} ccy=${t.ccy} greeks轮询=${t.greeksPollMs}ms 带=均线上 上${t.tightAtr}ATR/下${t.looseAtr}ATR 单笔上限=${t.maxHedgeQty}")
  logger.warn(s"配置=$confPath  ${if conf.testnet then "testnet" else "mainnet"}")

  supervised:
    val backend = DefaultSyncBackend()
    val perp = BybitClient(backend, credentials) // 永续 (linear) 下单/查仓
    val opt = BybitOptionsClient(backend, credentials, testnet = conf.testnet)
    val market = BybitMarketStream(backend)
    // 一个 accountStream = 期权 greeks 注入流 (先, 同步发 ccy 余额兜底) + Bybit 永续账户流 (持仓/订单回报)
    val account = CompositeAccountStream(Exchange.Bybit, Seq(OptionGreeksStream(opt, Exchange.Bybit, t.ccy, t.greeksPollMs), BybitAccountStream(perp, backend)))

    val engine = Engine.start(gateways = Vector(ExchangeGateway(perp, market, Some(account)))) // 实盘 (dryRun 默认 false)

    // greeks 陈旧阈值 = 4× 轮询间隔 (连续几次拉取失败即暂停对冲, 不按过期 delta 乱挂)
    val strategy = MakerHedgeStrategy(Exchange.Bybit, t.symbol, t.ccy, MaAsymHedgeBand(t.tightAtr, t.looseAtr),
      offsetPct = t.offset, requoteMs = t.requoteMs, gammaAdjust = true, maxGreeksStaleMs = t.greeksPollMs * 4, maxHedgeQty = t.maxHedgeQty)
    // 历史 K 线预热 ATR/均线 (开机即就绪)
    opt.linearKlines(t.symbol, t.klineBar, 64) match
      case Right(bars) => strategy.prewarm(bars); logger.warn(s"prewarm ${bars.size} 根 ${t.klineBar} K线 -> ATR/均线就绪")
      case Left(e)     => logger.error(s"prewarm 取 K 线失败 (ATR 将靠实时 BBO 慢热): $e")
    engine.addStrategy(strategy)

    logger.warn("对冲腿运行中 (BBO 复用引擎行情流, 期权 greeks 每 %dms 注入). Ctrl+C 退出".format(t.greeksPollMs))
    Thread.sleep(Long.MaxValue)
