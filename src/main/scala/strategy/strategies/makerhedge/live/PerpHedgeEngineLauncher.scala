package strategy.strategies.makerhedge.live
import strategy.utils.option.*

import hft.domain.{AccountId, Coin, Exchange}
import hft.engine.Engine
import hft.exchange.bybit.{BybitAccountFeed, BybitClient, BybitCredentials, BybitMarketFeed}
import hft.exchange.RestTradingGateway
import strategy.strategies.makerhedge.logic.{AsymHedgeBand, MakerHedgeStrategy}
import org.slf4j.LoggerFactory
import ox.supervised
import sttp.client4.DefaultSyncBackend

/** 永续 delta 对冲实盘启动器 (引擎集成版)。**复用实盘引擎**: BBO 走 [[BybitMarketFeed]] (不二次订阅),
  * 期权净 greeks 由 [[OptionGreeksFeed]] 从柜台的汇报面注入同一条总线; 对冲用引擎原生的
  * [[MakerHedgeStrategy]] (MaAsym 带 maker 挂单 + 5s 重挂), 订单经下单指令进柜台 (柜台按
  * SymbolMeta 对齐精度)。
  * gammaAdjust=true: 两次 greeks 轮询间用引擎 BBO + gamma 一阶刷新 delta -> tick 级新鲜。
  *
  * ATR/均线的历史预热由策略自己在 [[hft.strategy.Strategy.prepare]] 里做 (本启动器只注入取数通道), 避免冷启动等数十小时。**无 dry-run, 启动即真实对冲下单** —— 用小资金测试。
  * 护栏 (替代 dry-run): 必须有 key; `maxHedgeQty` 单笔对冲张数硬上限 (超出不下单+告警); greeks 缺失/陈旧暂停。
  *
  * **配置**: 全部参数 (含 API 密钥) 走 JSON 文件 [[BybitHedgeConfig]], 路径由第一个命令行参数指定
  * (默认 `conf/perp-hedge-bybit.json`)。模板见 `conf/perp-hedge-bybit.example.json`。
  * **密钥在文件中明文 -> chmod 600 且勿入库** (真实配置已 .gitignore)。
  *
  * 运行: sbt "runMain strategy.strategies.makerhedge.live.PerpHedgeEngineLauncher [conf/perp-hedge-bybit.json]"
  */
@main def PerpHedgeEngineLauncher(args: String*): Unit =
  val logger = LoggerFactory.getLogger("PerpHedgeEngineLauncher")
  val confPath = args.headOption.getOrElse("conf/perp-hedge-bybit.json")
  val conf = HedgeConfig.loadBybit(confPath) match
    case Right(c) => c
    case Left(e)  => logger.error(s"加载配置失败 ($confPath): $e"); sys.exit(1)
  if conf.apiKey.isEmpty || conf.apiSecret.isEmpty then
    logger.error(s"配置 $confPath 缺 apiKey/apiSecret (读期权/永续持仓 + 下单需签名), 退出"); sys.exit(1)

  // **非主网配置一律拒绝启动** —— 理由同 OKX 侧: BybitClient.trading / BybitMarketFeed /
  // BybitAccountFeed 都恒连主网 (工厂里写死 RestBaseUrl 与 WsUrl), testnet 只传给了
  // BybitOptionsClient。配置写 testnet=true 的结果是期权腿去测试网、**对冲腿的永续在主网
  // 真金白银下单**, 而日志还打印着 "testnet"。在框架支持环境切换之前, 宁可不启动。
  if conf.testnet then
    logger.error(
      s"配置 $confPath 要求测试网 (testnet=true), 但永续腿 (BybitClient/BybitMarketFeed/BybitAccountFeed) " +
        "目前只连主网 —— 期权腿会去测试网而对冲腿在主网真实下单, 两条腿的持仓互不相干。拒绝启动。"
    )
    sys.exit(1)
  val t = conf.tuning
  val credentials = Some(BybitCredentials(conf.apiKey, conf.apiSecret))

  logger.warn(s"PerpHedge(引擎集成) *** 实盘 LIVE *** (无 dry-run): symbol=${t.symbol} ccy=${t.ccy} greeks轮询=${t.greeksPollMs}ms 带=均线上 上${t.tightAtr}ATR/下${t.looseAtr}ATR 单笔上限=${t.maxHedgeQty}")
  logger.warn(s"配置=$confPath  ${if conf.testnet then "testnet" else "mainnet"}")

  supervised:
    val backend = DefaultSyncBackend()
    val perp = BybitClient.trading(backend, credentials.get) // 永续 (linear) 下单/查仓
    val opt = BybitOptionsClient(backend, credentials, testnet = conf.testnet)

    // 一个汇报面 = 期权 greeks 注入流 (先, 同步发 ccy 余额兜底) + Bybit 永续账户流 (持仓/订单回报)
    val feed = CompositeAccountFeed(Exchange.Bybit, Seq(OptionGreeksFeed(opt, Exchange.Bybit, t.ccy, t.greeksPollMs), BybitAccountFeed(perp, backend)))

    // 柜台在前、行情在后: 柜台既接下单指令也推回报 (消费者), 行情源是纯生产者。
    val gateway = RestTradingGateway.load(perp, feed, AccountId.Live)
    val engine = Engine.start(plugins = Vector(gateway, BybitMarketFeed(backend))) // 实盘

    // greeks 陈旧阈值 = 4× 轮询间隔 (连续几次拉取失败即暂停对冲, 不按过期 delta 乱挂)
    val strategy = MakerHedgeStrategy(Exchange.Bybit, t.symbol, t.ccy, AsymHedgeBand.byMa(t.tightAtr, t.looseAtr),
      offsetPct = t.offset, requoteMs = t.requoteMs, gammaAdjust = true, maxGreeksStaleMs = t.greeksPollMs * 4, maxHedgeQty = Coin(t.maxHedgeQty),
      // K 线粒度只配一处 (klineBar), 序列与预热都从它来 —— 从前策略这边用的是默认 1h,
      // 与配置无关, 配成别的粒度就会把预热数据按 1h 打时间戳喂进去。
      barIntervalMs = t.klineBarMs,
      // 预热取数。**根数听策略的** —— 要多少根取决于 ATR/RV 的窗口, 那是策略的知识;
      // 从前这里写死 64 根, 而序列容量按 max(atrPeriodBars*4, rvLongWindowBars+8, 64) 算,
      // 长窗口的 RV 一直没喂满。粒度对不上就报错, 别让它静默按错的粒度喂。
      history = (barMs, bars) =>
        if barMs != t.klineBarMs then
          Left(s"策略要 ${barMs}ms 的 K 线, 而配置的粒度是 ${t.klineBar} (${t.klineBarMs}ms)")
        else opt.linearKlines(t.symbol, t.klineBar, bars))
    engine.addStrategy(strategy, AccountId.Live) // 真实盘

    logger.warn("对冲腿运行中 (BBO 复用引擎行情流, 期权 greeks 每 %dms 注入). Ctrl+C 退出".format(t.greeksPollMs))
    // 阻塞到停机: 中断信号或组件失败都会唤醒它, 停完全部组件 (onStop 逐个跑到) 核心最后退出
    engine.awaitShutdown()
