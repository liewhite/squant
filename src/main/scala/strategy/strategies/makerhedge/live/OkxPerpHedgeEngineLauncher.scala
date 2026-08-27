package strategy.strategies.makerhedge.live
import strategy.utils.option.*

import hft.domain.{AccountId, Coin, Exchange}
import hft.engine.Engine
import hft.exchange.okx.{OkxAccountFeed, OkxClient, OkxCredentials, OkxMarketFeed}
import hft.exchange.RestTradingGateway
import strategy.strategies.makerhedge.logic.{AsymHedgeBand, MakerHedgeStrategy}
import org.slf4j.LoggerFactory
import ox.supervised
import sttp.client4.DefaultSyncBackend

/** 永续 delta 对冲实盘启动器 (OKX, 引擎集成版)。与 [[PerpHedgeEngineLauncher]] (Bybit) 同构, 仅交易所实现不同:
  * BBO 走引擎的 [[OkxMarketFeed]] (不二次订阅), 永续持仓/订单回报走 [[OkxAccountFeed]], 期权净 greeks 由
  * [[OptionGreeksFeed]] 经 [[OkxOptionsClient]] 注入同一条 事件总线; 对冲用引擎原生 [[MakerHedgeStrategy]]。
  *
  * **OKX 特性**: [[OkxAccountFeed]] 已原生轮询账户级 greeks, 这里的 [[OptionGreeksFeed]] 主要保证
  * **ccy 余额兜底** (否则 StateManager.greeks 恒为 None -> 静默不对冲 -> 期权裸敞口), 并以 [[OptionsExchange]]
  * 抽象与 Bybit 路径保持一致; 二者 greeks 同源 (account/greeks), last-write-wins, 冗余轮询成本可忽略。
  *
  * ATR/均线的历史预热由策略自己在 [[hft.strategy.Strategy.prepare]] 里做 (本启动器只注入取数通道)。**无 dry-run, 启动即真实对冲下单** —— 用小资金测试。
  * 护栏: 必须有 key+passphrase; `maxHedgeQty` 单笔对冲张数硬上限; greeks 缺失/陈旧暂停。
  *
  * **配置**: 全部参数 (含 API 密钥+passphrase) 走 JSON 文件 [[OkxHedgeConfig]], 路径由第一个命令行参数指定
  * (默认 `conf/perp-hedge-okx.json`)。模板见 `conf/perp-hedge-okx.example.json`。
  * **密钥在文件中明文 -> chmod 600 且勿入库** (真实配置已 .gitignore)。
  *
  * **OKX 符号约定**: tuning.symbol 用基础币 (ETH, 框架 OKX 统一 symbol 即基础币), 永续 instId 由框架按
  * `ETH-<quote>-SWAP` 转换; 对冲与现货修正的 ccy 同为基础币; tuning.klineBar 用 OKX 粒度 (如 "1H")。
  *
  * 运行: sbt "runMain strategy.strategies.makerhedge.live.OkxPerpHedgeEngineLauncher [conf/perp-hedge-okx.json]"
  */
@main def OkxPerpHedgeEngineLauncher(args: String*): Unit =
  val logger = LoggerFactory.getLogger("OkxPerpHedgeEngineLauncher")
  val confPath = args.headOption.getOrElse("conf/perp-hedge-okx.json")
  val conf = HedgeConfig.loadOkx(confPath) match
    case Right(c) => c
    case Left(e)  => logger.error(s"加载配置失败 ($confPath): $e"); sys.exit(1)
  if conf.apiKey.isEmpty || conf.apiSecret.isEmpty || conf.passphrase.isEmpty then
    logger.error(s"配置 $confPath 缺 apiKey/apiSecret/passphrase (读期权/永续持仓 + 下单需签名), 退出"); sys.exit(1)

  // **非主网配置一律拒绝启动** —— 这里的"模拟盘"目前只到期权腿。
  //
  // OkxClient.trading / OkxMarketFeed / OkxAccountFeed 都恒连主网 (工厂里写死 RestBaseUrl 与
  // 两条 WsUrl, 签名上根本没有环境这个参数), 而 simulated 只传给了 OkxOptionsClient。
  // 于是配置写 simulated=true、日志大声打印"模拟盘"、期权腿确实去了模拟环境, 而**对冲腿的
  // 永续在主网真金白银下单** —— 两条腿的持仓从此互不相干, delta 对冲对着一个不存在的敞口做。
  //
  // 在框架把"连的是不是真钱"变成一个贯穿三件套的类型之前, 宁可不启动: 让人以为在模拟盘
  // 而实际在下真单, 是这套系统能犯的最贵的错。
  if conf.simulated then
    logger.error(
      s"配置 $confPath 要求模拟盘 (simulated=true), 但永续腿 (OkxClient/OkxMarketFeed/OkxAccountFeed) " +
        "目前只连主网 —— 期权腿会去模拟环境而对冲腿在主网真实下单, 两条腿的持仓互不相干。拒绝启动。"
    )
    sys.exit(1)
  val t = conf.tuning
  val credentials = Some(OkxCredentials(conf.apiKey, conf.apiSecret, conf.passphrase))

  logger.warn(s"OkxPerpHedge(引擎集成) *** 实盘 LIVE *** (无 dry-run): symbol=${t.symbol} ccy=${t.ccy} greeks轮询=${t.greeksPollMs}ms 带=均线上 上${t.tightAtr}ATR/下${t.looseAtr}ATR 单笔上限=${t.maxHedgeQty}")
  logger.warn(s"配置=$confPath  ${if conf.simulated then "模拟盘(simulated)" else "主网(mainnet)"}")

  supervised:
    val backend = DefaultSyncBackend()
    val perp = OkxClient.trading(backend, credentials.get, quote = conf.quote) // 永续 (SWAP) 下单/查仓
    val opt = OkxOptionsClient(backend, credentials, quote = conf.quote, optionCcy = Some(t.ccy), simulated = conf.simulated)

    // 汇报面 = 期权 greeks 注入流 (先, 同步发 ccy 余额兜底) + OKX 永续账户流 (持仓/订单回报/账户)
    val feed = CompositeAccountFeed(Exchange.Okx, Seq(OptionGreeksFeed(opt, Exchange.Okx, t.ccy, t.greeksPollMs), OkxAccountFeed(perp, backend)))

    // 柜台在前、行情在后: 柜台既接下单指令也推回报 (消费者), 行情源是纯生产者。
    val gateway = RestTradingGateway.load(perp, feed, AccountId.Live)
    val engine = Engine.start(plugins = Vector(gateway, OkxMarketFeed(perp, backend))) // 实盘

    // greeks 陈旧阈值 = 4× 轮询间隔 (连续几次拉取失败即暂停对冲, 不按过期 delta 乱挂)
    val strategy = MakerHedgeStrategy(Exchange.Okx, t.symbol, t.ccy, AsymHedgeBand.byMa(t.tightAtr, t.looseAtr),
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
    Thread.sleep(Long.MaxValue)
