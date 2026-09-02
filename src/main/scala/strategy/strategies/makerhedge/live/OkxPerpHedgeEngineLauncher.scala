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
  * BBO 走引擎的 [[OkxMarketFeed]] (不二次订阅), 永续持仓/订单回报/账户级 greeks/逐币种余额都走
  * [[OkxAccountFeed]], 推上同一条事件总线; 对冲用引擎原生 [[MakerHedgeStrategy]]。
  *
  * **OKX 特性**: [[OkxAccountFeed]] 已原生轮询账户级 greeks, 因此这条路径上不需要
  * [[OptionGreeksFeed]] —— 叠上去只会让 `Topics.Greeks` 有两个同源发布者 (last-write-wins)。
  *
  * 现货余额: WS 只推**发生变动的币种**, 那份"整份钱包"由启动对齐的一次 REST 查询给出
  * (见 [[hft.domain.Wallet]]) —— 对 delta 对冲来说, 它决定了要不要把现货算进敞口。
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

    // 汇报面 = OKX 永续账户流 (持仓/订单回报/账户级 greeks/全量钱包)。
    //
    // **不再叠一个 OptionGreeksFeed**: OkxAccountFeed 已原生轮询 account/greeks, 两者同源
    // (同一个 REST 接口、同一份口径), 装两个的结果是 Topics.Greeks 上有两个发布者、
    // last-write-wins —— 一个 topic 两个真相来源, 而它们唯一可能的差别就是新旧。
    // 它当初存在的真正理由是"注入一条假的 ccy 余额", 而那件事现在由启动对齐的 REST 钱包
    // 快照如实给出 (Topics.Wallet, 见 hft.domain.Wallet)。
    // greeks 轮询间隔从配置注入 —— 它同时决定发布节奏与下游的陈旧闸门 (下面的 maxGreeksStaleMs),
    // 两处必须是同一个数: 闸门比真实轮询周期还紧的话, 对冲会永久判为"读数陈旧"而暂停。
    val feed = OkxAccountFeed(perp, backend, greeksPollMs = t.greeksPollMs)

    // 柜台在前、行情在后: 柜台既接下单指令也推回报 (消费者), 行情源是纯生产者。
    val gateway = RestTradingGateway.load(perp, feed, AccountId.Live)
    Engine.run(plugins = Vector(gateway, OkxMarketFeed(perp, backend))) { engine => // 实盘

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
      // 阻塞到停机: 中断信号或组件失败都会唤醒它, 停完全部组件 (onStop 逐个跑到) 核心最后退出
      engine.awaitShutdown()
    }
