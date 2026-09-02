package strategy.strategies.ivsellhedge.live

import strategy.strategies.ivsellhedge.logic.DeltaHedgeStrategy
import strategy.utils.hedge.DeltaBand
import strategy.utils.option.OkxOptionsClient

import hft.domain.{AccountId, Coin, Exchange}
import hft.engine.Engine
import hft.exchange.okx.{OkxAccountFeed, OkxClient, OkxCredentials, OkxMarketFeed}
import hft.exchange.RestTradingGateway
import org.slf4j.LoggerFactory
import ox.supervised
import sttp.client4.DefaultSyncBackend

/** **IV 定量卖出宽跨 + KAMA 死区 delta 对冲** 的实盘启动器 (OKX)。
  *
  * ```
  *   期权链/IV/持仓/现金 ──> OptionSellerActor ──┬─> REST 卖出 (IOC, 旁路框架下单通道)
  *                                              └─> OptionExposure (1s) ─┐
  *                                                                       ↓
  *   永续 BBO ─> 引擎行情流 ──────────────────────> DeltaHedgeStrategy ─> 框架下单通道 ─> 永续
  *   永续持仓/回报 ─> OkxAccountFeed ────────────↗
  * ```
  *
  * 两条腿的分工是刻意的：
  *   - **期权腿**走 actor + REST。框架 OKX 适配层只认永续 instId，期权单进不了它的下单通道；
  *     换来的是零框架改动，代价是这条腿没有 pending 跟踪与绩效统计 (卖出走 IOC，无需要跟踪的状态)。
  *   - **对冲腿**走框架原生策略。永续下单该有的一切 (clientOrderId、pending 登记、超时校验、
  *     停机撤单、精度换算) 都由框架施加。
  *
  * 装配顺序有讲究：**先挂策略、再起 actor**。`addStrategy` 的第一步就是订阅总线，
  * 反过来的话 actor 已经开始发敞口读数而没有人在听 —— 那些事件就丢了 (总线不重放)。
  *
  * **无 dry-run**：对冲腿启动即真实下单；卖出腿可用 `enableOpen=false` 只看意图。用小资金测试。
  *
  * **配置**: 全部参数 (含密钥) 走 JSON [[OkxIvSellHedgeConfig]]，路径 = 首个命令行参数
  * (默认 `conf/iv-sell-hedge-okx.json`)。模板见 `conf/iv-sell-hedge-okx.example.json`。
  * **密钥明文 -> chmod 600 且勿入库**。
  *
  * 运行: sbt "runMain strategy.strategies.ivsellhedge.live.OkxIvSellHedgeLauncher [conf/iv-sell-hedge-okx.json]"
  */
@main def OkxIvSellHedgeLauncher(args: String*): Unit =
  val logger = LoggerFactory.getLogger("OkxIvSellHedgeLauncher")
  val confPath = args.headOption.getOrElse("conf/iv-sell-hedge-okx.json")
  val conf = IvSellHedgeConfig.loadOkx(confPath) match
    case Right(c) => c
    case Left(e)  => logger.error(s"加载配置失败 ($confPath): $e"); sys.exit(1)
  if conf.apiKey.isEmpty || conf.apiSecret.isEmpty || conf.passphrase.isEmpty then
    logger.error(s"配置 $confPath 缺 apiKey/apiSecret/passphrase (读持仓/净值 + 下单需签名), 退出"); sys.exit(1)

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
  val sellerCfg = t.toSellerConfig // 内含参数校验, 越界即在此抛错 (启动即失败, 不带病上线)
  val credentials = OkxCredentials(conf.apiKey, conf.apiSecret, conf.passphrase)

  logger.warn(
    s"IvSellHedge *** 实盘 LIVE *** ${if conf.simulated then "模拟盘(simulated)" else "主网(mainnet)"} 配置=$confPath"
  )
  logger.warn(
    f"对冲: 判据=真实净敞口; 阈值 = |gamma|×现价×σ×√${t.hedgeHorizonMinutes}分 × " +
      f"(顺漂移侧 ${t.tightMult}%.2f / 反侧 ${t.looseMult}%.2f), " +
      f"clamp[${t.minTheta}%.3f, ${t.maxTheta}%.3f] ${t.ccy} (上限即敞口硬上界)%n" +
      f"σ来源=${t.sigmaSource} RV@${t.fastBar}×${t.rvBars}根; 方向=MACD@${t.macdBar}柱符号; " +
      f"ER@${t.fastBar}×${t.erPeriod}根(只选挂法) " +
      f"MACD@${t.macdBar} 单笔上限=${t.maxHedgeQty}%n" +
      f"报价: ER<${t.trendErThreshold}%.2f 平缓 -> 被动挂对手价外 ${t.passiveOffset * 100}%.3f%%, 给 ${t.passiveTtlMs}ms; " +
      f"ER>=${t.trendErThreshold}%.2f 单边 -> 跨价穿透 ${t.crossOffset * 100}%.3f%%, 只给 ${t.crossTtlMs}ms"
  )

  supervised:
    val backend = DefaultSyncBackend()
    val perp = OkxClient.trading(backend, credentials, quote = conf.quote) // 永续: 对冲腿下单/查仓
    val opt = OkxOptionsClient(backend, Some(credentials), quote = conf.quote, optionCcy = Some(t.ccy), simulated = conf.simulated)
    // 柜台在前、行情在后: 柜台既接下单指令也推回报 (消费者), 行情源是纯生产者。
    val gateway = RestTradingGateway.load(perp, OkxAccountFeed(perp, backend), AccountId.Live)
    Engine.run(plugins = Vector(gateway, OkxMarketFeed(perp, backend))) { engine =>

      // 预热的取数通道。策略按**自己序列**的粒度与长度来要 (它才知道指标窗口多长),
      // 这里只负责把毫秒换回 OKX 的粒度串 —— 粒度串是配置里的事实, 毫秒由它派生。
      // 什么时候预热、失败了怎么办, 都在 DeltaHedgeStrategy.prepare 里, 不再是本启动器的记性。
      val barLabel = Map(t.macdBarMs -> t.macdBar, t.fastBarMs -> t.fastBar)
      def klineHistory(barMs: Long, bars: Int): Either[String, Seq[(Double, Double, Double)]] =
        barLabel
          .get(barMs)
          .toRight(s"配置里没有 ${barMs}ms 对应的 OKX K 线粒度串 (只有 ${t.macdBar} 与 ${t.fastBar})")
          .flatMap(bar => opt.linearKlines(t.symbol, bar, bars))

      val hedge = DeltaHedgeStrategy(
        Exchange.Okx, t.symbol, t.ccy,
        band = t.deltaBand,
        fastBarMs = t.fastBarMs,
        erPeriodBars = t.erPeriod,
        rvBars = t.rvBars,
        sigmaSource = t.sigma,
        macdBarMs = t.macdBarMs,
        macdFastPeriod = t.macdFast,
        macdSlowPeriod = t.macdSlow,
        macdSignal = t.macdSignal,
        quotes = t.quotePolicy,
        cancelConfirmMs = t.cancelConfirmMs,
        minHedgeQty = Coin(t.minHedgeQty),
        maxHedgeQty = Coin(t.maxHedgeQty),
        maxExposureStaleMs = t.exposureStaleMs,
        history = klineHistory,
      )
      // addStrategy 会在策略开跑之前调用 hedge.prepare() 把两条序列喂热 (阻塞, 见 Strategy.prepare)
      engine.addStrategy(hedge, AccountId.Live) // 先订阅总线
      engine.install(OptionSellerActor(opt, Exchange.Okx, sellerCfg)) // 再开始发敞口读数

      logger.warn("运行中 (期权腿旁路 REST, 对冲腿走框架通道). Ctrl+C 退出")
      // 阻塞到停机: 中断信号或组件失败都会唤醒它, 停完全部组件 (onStop 逐个跑到) 核心最后退出
      engine.awaitShutdown()
    }
