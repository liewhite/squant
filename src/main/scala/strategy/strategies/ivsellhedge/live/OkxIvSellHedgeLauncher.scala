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
  *   永续 BBO ─> 引擎行情流 ──────────────────────> DeltaKamaHedgeStrategy ─> 框架下单通道 ─> 永续
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
    val engine = Engine.start(plugins = Vector(gateway, OkxMarketFeed(perp, backend)))

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
    )
    // 两条序列都用历史 K 线预热, 开机即就绪:
    //   MACD 不预热 -> 数十根 bar 内方向恒为 0, 死区退化为对称 (那条规则在启动期缺席);
    //   σ 不预热 -> 阈值取下限 (对冲偏频但安全); ER 不预热 -> 报价按单边处理 (更贵)。
    opt.linearKlines(t.symbol, t.macdBar, math.max(t.macdSlow + t.macdSignal + 8, 64)) match
      case Right(bars) => hedge.prewarmMacd(bars); logger.warn(s"prewarm ${bars.size} 根 ${t.macdBar} K线 -> MACD 就绪")
      case Left(e)     => logger.error(s"prewarm 取 K 线失败 (MACD 将靠实时 BBO 慢热, 期间死区对称): $e")
    opt.linearKlines(t.symbol, t.fastBar, math.max(math.max(t.erPeriod, t.rvBars) * 4, 64)) match
      case Right(bars) => hedge.prewarmFast(bars); logger.warn(s"prewarm ${bars.size} 根 ${t.fastBar} K线 -> σ 与 ER 就绪")
      case Left(e)     => logger.error(s"prewarm 取 K 线失败 (σ 未就绪期间阈值取下限 ${t.minTheta}, 对冲偏频): $e")

    engine.addStrategy(hedge, AccountId.Live) // 先订阅总线
    engine.install(OptionSellerActor(opt, Exchange.Okx, sellerCfg)) // 再开始发敞口读数

    logger.warn("运行中 (期权腿旁路 REST, 对冲腿走框架通道). Ctrl+C 退出")
    Thread.sleep(Long.MaxValue)
