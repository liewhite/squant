package strategy.strategies.ivsellhedge.live

import strategy.strategies.ivsellhedge.logic.DeltaKamaHedgeStrategy
import strategy.utils.hedge.DeltaBand
import strategy.utils.option.OkxOptionsClient

import hft.domain.{AccountId, Coin, Exchange}
import hft.engine.{Engine, ExchangeGateway}
import hft.exchange.okx.{OkxAccountStream, OkxClient, OkxCredentials, OkxMarketStream}
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
  *   永续持仓/回报 ─> OkxAccountStream ──────────↗
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
    f"对冲: 死区基准=${t.deltaThreshold}%.4f ${t.ccy} MACD逆势侧×${t.macdTightenRatio}%.2f " +
      f"KAMA(${t.kamaBucketMs / 1000}s桶, ER${t.kamaErPeriod}/${t.kamaFast}/${t.kamaSlow}) " +
      f"MACD@${t.macdBar} 挂单外移${t.offset * 100}%%/${t.requoteMs}ms重挂 单笔上限=${t.maxHedgeQty}"
  )

  supervised:
    val backend = DefaultSyncBackend()
    val perp = OkxClient.trading(backend, credentials, quote = conf.quote) // 永续: 对冲腿下单/查仓
    val opt = OkxOptionsClient(backend, Some(credentials), quote = conf.quote, optionCcy = Some(t.ccy), simulated = conf.simulated)
    val market = OkxMarketStream(perp, backend)
    val account = OkxAccountStream(perp, backend)

    val engine = Engine.start(gateways = Vector(ExchangeGateway.trading(perp, market, Some(account))))

    val hedge = DeltaKamaHedgeStrategy(
      Exchange.Okx, t.symbol, t.ccy,
      band = DeltaBand.macdTightened(Coin(t.deltaThreshold), t.macdTightenRatio),
      kamaBucketMs = t.kamaBucketMs,
      kamaErPeriod = t.kamaErPeriod,
      kamaFast = t.kamaFast,
      kamaSlow = t.kamaSlow,
      macdBarMs = t.macdBarMs,
      macdFastPeriod = t.macdFast,
      macdSlowPeriod = t.macdSlow,
      macdSignal = t.macdSignal,
      offsetPct = t.offset,
      requoteMs = t.requoteMs,
      minHedgeQty = Coin(t.minHedgeQty),
      maxHedgeQty = Coin(t.maxHedgeQty),
      maxExposureStaleMs = t.exposureStaleMs,
    )
    // MACD 预热: 不预热的话开机后数十根 bar 内 macdDirection 恒为 0, 死区退化为对称 —— 能跑,
    // 但"顺势侧收紧"这条规则在最需要它的启动期是缺席的。
    opt.linearKlines(t.symbol, t.macdBar, math.max(t.macdSlow + t.macdSignal + 8, 64)) match
      case Right(bars) => hedge.prewarm(bars); logger.warn(s"prewarm ${bars.size} 根 ${t.macdBar} K线 -> MACD 就绪")
      case Left(e)     => logger.error(s"prewarm 取 K 线失败 (MACD 将靠实时 BBO 慢热, 期间死区对称): $e")

    engine.addStrategy(hedge, AccountId.Live) // 先订阅总线
    engine.spawn(OptionSellerActor(opt, Exchange.Okx, sellerCfg)) // 再开始发敞口读数

    logger.warn("运行中 (期权腿旁路 REST, 对冲腿走框架通道). Ctrl+C 退出")
    Thread.sleep(Long.MaxValue)
