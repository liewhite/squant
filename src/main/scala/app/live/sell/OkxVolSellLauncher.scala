package app.live.sell
import app.live.option.*

import hft.exchange.okx.OkxCredentials
import org.slf4j.LoggerFactory
import sttp.client4.DefaultSyncBackend

/** 卖方期权**实盘**启动器 (OKX)。与 [[VolSellLauncher]] (Bybit) 同一决策编排 [[SellRunner]]/[[VolSell]],
  * 仅交易所实现不同 (注入 [[OkxOptionsClient]])——策略逻辑零改动, 体现 [[OptionsExchange]] 抽象。
  * **无 dry-run, 启动即真实下单**。
  *
  * **配置**: 全部参数 (含 API 密钥+passphrase) 走 JSON 文件 [[OkxSellConfig]], 路径=首个命令行参数
  * (默认 `conf/vol-sell-okx.json`)。模板见 `conf/vol-sell-okx.example.json`。**密钥明文 -> chmod 600 且勿入库**。
  *
  * **OKX 符号约定**: tuning.symbol 用基础币 (ETH), 内部拼 `ETH-<quote>-SWAP` 取永续 K 线; 期权为币本位
  * (instFamily=ETH-USD)。`simulated=true` 走 OKX 模拟盘。
  *
  * 运行: sbt "runMain app.live.sell.OkxVolSellLauncher [conf/vol-sell-okx.json]"
  */
@main def OkxVolSellLauncher(args: String*): Unit =
  val logger = LoggerFactory.getLogger("OkxVolSellLauncher")
  val confPath = args.headOption.getOrElse("conf/vol-sell-okx.json")
  val conf = SellConfig.loadOkx(confPath) match
    case Right(c) => c
    case Left(e)  => logger.error(s"加载配置失败 ($confPath): $e"); sys.exit(1)
  if conf.apiKey.isEmpty || conf.apiSecret.isEmpty || conf.passphrase.isEmpty then
    logger.error(s"配置 $confPath 缺 apiKey/apiSecret/passphrase, 实盘下单需签名, 退出"); sys.exit(1)
  val cfg = conf.tuning.toVolSellConfig

  val backend = DefaultSyncBackend()
  val ex: OptionsExchange =
    OkxOptionsClient(backend, Some(OkxCredentials(conf.apiKey, conf.apiSecret, conf.passphrase)), quote = conf.quote, optionCcy = Some(conf.tuning.baseCoin), simulated = conf.simulated)

  logger.warn(s"OkxVolSell *** 实盘 LIVE *** (无 dry-run, 真实下单): $cfg")
  logger.warn(s"配置=$confPath  ${if conf.simulated then "模拟盘(simulated)" else "主网(mainnet)"}  单腿硬上限=${cfg.maxQty}")
  SellRunner.run(ex, cfg, conf.tuning.runNow)
