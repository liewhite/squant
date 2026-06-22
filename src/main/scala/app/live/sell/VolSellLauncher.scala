package app.live.sell
import app.live.option.*

import hft.exchange.bybit.BybitCredentials
import org.slf4j.LoggerFactory
import sttp.client4.DefaultSyncBackend

/** 卖方期权**实盘**启动器 (Bybit)。**无 dry-run, 启动即真实下单** —— 用小资金测试。
  *
  * 每**北京时间周五 15:00** 决策一次 (调度/下单见 [[SellRunner]]): 取标的最近 2 周 5min K 线算 RV →
  * 本周较上周升则卖 gridHigh×、降则 gridLow× → 选 ~targetDays 到期 ATM 跨式 → 最优卖价 PostOnly 卖 call+put。
  *
  * **配置**: 全部参数 (含 API 密钥) 走 JSON 文件 [[BybitSellConfig]], 路径=首个命令行参数
  * (默认 `conf/vol-sell-bybit.json`)。模板见 `conf/vol-sell-bybit.example.json`。**密钥明文 -> chmod 600 且勿入库**。
  * 护栏: 必须有 key; baseQty 小仓; maxQty 单腿硬上限; 幂等 clOrdId + 当周防重。
  *
  * 运行: sbt "runMain app.live.sell.VolSellLauncher [conf/vol-sell-bybit.json]"
  */
@main def VolSellLauncher(args: String*): Unit =
  val logger = LoggerFactory.getLogger("VolSellLauncher")
  val confPath = args.headOption.getOrElse("conf/vol-sell-bybit.json")
  val conf = SellConfig.loadBybit(confPath) match
    case Right(c) => c
    case Left(e)  => logger.error(s"加载配置失败 ($confPath): $e"); sys.exit(1)
  if conf.apiKey.isEmpty || conf.apiSecret.isEmpty then
    logger.error(s"配置 $confPath 缺 apiKey/apiSecret, 实盘下单需签名, 退出"); sys.exit(1)
  val cfg = conf.tuning.toVolSellConfig

  val backend = DefaultSyncBackend()
  val ex: OptionsExchange = BybitOptionsClient(backend, Some(BybitCredentials(conf.apiKey, conf.apiSecret)), testnet = conf.testnet)

  logger.warn(s"VolSell *** 实盘 LIVE *** (无 dry-run, 真实下单): $cfg")
  logger.warn(s"配置=$confPath  ${if conf.testnet then "testnet" else "mainnet"}  单腿硬上限=${cfg.maxQty}")
  SellRunner.run(ex, cfg, conf.tuning.runNow)
