package strategy.strategies.fundingwatch.live
import strategy.utils.DemoConfig

import hft.engine.{Engine, ExchangeGateway}
import hft.exchange.binance.{BinanceAccountStream, BinanceClient, BinanceCredentials, BinanceMarketStream}
import ox.supervised
import sttp.client4.DefaultSyncBackend
import strategy.strategies.fundingwatch.logic.FundingWatchStrategy

/** HFT 框架演示入口。
  *
  * 以 dry-run 模式接入 Binance 公开行情 (无需 API key)：
  *   - WS 订阅 BTCUSDT/ETHUSDT 的 bookTicker 与 markPrice 流
  *   - FundingWatchStrategy 在日化资金费率超阈值时产出下单信号
  *   - dry-run 下信号只打日志不下单，并以 OrderUpdate(Error) 回流清理 pending，可观察完整闭环
  *
  * Fail-fast: 框架不做任何错误恢复 (包括 WS 重连)，任何异常都终止进程，
  * 由外层 (systemd/k8s) 重新拉起，重启后的启动对齐保证状态正确。
  *
  * 可选 JSON 配置 (apiKey/apiSecret) 接入私有流 (路径=首个命令行参数, 默认 `conf/demo-binance.json`;
  * 缺省则纯公共行情)。模板见 `conf/demo.example.json`。本 demo 恒 dry-run。
  *
  * 运行: sbt "runMain strategy.strategies.fundingwatch.live.HftDemo [conf/demo-binance.json]"
  */
@main def HftDemo(args: String*): Unit =
  System.setProperty("org.slf4j.simpleLogger.showDateTime", "true")
  System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "HH:mm:ss.SSS")

  val conf = DemoConfig.loadOrEmpty(args.headOption.getOrElse("conf/demo-binance.json"))
  val credentials = if conf.hasCreds then Some(BinanceCredentials(conf.apiKey, conf.apiSecret)) else None

  supervised:
    val backend = DefaultSyncBackend()
    val client = BinanceClient(backend, credentials)
    val market = BinanceMarketStream(backend)
    val account = if client.hasCredentials then Some(BinanceAccountStream(client, backend)) else None

    val engine = Engine.start(
      gateways = Vector(ExchangeGateway(client, market, account)),
      dryRun = true,
    )

    engine.addStrategy(
      FundingWatchStrategy(
        symbols = Vector("BTCUSDT", "ETHUSDT"),
        dailyRateThreshold = 0.00005, // 日化 0.005%，阈值刻意调低以便演示触发下单闭环
        orderQty = Map("BTCUSDT" -> 0.002, "ETHUSDT" -> 0.01),
      )
    )

    // 常驻运行，Ctrl+C 退出
    Thread.sleep(Long.MaxValue)
