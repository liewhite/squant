package hft.demo

import hft.engine.{Engine, ExchangeGateway}
import hft.exchange.binance.{BinanceClient, BinanceConnector, BinanceCredentials}
import ox.supervised
import sttp.client4.DefaultSyncBackend

/** HFT 框架演示入口。
  *
  * 以 dry-run 模式接入 Binance 公开行情 (无需 API key)：
  *   - WS 订阅 BTCUSDT/ETHUSDT 的 bookTicker 与 markPrice 流
  *   - FundingWatchStrategy 在日化资金费率超阈值时产出下单信号
  *   - dry-run 下信号只打日志不下单；pending 订单 5 秒无确认被时钟清理，可观察完整闭环
  *
  * 配置环境变量 BINANCE_API_KEY / BINANCE_API_SECRET 可接入私有流与真实下单
  * (此时应去掉 dryRun)。
  *
  * 运行: sbt "runMain hft.demo.HftDemo"
  */
@main def HftDemo(): Unit =
  System.setProperty("org.slf4j.simpleLogger.showDateTime", "true")
  System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "HH:mm:ss.SSS")

  val credentials =
    for
      key <- sys.env.get("BINANCE_API_KEY")
      secret <- sys.env.get("BINANCE_API_SECRET")
    yield BinanceCredentials(key, secret)

  supervised:
    val backend = DefaultSyncBackend()
    val client = BinanceClient(backend, credentials)
    val connector = BinanceConnector(client, backend)

    val engine = Engine.start(
      gateways = Vector(ExchangeGateway(client, connector)),
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
