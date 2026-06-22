package app.live.demo

import hft.engine.{Engine, ExchangeGateway}
import hft.exchange.bybit.{BybitAccountStream, BybitClient, BybitCredentials, BybitMarketStream}
import ox.supervised
import sttp.client4.DefaultSyncBackend
import strategy.research.FundingWatchStrategy

/** Bybit v5 USDT linear 永续接入演示入口。
  *
  * 以 dry-run 模式接入 Bybit 公开行情 (无需 API key)：
  *   - WS 订阅 BTCUSDT/ETHUSDT 的 orderbook.1 (BBO) 与 tickers (markPrice/fundingRate) 流
  *   - FundingWatchStrategy 在日化资金费率超阈值时产出下单信号
  *   - dry-run 下信号只打日志不下单，并以 OrderUpdate(Error) 回流清理 pending，可观察完整闭环
  *
  * Fail-fast: 框架不做任何错误恢复 (包括 WS 重连)，任何异常都终止进程，由外层重新拉起，
  * 重启后的启动对齐保证状态正确。私有/公共连接均每 20s 发应用层 ping 维持。
  *
  * 配置环境变量 BYBIT_API_KEY / BYBIT_API_SECRET 可接入私有流与真实下单 (此时应去掉 dryRun)。
  *
  * 运行: sbt "runMain app.live.BybitDemo"
  */
@main def BybitDemo(): Unit =
  System.setProperty("org.slf4j.simpleLogger.showDateTime", "true")
  System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "HH:mm:ss.SSS")

  val credentials =
    for
      key <- sys.env.get("BYBIT_API_KEY")
      secret <- sys.env.get("BYBIT_API_SECRET")
    yield BybitCredentials(key, secret)

  supervised:
    val backend = DefaultSyncBackend()
    val client = BybitClient(backend, credentials)
    val market = BybitMarketStream(backend)
    val account = if client.hasCredentials then Some(BybitAccountStream(client, backend)) else None

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
