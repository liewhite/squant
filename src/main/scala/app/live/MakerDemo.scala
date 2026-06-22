package app.live

import hft.domain.Exchange
import hft.engine.{Engine, ExchangeGateway}
import hft.exchange.binance.{BinanceAccountStream, BinanceClient, BinanceCredentials, BinanceMarketStream}
import strategy.research.BboMakerStrategy
import ox.supervised
import sttp.client4.DefaultSyncBackend

/** BBO 做市策略入口: BTCUSDT 双边 BBO 外 0.01% 挂单 0.002 BTC，杠杆率上限 2。
  *
  * 需要环境变量 BINANCE_API_KEY / BINANCE_API_SECRET (策略依赖账户净值与私有流订单回报)。
  * 默认 dry-run；显式设置 LIVE=1 才会真实下单。
  *
  * 运行: BINANCE_API_KEY=.. BINANCE_API_SECRET=.. sbt "runMain app.live.MakerDemo"
  */
@main def MakerDemo(): Unit =
  System.setProperty("org.slf4j.simpleLogger.showDateTime", "true")
  System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "HH:mm:ss.SSS")

  val credentials =
    (for
      key <- sys.env.get("BINANCE_API_KEY")
      secret <- sys.env.get("BINANCE_API_SECRET")
    yield BinanceCredentials(key, secret))
      .getOrElse(throw IllegalStateException("MakerDemo requires BINANCE_API_KEY / BINANCE_API_SECRET"))

  val live = sys.env.get("LIVE").contains("1")

  supervised:
    val backend = DefaultSyncBackend()
    val client = BinanceClient(backend, Some(credentials))
    val market = BinanceMarketStream(backend)
    val account = BinanceAccountStream(client, backend)

    val engine = Engine.start(
      gateways = Vector(ExchangeGateway(client, market, Some(account))),
      dryRun = !live,
    )

    engine.addStrategy(
      BboMakerStrategy(
        targetExchange = Exchange.Binance,
        symbol = "BTCUSDT",
        offsetRatio = 0.0001, // bbo 外 0.01%
        orderSize = 0.002,
        maxLeverage = 2.0,
      )
    )

    Thread.sleep(Long.MaxValue)
