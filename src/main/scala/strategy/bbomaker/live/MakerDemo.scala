package strategy.bbomaker.live
import strategy.utils.DemoConfig

import hft.domain.Exchange
import hft.engine.{Engine, ExchangeGateway}
import hft.exchange.binance.{BinanceAccountStream, BinanceClient, BinanceCredentials, BinanceMarketStream}
import strategy.bbomaker.logic.BboMakerStrategy
import ox.supervised
import sttp.client4.DefaultSyncBackend

/** BBO 做市策略入口: BTCUSDT 双边 BBO 外 0.01% 挂单 0.002 BTC，杠杆率上限 2。
  *
  * JSON 配置 (apiKey/apiSecret 必填, 策略依赖账户净值与私有流订单回报; live 开关) 路径=首个命令行参数
  * (默认 `conf/demo-maker.json`)。模板见 `conf/demo.example.json`。默认 dry-run; 配置 live=true 才真实下单。
  *
  * 运行: sbt "runMain strategy.bbomaker.live.MakerDemo [conf/demo-maker.json]"
  */
@main def MakerDemo(args: String*): Unit =
  System.setProperty("org.slf4j.simpleLogger.showDateTime", "true")
  System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "HH:mm:ss.SSS")

  val conf = DemoConfig.loadOrEmpty(args.headOption.getOrElse("conf/demo-maker.json"))
  if !conf.hasCreds then throw IllegalStateException("MakerDemo 需在配置文件提供 apiKey/apiSecret (依赖账户净值与私有流)")
  val credentials = BinanceCredentials(conf.apiKey, conf.apiSecret)
  val live = conf.live

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
