package strategy.bbomaker.live

import hft.domain.Exchange
import hft.engine.{Engine, ExchangeGateway}
import hft.exchange.binance.{BinanceClient, BinanceMarketStream}
import hft.sim.{FillRecorder, SimConfig, SimulatedExchange}
import strategy.bbomaker.logic.BboMakerStrategy
import ox.supervised
import sttp.client4.DefaultSyncBackend

import java.nio.file.Path
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/** 模拟盘 (纸面交易) 入口：接入 **真实** Binance 公共行情，用虚拟柜台撮合，无需任何凭证。
  *
  * 同一个 [[SimulatedExchange]] 同时充当 REST 客户端 / 公共行情流 / 私有账户流，
  * 因此 [[BboMakerStrategy]] 与跑实盘时是完全相同的代码，对"真实还是模拟"无感知。
  *
  * 延迟被如实建模：撮合用上游实时行情，策略看到的是延迟行情；下单亦有在途延迟。
  *
  * 运行: sbt "runMain strategy.bbomaker.live.SimMakerDemo"
  */
@main def SimMakerDemo(): Unit =
  System.setProperty("org.slf4j.simpleLogger.showDateTime", "true")
  System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "HH:mm:ss.SSS")

  supervised:
    val backend = DefaultSyncBackend()
    // 公共行情与 symbol 元数据走真实 Binance (无凭证)；账户侧全部由虚拟柜台模拟
    val publicClient = BinanceClient(backend, credentials = None)
    val realMarket = BinanceMarketStream(backend)
    val sim = SimulatedExchange(
      market = realMarket,
      publicClient = publicClient,
      config = SimConfig(
        exchangeToStrategyDelayMs = 100,
        orderToExchangeDelayMs = 50,
        initialBalanceUsdt = 10_000.0,
      ),
    )

    // client / marketData / accountStream 同指虚拟柜台 -> 策略无感知
    val engine = Engine.start(
      gateways = Vector(ExchangeGateway(client = sim, marketData = sim, accountStream = Some(sim))),
      dryRun = false, // 虚拟柜台即沙箱, 真实"下单"到模拟撮合
    )

    // 策略之外的旁路观察者: 订阅成交事件写 CSV + 累计已实现利润 (须在成交产生前订阅)。
    // 文件名带启动时间戳, 每次运行独占一个文件, 避免跨运行累计列重置/并发追加交错
    val stamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"))
    FillRecorder(Path.of(s"sim-fills-$stamp.csv")).run(engine.subscribeIncome())

    engine.addStrategy(
      BboMakerStrategy(
        targetExchange = Exchange.Binance,
        symbol = "COAIUSDT",
        offsetRatio = 0.0003, // bbo 外 0.01%
        orderSize = 10,
        maxLeverage = 2.0,
      )
    )

    Thread.sleep(Long.MaxValue)
