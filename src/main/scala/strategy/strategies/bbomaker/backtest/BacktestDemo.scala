package strategy.strategies.bbomaker.backtest

import hft.backtest.{BacktestEngine, BinanceHistoryDownloader, BinanceHistorySource, LocalFsDataCache}
import hft.domain.Exchange
import hft.engine.StrategyRunner
import hft.sim.{FillRecorder, SimConfig}
import strategy.strategies.bbomaker.logic.BboMakerStrategy
import sttp.client4.DefaultSyncBackend

import java.nio.file.Path
import java.time.LocalDate
import java.time.format.DateTimeFormatter

/** 回测入口：用币安官方历史 bookTicker + trades 还原行情，对策略做确定性回测。
  *
  * 与实盘/模拟盘**完全相同的策略代码** (BboMakerStrategy) —— 差异只在驱动层 ([[BacktestEngine]]
  * 单线程虚拟时间 vs 实盘并发墙钟)。首跑联网下载历史数据并落 [[LocalFsDataCache]]，二跑命中缓存。
  *
  * 运行: sbt "runMain strategy.strategies.bbomaker.backtest.BacktestDemo [SYMBOL] [START yyyy-MM-dd] [END yyyy-MM-dd]"
  * 例:   sbt "runMain strategy.strategies.bbomaker.backtest.BacktestDemo BTCUSDT 2024-01-01 2024-01-01"
  */
@main def BacktestDemo(args: String*): Unit =
  System.setProperty("org.slf4j.simpleLogger.showDateTime", "true")
  System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "HH:mm:ss.SSS")

  val symbol = args.lift(0).getOrElse("BTCUSDT")
  val startDate = LocalDate.parse(args.lift(1).getOrElse("2024-01-01"))
  val endDate = LocalDate.parse(args.lift(2).getOrElse(startDate.toString))

  val backend = DefaultSyncBackend()

  // 真实 Binance (无凭证) 拉取 symbol 元数据 (精度)，与实盘一致
  val publicClient = hft.exchange.binance.BinanceClient(backend, credentials = None)
  val symbolMetas = publicClient
    .fetchAllSymbolMetas()
    .fold(e => sys.error(s"fetch symbol metas failed: ${e.message}"), _.map(m => (m.exchange, m.symbol) -> m).toMap)

  val cache = LocalFsDataCache(Path.of("data-cache"))
  val downloader = BinanceHistoryDownloader(backend, cache)
  val source = BinanceHistorySource(downloader, Seq(symbol), startDate, endDate)

  val strategy = BboMakerStrategy(
    targetExchange = Exchange.Binance,
    symbol = symbol,
    offsetRatio = 0.0003,
    orderSize = 0.01,
    maxLeverage = 2.0,
  )
  val runner = StrategyRunner.backtest(strategy, symbolMetas)

  // 旁路观察者: 成交写 CSV (与模拟盘同一份逻辑, 这里同步复用)
  val stamp = LocalDate.now().toString
  val recorder = FillRecorder(Path.of(s"backtest-fills-$symbol-$stamp.csv"))
  recorder.open()
  try
    val engine = BacktestEngine(
      exchange = Exchange.Binance,
      source = source,
      runners = Seq(runner),
      config = SimConfig(exchangeToStrategyDelayMs = 100, orderToExchangeDelayMs = 50, initialBalanceUsdt = 10_000.0),
      observers = Seq(recorder.onEvent),
    )
    val result = engine.run()
    println("==================== Backtest Result ====================")
    println(s"symbol         : $symbol  [$startDate .. $endDate]")
    println(s"market events  : ${result.marketEvents}")
    println(s"fills          : ${result.fills}")
    println(s"realized PnL   : ${result.realizedPnl}")
    println(s"final equity   : ${result.finalEquity}  (init ${result.initialBalance})")
    println(s"open positions : ${result.positions}")
    println(f"cumulative realizedPnl (recorder): ${recorder.cumulativeRealizedPnl}%.4f")
    println("=========================================================")
  finally recorder.close()
