package strategy.crashchase.backtest

import hft.backtest.{BacktestEngine, BinanceHistoryDownloader, BinanceHistorySource, LocalFsDataCache, TradePrintBboSource}
import hft.domain.Exchange
import hft.engine.StrategyRunner
import hft.sim.{FillRecorder, SimConfig}
import strategy.crashchase.logic.CrashChaseStrategy
import sttp.client4.DefaultSyncBackend

import java.nio.file.Path
import java.time.LocalDate

/** 追暴跌做空策略回测入口 ([[CrashChaseStrategy]])。
  *
  * "监控 -> 识别突然暴跌 -> maker 逐档加空追跌 -> 继续下挫分批止盈 / 反弹止损"的确定性回测。
  * 策略代码与实盘/模拟盘完全相同，差异只在驱动层 (虚拟时间 vs 墙钟)。
  *
  * 数据：币安自 2024 年中起停发 futures 每日 bookTicker，较新合约 (如 SIRENUSDT) 只有 trades，
  * 故用 [[TradePrintBboSource]] 把成交印记合成零价差 L1 行情后再撮合。首跑联网下载并落 data-cache。
  *
  * 运行: sbt "runMain strategy.crashchase.backtest.CrashChaseBacktest [SYMBOL] [START yyyy-MM-dd] [END yyyy-MM-dd]"
  * 例:   sbt "runMain strategy.crashchase.backtest.CrashChaseBacktest SIRENUSDT 2026-06-04 2026-06-14"
  */
@main def CrashChaseBacktest(args: String*): Unit =
  System.setProperty("org.slf4j.simpleLogger.showDateTime", "true")
  System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "HH:mm:ss.SSS")
  // 成交量大，默认 INFO 会刷屏逐笔日志，回测时压到 WARN
  System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn")

  val symbol = args.lift(0).getOrElse("SIRENUSDT")
  val startDate = LocalDate.parse(args.lift(1).getOrElse("2026-06-04"))
  val endDate = LocalDate.parse(args.lift(2).getOrElse("2026-06-14"))

  val backend = DefaultSyncBackend()

  val publicClient = hft.exchange.binance.BinanceClient(backend, credentials = None)
  val symbolMetas = publicClient
    .fetchAllSymbolMetas()
    .fold(e => sys.error(s"fetch symbol metas failed: ${e.message}"), _.map(m => (m.exchange, m.symbol) -> m).toMap)

  val cache = LocalFsDataCache(Path.of("data-cache"))
  val downloader = BinanceHistoryDownloader(backend, cache)
  // SIREN 等较新合约无 bookTicker 历史，用成交印记合成 L1 行情。
  val source = TradePrintBboSource(BinanceHistorySource(downloader, Seq(symbol), startDate, endDate))

  val strategy = CrashChaseStrategy(
    targetExchange = Exchange.Binance,
    symbol = symbol,
    windowMs = 60_000,        // 60s 内的暴跌才算"突然"
    crashThreshold = 0.02,    // 相对 60s 峰值回撤 2% 触发追空
    entryOffsetRatio = 0.0005,
    rungUsdt = 500.0,         // 每档 500 USDT
    maxLeverage = 3.0,        // 最多加到 3x
    takeProfitRatio = 0.01,   // 再跌 1% 分批止盈
    stopLossRatio = 0.03,     // 反弹 3% 止损
    cooldownMs = 60_000,
  )
  val runner = StrategyRunner.backtest(strategy, symbolMetas)

  val stamp = LocalDate.now().toString
  val recorder = FillRecorder(Path.of(s"backtest-fills-crashchase-$symbol-$stamp.csv"))
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
    val days = java.time.temporal.ChronoUnit.DAYS.between(startDate, endDate) + 1
    val ret = (result.finalEquity / result.initialBalance - 1) * 100
    println("==================== CrashChase Backtest Result ====================")
    println(s"symbol         : $symbol  [$startDate .. $endDate] ($days days)")
    println(s"market events  : ${result.marketEvents}")
    println(s"fills          : ${result.fills}")
    println(f"realized PnL   : ${result.realizedPnl}%.4f USDT")
    println(f"final equity   : ${result.finalEquity}%.4f USDT  (init ${result.initialBalance}%.1f)")
    println(f"total return   : $ret%.2f%%")
    println(s"open positions : ${result.positions}")
    println(f"cumulative realizedPnl (recorder): ${recorder.cumulativeRealizedPnl}%.4f")
    println("====================================================================")
  finally recorder.close()
