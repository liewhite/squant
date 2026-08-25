package strategy.strategies.macdgrid.backtest

import hft.backtest.binance.BinanceMarketDataProvider
import hft.backtest.{BacktestEngine, MarketDataKind, SyntheticBboSource}
import hft.engine.StrategyRunner
import hft.sim.SimConfig
import strategy.strategies.macdgrid.logic.MacdGridStrategy
import strategy.utils.backtest.{BacktestRecorder, BacktestReport}
import sttp.client4.DefaultSyncBackend

import java.time.{LocalDate, ZoneOffset}

/** MACD 网格策略回测 —— DEA 定向 / 柱定加减仓 / MA20-ATR 定超买超卖的滚动单份括号网格。
  *
  * 默认跑 2026-05 ETHUSDT。预热 [[warmupDays]] 让 1h MACD/MA20/ATR 充分就绪。加仓/止盈为 maker 限价 (GTC),
  * DEA 转向平仓为 taker 市价。出图/落盘走统一框架 [[strategy.utils.backtest.BacktestReport]] (净值/仓位/买卖点)。
  *
  * 运行: sbt "runMain strategy.strategies.macdgrid.backtest.MacdGridBacktest [SYMBOL] [startYYYY-MM-DD] [endYYYY-MM-DD]"
  */
@main def MacdGridBacktest(args: String*): Unit =
  System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn")

  val symbol = args.lift(0).getOrElse("ETHUSDT")
  val start = args.lift(1).map(LocalDate.parse).getOrElse(LocalDate.parse("2026-05-01"))
  val end = args.lift(2).map(LocalDate.parse).getOrElse(LocalDate.parse("2026-05-31"))
  val initBalance = 100_000.0
  val makerFee = 0.0002
  val takerFee = 0.0005
  val warmupDays = 21L
  val outDir = sys.env.getOrElse("OUT_DIR", "/tmp/macdgrid")
  java.nio.file.Files.createDirectories(java.nio.file.Path.of(outDir))
  val label = s"macdgrid_${start}_${end}"

  val backend = DefaultSyncBackend()
  val publicClient = hft.exchange.binance.BinanceClient(backend, credentials = None)
  val symbolMetas = publicClient
    .fetchAllSymbolMetas()
    .fold(e => sys.error(s"fetch symbol metas failed: ${e.message}"), _.map(m => (m.exchange, m.symbol) -> m).toMap)

  def epochMs(d: LocalDate): Long = d.atStartOfDay(ZoneOffset.UTC).toInstant.toEpochMilli
  val startMs = epochMs(start)

  // 币安 bookTicker 历史止于 2024-03-30, 本回测区间只有 trades -> 显式合成零价差盘口供
  // taker 对冲单取对手价 (低估真实点差成本, 见 SyntheticBboSource)
  val provider = BinanceMarketDataProvider(backend)
  val source = SyntheticBboSource(
    provider.source(Seq(symbol), start.minusDays(warmupDays), end, Set(MarketDataKind.Trades))
  )
  val strategy = MacdGridStrategy(exchange = provider.exchange, symbol = symbol, leverage = 1.0, referenceEquity = initBalance)
  val runner = StrategyRunner.backtest(strategy, symbolMetas)

  val rec = BacktestRecorder(exchange = provider.exchange, symbol = symbol, startMs = startMs, initialBalance = initBalance)

  val engine = BacktestEngine(
    exchange = provider.exchange, source = source, runners = Seq(runner),
    config = SimConfig(
      exchangeToStrategyDelayMs = 100, orderToExchangeDelayMs = 50,
      initialBalanceUsdt = initBalance, makerFeeRate = makerFee, takerFeeRate = takerFee,
    ),
    symbolMetas = symbolMetas,
    observers = Seq(rec.observe),
  )
  val r = try engine.run() finally backend.close()

  val out = BacktestReport.write(
    outDir = outDir,
    label = label,
    title = s"MacdGrid 网格 (DEA定向/柱加减/MA20超买超卖) — $symbol  $start .. $end",
    symbol = symbol,
    rec = rec,
    finalEquity = r.finalEquity,
    extraStats = Seq("区间" -> s"$start .. $end", "maker费" -> f"${makerFee * 100}%.2f%%", "taker费" -> f"${takerFee * 100}%.2f%%"),
  )

  println("==================== MacdGrid 网格回测 ====================")
  println(f"symbol=$symbol  $start..$end  warmup=${warmupDays}d  capital=$initBalance  maker=${makerFee * 100}%.2f%% taker=${takerFee * 100}%.2f%%")
  println(out.summaryLine)
  println(f"末净值=${r.finalEquity}%.2f  (起点基准=${out.baseEquity}%.2f)")
  println(s"净值曲线网页 -> ${out.htmlPath}")
  println(s"净值数据     -> ${out.equityCsv}")
  println(s"成交记录     -> ${out.fillsCsv}")
  println("==========================================================")
