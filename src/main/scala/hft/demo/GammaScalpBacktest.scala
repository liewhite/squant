package hft.demo

import hft.backtest.{BacktestEngine, BinanceHistoryDownloader, BinanceHistorySource, BsGreeksConfig, BsGreeksSource, LocalFsDataCache, TradePrintBboSource}
import hft.domain.Exchange
import hft.engine.StrategyRunner
import hft.messaging.{EventData, IncomeEvent}
import hft.option.{OptionPosition, OptionRight, OptionSpec}
import hft.sim.{FillRecorder, SimConfig}
import hft.strategy.GammaScalpStrategy
import sttp.client4.DefaultSyncBackend

import java.nio.file.Path
import java.time.{LocalDate, ZoneOffset}

/** 纯 gamma scalping 回测入口 ([[GammaScalpStrategy]])。
  *
  * 数据：Binance ETHUSDT 永续历史 trades (2024 年中后无 bookTicker，用 [[TradePrintBboSource]]
  * 合成零价差 L1)。期权希腊字母为 **mock**：用 [[BsGreeksSource]] 以 Black-Scholes 合成一个
  * **长 ATM 跨式** (N 个 call + N 个 put，行权价取回测起点价) 的账户级 greeks，喂入与实盘 OKX
  * 同一 Greeks 通道——策略代码与实盘完全相同。
  *
  * 撮合按 **maker 手续费** 计 (对冲单全为 PostOnly)。回测 P&L = 永续对冲的 maker scalp 净收益
  * (毛 gamma 收益 - 手续费)；期权 theta/权利金为已知结构性成本，不在永续 P&L 内建模。
  *
  * 运行: sbt "runMain hft.demo.GammaScalpBacktest [START yyyy-MM-dd] [END yyyy-MM-dd]"
  * 缺省回测最近一周 (end = 今天-2, 留出 Binance Vision 数据上架延迟)。
  */
@main def GammaScalpBacktest(args: String*): Unit =
  System.setProperty("org.slf4j.simpleLogger.showDateTime", "true")
  System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "HH:mm:ss.SSS")
  System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn") // 成交量大，压到 WARN 防刷屏

  // ==================== 配置 ====================
  val symbol = "ETHUSDT"
  val ccy = "ETH"
  val impliedVol = 0.6        // 年化 IV (ETH 典型)
  val riskFreeRate = 0.0
  val expiryDays = 30L        // 期权到期 (远于回测窗口，使 greeks 稳定、gamma 不在到期日爆炸)
  val straddles = 10.0        // 长跨式张数 (N 个 call + N 个 put)
  val deltaBand = 0.1         // 净 delta 对称容忍带 (ETH)
  val offsetRatio = 0.0002    // PostOnly 距 BBO 偏移
  val makerFeeRate = 0.0002   // maker 手续费 0.02% (gamma scalp 的核心成本)
  val takerFeeRate = 0.0005
  val initialBalanceUsdt = 100_000.0

  val end = args.lift(1).map(LocalDate.parse).getOrElse(LocalDate.now().minusDays(2))
  val start = args.lift(0).map(LocalDate.parse).getOrElse(end.minusDays(6))

  val backend = DefaultSyncBackend()
  val publicClient = hft.exchange.binance.BinanceClient(backend, credentials = None)
  val symbolMetas = publicClient
    .fetchAllSymbolMetas()
    .fold(e => sys.error(s"fetch symbol metas failed: ${e.message}"), _.map(m => (m.exchange, m.symbol) -> m).toMap)

  val cache = LocalFsDataCache(Path.of("data-cache"))
  val downloader = BinanceHistoryDownloader(backend, cache)
  def tradeSource() = TradePrintBboSource(BinanceHistorySource(downloader, Seq(symbol), start, end))

  // 以回测起点首个成交价作为 ATM 行权价 (peek 仅加载首日)
  val atmStrike = tradeSource()
    .events()
    .collectFirst { case IncomeEvent(_, _, EventData.BboUpdate(b)) => b.midPrice }
    .getOrElse(sys.error(s"no market data for $symbol [$start .. $end] (数据未上架或日期无效?)"))

  val expiryMs = start.plusDays(expiryDays).atStartOfDay.toInstant(ZoneOffset.UTC).toEpochMilli
  // 长 ATM 跨式：long call + long put，近似 delta 中性、纯 gamma
  val positions = Vector(
    OptionPosition(OptionSpec(OptionRight.Call, atmStrike, expiryMs), straddles),
    OptionPosition(OptionSpec(OptionRight.Put, atmStrike, expiryMs), straddles),
  )
  val greeksConfig = BsGreeksConfig(
    exchange = Exchange.Binance,
    ccy = ccy,
    underlyingSymbol = symbol,
    positions = positions,
    impliedVol = impliedVol,
    riskFreeRate = riskFreeRate,
    spotHolding = 0.0,        // 纯期权 + 永续对冲，无现货
    emitIntervalMs = 1000,    // 对齐实盘 OKX greeks 轮询节奏
  )
  val source = BsGreeksSource(tradeSource(), greeksConfig)

  val strategy = GammaScalpStrategy(
    exchange = Exchange.Binance,
    symbol = symbol,
    ccy = ccy,
    deltaBand = deltaBand,
    offsetRatio = offsetRatio,
  )
  val runner = StrategyRunner(strategy, symbolMetas)

  val stamp = LocalDate.now().toString
  val recorder = FillRecorder(Path.of(s"backtest-fills-gammascalp-$symbol-$stamp.csv"))
  recorder.open()
  try
    val engine = BacktestEngine(
      exchange = Exchange.Binance,
      source = source,
      runners = Seq(runner),
      config = SimConfig(
        exchangeToStrategyDelayMs = 100,
        orderToExchangeDelayMs = 50,
        initialBalanceUsdt = initialBalanceUsdt,
        makerFeeRate = makerFeeRate,
        takerFeeRate = takerFeeRate,
      ),
      observers = Seq(recorder.onEvent),
    )
    val result = engine.run()
    val days = java.time.temporal.ChronoUnit.DAYS.between(start, end) + 1
    val ret = (result.finalEquity / result.initialBalance - 1) * 100
    println("==================== GammaScalp Backtest Result ====================")
    println(s"symbol         : $symbol  [$start .. $end] ($days days)")
    println(f"ATM strike     : $atmStrike%.2f  | IV=$impliedVol  straddles=$straddles  band=$deltaBand ETH")
    println(f"maker fee      : ${makerFeeRate * 100}%.3f%%  (PostOnly 对冲单)")
    println(s"market events  : ${result.marketEvents}")
    println(s"fills          : ${result.fills}")
    println(f"realized PnL   : ${result.realizedPnl}%.4f USDT  (= 永续 scalp 净收益, 已扣手续费)")
    println(f"final equity   : ${result.finalEquity}%.4f USDT  (init ${result.initialBalance}%.1f, 含未实现)")
    println(f"total return   : $ret%.3f%%")
    println(s"open positions : ${result.positions}")
    println(f"cumulative realizedPnl (recorder): ${recorder.cumulativeRealizedPnl}%.4f")
    println("====================================================================")
  finally recorder.close()
