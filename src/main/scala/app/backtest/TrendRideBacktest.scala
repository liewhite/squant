package app.backtest

import hft.backtest.{BacktestEngine, BinanceDataKind, BinanceHistory, TradeBboAugmentSource}
import hft.domain.{Exchange, Side}
import hft.engine.StrategyRunner
import hft.messaging.{EventData, IncomeEvent}
import hft.sim.SimConfig
import strategy.research.TrendRideStrategy
import sttp.client4.DefaultSyncBackend

import java.time.{LocalDate, ZoneOffset}

/** 趋势骑乘策略回测 —— 对比震荡期与趋势期表现，落原始数据 (净值/成交/仓位) 供画图与分析。
  *
  * 每个周期：往前预热 [[warmupDays]] 天 (让最长 drift 周期 + 波动 EWMA 预热)，正式区间起点重置基准净值；
  * 用 [[TradeBboAugmentSource]] 在 trade 后补零价差 BBO，使 taker (市价) 强平/反手可成交 (零价差低估点差成本)。
  *
  * 输出 (默认 /tmp/trendride)：
  *   - {label}_equity.csv : ts,equity,price,pos  (小时采样, 仅正式区间)
  *   - {label}_fills.csv   : ts,side,price,qty    (全部成交)
  *
  * 运行: sbt "runMain app.backtest.TrendRideBacktest [SYMBOL]"
  */
@main def TrendRideBacktest(args: String*): Unit =
  System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn")

  val symbol = args.lift(0).getOrElse("ETHUSDT")
  val initBalance = 100_000.0
  val makerFee = 0.0002
  val takerFee = 0.0005
  val warmupDays = 8L
  val outDir = sys.env.getOrElse("OUT_DIR", "/tmp/trendride")
  java.nio.file.Files.createDirectories(java.nio.file.Path.of(outDir))

  final case class Period(label: String, start: LocalDate, end: LocalDate)
  val periods = Seq(
    Period("chop", LocalDate.parse("2026-02-16"), LocalDate.parse("2026-05-25")),
    Period("trend", LocalDate.parse("2025-10-06"), LocalDate.parse("2025-11-17")),
  )

  val backend = DefaultSyncBackend()
  val publicClient = hft.exchange.binance.BinanceClient(backend, credentials = None)
  val symbolMetas = publicClient
    .fetchAllSymbolMetas()
    .fold(e => sys.error(s"fetch symbol metas failed: ${e.message}"), _.map(m => (m.exchange, m.symbol) -> m).toMap)

  def epochMs(d: LocalDate): Long = d.atStartOfDay(ZoneOffset.UTC).toInstant.toEpochMilli

  final case class Stat(
      label: String,
      fills: Int,
      baseEquity: Double, // 正式区间起点净值 (重置基准)
      finalEquity: Double,
      retPct: Double,
      bhRetPct: Double,    // buy&hold 同期
      maxDrawdownPct: Double,
      peakPosCoin: Double,
      avgAbsPosCoin: Double,
  )

  def runOne(period: Period): Stat =
    val startMs = epochMs(period.start)
    val source = TradeBboAugmentSource(
      BinanceHistory.source(backend, Seq(symbol), period.start.minusDays(warmupDays), period.end, kinds = Seq(BinanceDataKind.Trades))
    )
    val strategy = TrendRideStrategy(exchange = Exchange.Binance, symbol = symbol)
    val runner = StrategyRunner.backtest(strategy, symbolMetas)

    val equityWriter = java.io.PrintWriter(s"$outDir/${period.label}_equity.csv")
    equityWriter.println("ts,equity,price,pos")
    val fillsWriter = java.io.PrintWriter(s"$outDir/${period.label}_fills.csv")
    fillsWriter.println("ts,side,price,qty")

    var lastPx = 0.0
    var runPos = 0.0
    var baseEquity = 0.0     // 正式区间首个采样净值
    var baseSet = false
    var firstPxInPeriod = 0.0 // buy&hold 基准 (正式区间首个成交价)
    var lastSampleTs = 0L
    var peakEquity = 0.0
    var maxDd = 0.0
    var posAbsSum = 0.0
    var posSamples = 0L
    var peakPos = 0.0
    val sampleIntervalMs = 3_600_000L

    val obs: IncomeEvent => Unit = ev =>
      ev.data match
        case EventData.MarketTradeUpdate(t) =>
          lastPx = t.price
          if t.timestamp >= startMs && firstPxInPeriod == 0.0 then firstPxInPeriod = t.price
        case EventData.FillUpdate(f) =>
          runPos += (if f.side == Side.Long then f.size else -f.size)
          if f.timestamp >= startMs then
            fillsWriter.println(s"${f.timestamp},${f.side},${f.price},${f.size}")
        case EventData.AccountInfoUpdate(_, info) =>
          if ev.exchangeTs >= startMs && info.equity > 0 then
            if !baseSet then
              baseEquity = info.equity; baseSet = true; peakEquity = info.equity
            if info.equity > peakEquity then peakEquity = info.equity
            val dd = (peakEquity - info.equity) / peakEquity
            if dd > maxDd then maxDd = dd
            posAbsSum += math.abs(runPos); posSamples += 1
            if math.abs(runPos) > peakPos then peakPos = math.abs(runPos)
            if ev.exchangeTs - lastSampleTs >= sampleIntervalMs then
              lastSampleTs = ev.exchangeTs
              equityWriter.println(s"${ev.exchangeTs},${info.equity},$lastPx,$runPos")
        case _ => ()

    val engine = BacktestEngine(
      exchange = Exchange.Binance,
      source = source,
      runners = Seq(runner),
      config = SimConfig(
        exchangeToStrategyDelayMs = 100,
        orderToExchangeDelayMs = 50,
        initialBalanceUsdt = initBalance,
        makerFeeRate = makerFee,
        takerFeeRate = takerFee,
      ),
      observers = Seq(obs),
    )
    val r =
      try engine.run()
      finally
        equityWriter.close(); fillsWriter.close()

    val base = if baseSet then baseEquity else initBalance
    val retPct = (r.finalEquity - base) / base * 100
    val bhRetPct = if firstPxInPeriod > 0 then (lastPx - firstPxInPeriod) / firstPxInPeriod * 100 else 0.0
    val avgAbsPos = if posSamples > 0 then posAbsSum / posSamples else 0.0
    Stat(period.label, r.fills, base, r.finalEquity, retPct, bhRetPct, maxDd * 100, peakPos, avgAbsPos)

  println("==================== TrendRide: 震荡 vs 趋势 ====================")
  println(s"symbol=$symbol  capital=$initBalance  warmup=${warmupDays}d  fee maker=${makerFee * 100}%% taker=${takerFee * 100}%%")
  val stats = periods.map(runOne)
  println("-" * 100)
  println(f"${"period"}%6s ${"fills"}%7s ${"baseEquity"}%13s ${"finalEquity"}%13s ${"return%"}%9s ${"b&h%"}%9s ${"maxDD%"}%8s ${"peakPos"}%8s ${"avgPos"}%8s")
  stats.foreach { s =>
    println(f"${s.label}%6s ${s.fills}%7d ${s.baseEquity}%13.2f ${s.finalEquity}%13.2f ${s.retPct}%+8.2f%% ${s.bhRetPct}%+8.2f%% ${s.maxDrawdownPct}%7.2f%% ${s.peakPosCoin}%8.2f ${s.avgAbsPosCoin}%8.2f")
  }
  println("-" * 100)
  println(s"raw data -> $outDir/{chop,trend}_{equity,fills}.csv")
  println("================================================================")
