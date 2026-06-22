package app.backtest

import hft.backtest.{BacktestEngine, BinanceDataKind, BinanceHistory}
import hft.domain.{Exchange, Side}
import hft.engine.StrategyRunner
import hft.messaging.{EventData, IncomeEvent}
import hft.sim.SimConfig
import strategy.research.MaMacdGridStrategy
import sttp.client4.DefaultSyncBackend

import java.time.LocalDate
import java.util.concurrent.Executors
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

/** 均线 + MACD 方向性网格策略回测 —— 止盈方案对比 (基线 / 方案1 / 方案2)。
  *
  * 净值每次冲高都大幅回吐, 根因是平仓太慢 (逐笔 min(baseQty,pos)) 且离场触发偏晚。两套补救:
  *   - 基线 (baseline)：逐笔平仓 + 双确认离场 (现状)。
  *   - 方案1 (half-close)：单次平仓至少 max(半仓, baseQty)，浮盈更快兑现。
  *   - 方案2 (atr-stretch)：价偏离均线 ≥ N×ATR 即切移动止盈 且 停止加仓 (N 由 MaDeviationAnalysis 标定)。
  *
  * 每方案各跑一遍, 输出成交数 / 已实现盈亏 / 末净值 / 收益率 / 资金利用率, 并落净值+仓位曲线 CSV 供画图。
  *
  * 运行:
  *   sbt "runMain hft.demo.MaMacdGridBacktest [SYMBOL] [START] [END] [baseQty] [maxPos] [atrN]"
  * 例:
  *   sbt "runMain hft.demo.MaMacdGridBacktest ETHUSDT 2025-04-10 2026-06-15 5 100 3.0"
  */
@main def MaMacdGridBacktest(args: String*): Unit =
  System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn")

  val symbol = args.lift(0).getOrElse("ETHUSDT")
  val start = LocalDate.parse(args.lift(1).getOrElse("2026-01-10"))
  val end = LocalDate.parse(args.lift(2).getOrElse("2026-06-16"))
  val baseQty = args.lift(3).map(_.toDouble).getOrElse(0.5)
  val maxPos = args.lift(4).map(_.toDouble).getOrElse(10.0)
  val atrN = args.lift(5).map(_.toDouble).getOrElse(3.0) // 方案2 拉伸阈值 N (×ATR)

  val initBalance = 100_000.0
  val makerFee = 0.0002
  val takerFee = 0.0005
  // 固定档距 (前序回测确定): 加仓/反向移动止盈用窄档, 顺势止盈用宽档
  val addSpacing = 0.004
  val largeSpacing = 0.050
  val exitSpacing = 0.004

  val backend = DefaultSyncBackend()
  val publicClient = hft.exchange.binance.BinanceClient(backend, credentials = None)
  val symbolMetas = publicClient
    .fetchAllSymbolMetas()
    .fold(e => sys.error(s"fetch symbol metas failed: ${e.message}"), _.map(m => (m.exchange, m.symbol) -> m).toMap)

  /** 止盈方案: 名字 + 两个可选增强 (单次平仓最小占比 / ATR 拉伸阈值)。两者皆 0 = 基线。 */
  final case class Scheme(name: String, closeMinFraction: Double, atrStretchN: Double)
  val schemes = Seq(
    Scheme("baseline", 0.0, 0.0),
    Scheme("half-close", 0.5, 0.0),
    Scheme("atr-stretch", 0.0, atrN),
  )

  /** 净值曲线输出目录 (每方案一份 ts,equity,price,pos 的小时采样 CSV) */
  val curveDir = sys.env.getOrElse("CURVE_DIR", "/tmp/grid_curves")
  java.nio.file.Files.createDirectories(java.nio.file.Path.of(curveDir))

  final case class Row(
      scheme: String,
      fills: Int,
      realized: Double,
      equity: Double,
      posCoin: Double,
      avgUtilPct: Double, // 平均资金利用率 = 平均持仓名义 / 净值
      peakUtilPct: Double, // 峰值资金利用率
  )

  def runOne(scheme: Scheme): (Row, (Double, Double)) =
    // 每方案重建数据源 (命中本地缓存)；b&h 价格用观察者抓首/末成交价
    val source = BinanceHistory.source(backend, Seq(symbol), start, end, kinds = Seq(BinanceDataKind.Trades))
    val strategy = MaMacdGridStrategy(
      exchange = Exchange.Binance,
      symbol = symbol,
      addSpacing = addSpacing,
      largeSpacing = largeSpacing, // 顺势止盈 (宽, 静态, 让利润跑)
      exitSpacing = exitSpacing,   // 反向后的移动止盈 (窄)
      closeMinFraction = scheme.closeMinFraction,
      atrStretchN = scheme.atrStretchN,
      baseQty = baseQty,
      maxPositionCoin = maxPos,
    )
    val runner = StrategyRunner.backtest(strategy, symbolMetas)
    var firstPx = 0.0
    var lastPx = 0.0
    // 资金利用率: 每次 AccountInfoUpdate (引擎每秒发) 取 notional/equity, 求均值与峰值
    var utilSum = 0.0
    var utilCnt = 0L
    var utilPeak = 0.0
    // 净值+仓位曲线: 按小时下采样 (ts,equity,price,posCoin) 落 CSV。仓位由 Fill 累计 (与账本一致)
    val curveWriter = java.io.PrintWriter(s"$curveDir/equity_${scheme.name}.csv")
    curveWriter.println("ts,equity,price,pos")
    var lastSampleTs = 0L
    var runPos = 0.0
    val sampleIntervalMs = 3_600_000L
    val obs: IncomeEvent => Unit = ev =>
      ev.data match
        case EventData.MarketTradeUpdate(t) =>
          if firstPx == 0.0 then firstPx = t.price
          lastPx = t.price
        case EventData.FillUpdate(f) =>
          runPos += (if f.side == Side.Long then f.size else -f.size)
        case EventData.AccountInfoUpdate(_, info) =>
          if info.equity > 0 then
            val util = info.notional / info.equity
            utilSum += util
            utilCnt += 1
            if util > utilPeak then utilPeak = util
            if ev.exchangeTs - lastSampleTs >= sampleIntervalMs then
              lastSampleTs = ev.exchangeTs
              curveWriter.println(s"${ev.exchangeTs},${info.equity},$lastPx,$runPos")
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
      finally curveWriter.close()
    val posCoin = r.positions.map(_.size).sum
    val avgUtil = if utilCnt > 0 then utilSum / utilCnt * 100 else 0.0
    (Row(scheme.name, r.fills, r.realizedPnl, r.finalEquity, posCoin, avgUtil, utilPeak * 100), (firstPx, lastPx))

  println("==================== MA+MACD Grid: 止盈方案对比 ====================")
  println(s"symbol=$symbol  range=[$start .. $end]  baseQty=$baseQty maxPos=$maxPos  atrN=$atrN")
  println(f"capital=$initBalance%.0f  fee maker=${makerFee * 100}%.3f%% taker=${takerFee * 100}%.3f%%")

  // 并行跑各方案 (数据已缓存且流式读取, 各线程独立读 zip)
  val pool = Executors.newFixedThreadPool(math.min(schemes.size, Runtime.getRuntime.availableProcessors))
  given ec: ExecutionContext = ExecutionContext.fromExecutor(pool)
  val results =
    try Await.result(Future.sequence(schemes.map(s => Future(runOne(s)))), Duration.Inf)
    finally pool.shutdown()
  val rows = results.map(_._1)
  val (firstPx, lastPx) = results.head._2

  println("-" * 86)
  println(f"${"scheme"}%12s ${"fills"}%7s ${"realizedPnL"}%14s ${"finalEquity"}%14s ${"return%"}%9s ${"posCoin"}%9s ${"avgUtil"}%9s ${"peakUtil"}%9s")
  rows.foreach { row =>
    val ret = (row.equity - initBalance) / initBalance * 100
    println(
      f"${row.scheme}%12s ${row.fills}%7d ${row.realized}%+14.2f ${row.equity}%14.2f ${ret}%+8.2f%% ${row.posCoin}%+9.3f ${row.avgUtilPct}%8.2f%% ${row.peakUtilPct}%8.2f%%"
    )
  }
  println("-" * 86)
  val bhRet = if firstPx > 0 then (lastPx - firstPx) / firstPx * 100 else 0.0
  println(f"buy&hold: $symbol $firstPx%.2f -> $lastPx%.2f  (${bhRet}%+.2f%%)")
  val best = rows.maxBy(_.equity)
  println(f"best: ${best.scheme}%s -> equity=${best.equity}%.2f (${(best.equity - initBalance) / initBalance * 100}%+.2f%%)")
  println("===================================================================")
