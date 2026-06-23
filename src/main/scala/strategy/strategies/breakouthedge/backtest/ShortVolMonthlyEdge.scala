package strategy.strategies.breakouthedge.backtest
import strategy.utils.backtest.{EquityPoint, ShortVolParams}
import strategy.utils.backtest.ShortVolHedgeRunner

import hft.backtest.{BinanceDataKind, BinanceHistory}
import hft.domain.{Exchange, Symbol}
import hft.indicator.{KlineSeries, RealizedVol}
import hft.messaging.EventData
import hft.option.BlackScholes
import hft.strategy.Strategy
import strategy.strategies.breakouthedge.logic.BreakoutHedgeStrategy
import strategy.utils.hedge.HedgeExecution
import sttp.client4.DefaultSyncBackend

import java.time.{LocalDate, ZoneOffset}
import java.time.temporal.ChronoUnit
import java.util.concurrent.Executors
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

/** **对冲 edge 隔离实验**：按 30 天滚动窗逐月卖跨式, 每窗 **IV = 该窗实现波动 RV**, 用 8h/10% 突破闸门对冲。
  *
  * 思路：short-vol 的系统性收益来自 VRP = f(IV − RV)。若把每个 30 天窗的 IV **设成该窗事后实现的 RV**,
  * 则一阶 VRP 被清零 (权利金按真实波动公平定价), 剩下的净 PnL 只能来自**对冲策略本身的路径行为**
  * (突破闸门的择时 gamma scalping) + 二阶凸性。**若逐月净 PnL 系统性为正 -> 对冲有 edge; ≈0 或为负 -> 无 edge / 纯成本。**
  *
  * 设计 (会在报表标注):
  *   - 每月**独立** 10 万本金 (不复利, 避免规模漂移污染逐月对比)；
  *   - 期权名义 = 1× 本金 (straddles = 本金/行权价, 行权价=窗首价), 各月规模可比；
  *   - IV = 该窗 30 天小时收益年化 RV (**完美前视定价**——非可交易, 仅为隔离 edge 的受控实验)；
  *   - 末窗不足 30 天按剩余天数, tenor = 实际窗长。
  *
  * 运行: sbt "runMain strategy.strategies.breakouthedge.backtest.ShortVolMonthlyEdge [start] [end] [windowDays] [windowH] [thrPct] [fee]"
  *   默认 [2025-04-10 .. 2026-06-15], 30 天窗, 8h 窗口, 10% 阈值, fee 0.0005。
  */
@main def ShortVolMonthlyEdge(args: String*): Unit =
  System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn")

  val start = args.lift(0).map(LocalDate.parse).getOrElse(LocalDate.parse("2025-04-10"))
  val end = args.lift(1).map(LocalDate.parse).getOrElse(LocalDate.parse("2026-06-15"))
  val windowDays = args.lift(2).map(_.toInt).getOrElse(30)
  val windowHours = args.lift(3).map(_.toDouble).getOrElse(8.0)
  val thrPct = args.lift(4).map(_.toDouble).getOrElse(0.10)
  val feeRate = args.lift(5).map(_.toDouble).getOrElse(0.0005)
  val initBalance = ShortVolHedgeRunner.InitialBalanceUsdt
  val notionalMult = 1.0 // 期权名义 = 1× 本金
  val symbol = ShortVolHedgeRunner.Symbol
  val breakoutBars = math.max(1, (windowHours * 60).round.toInt)

  val curveDir = sys.env.getOrElse("CURVE_DIR", "/tmp/shortvol_monthly")
  java.nio.file.Files.createDirectories(java.nio.file.Path.of(curveDir))

  val backend = DefaultSyncBackend()
  val symbolMetas = ShortVolHedgeRunner.fetchSymbolMetas(backend)

  // ==================== 1. 预扫: 全周期小时收盘价 (用于逐窗算 RV) ====================
  println(s"[1/3] 预扫小时收盘价 [$start .. $end] ...")
  val klines = new KlineSeries(3_600_000L, 24 * 500)
  val closes = scala.collection.mutable.ArrayBuffer.empty[(Long, Double)]
  var lastOpen = -1L
  BinanceHistory
    .source(backend, Seq(symbol), start, end, kinds = Seq(BinanceDataKind.Trades))
    .events()
    .foreach { ev =>
      ev.data match
        case EventData.MarketTradeUpdate(t) if t.symbol == symbol =>
          klines.update(t.timestamp, t.price, t.qty)
          klines.bars.lastOption.foreach { b => if b.openTime != lastOpen then { lastOpen = b.openTime; closes += ((b.openTime, b.close)) } }
        case _ => ()
    }
  val closeArr = closes.toVector
  println(s"      小时收盘 ${closeArr.size} 根")

  /** [tsFrom, tsTo) 内小时收益年化 RV */
  def rvBetween(tsFrom: Long, tsTo: Long): Double =
    val ps = closeArr.filter((ts, _) => ts >= tsFrom && ts < tsTo).map(_._2)
    RealizedVol.annualizedSampleStdFromPrices(ps, BlackScholes.HoursPerYear)

  def midnightMs(d: LocalDate): Long = d.atStartOfDay.toInstant(ZoneOffset.UTC).toEpochMilli

  // ==================== 2. 切 30 天窗 ====================
  final case class Win(idx: Int, wStart: LocalDate, wEnd: LocalDate, days: Long, rv: Double, strike: Double, straddles: Double)
  val totalDays = ChronoUnit.DAYS.between(start, end) + 1
  val nWin = math.ceil(totalDays.toDouble / windowDays).toInt
  val windows = (0 until nWin).flatMap { i =>
    val wStart = start.plusDays(i.toLong * windowDays)
    if wStart.isAfter(end) then None
    else
      val wEndRaw = wStart.plusDays(windowDays - 1)
      val wEnd = if wEndRaw.isAfter(end) then end else wEndRaw
      val days = ChronoUnit.DAYS.between(wStart, wEnd) + 1
      val tsFrom = midnightMs(wStart)
      val tsTo = midnightMs(wEnd.plusDays(1))
      val rv = rvBetween(tsFrom, tsTo)
      val strike = closeArr.find((ts, _) => ts >= tsFrom).map(_._2).getOrElse(0.0)
      val straddles = if strike > 0 then math.max(1.0, (notionalMult * initBalance / strike).round.toDouble) else 0.0
      Option.when(strike > 0 && rv > 0)(Win(i, wStart, wEnd, days, rv, strike, -straddles))
  }

  // ==================== 3. 逐窗回测 (IV=RV, 8h/10% 闸门) ====================
  println(s"[2/3] 逐窗回测 ${windows.size} 个窗 (IV=RV, ${windowHours}%.0fh/${(thrPct * 100).toInt}%% 闸门) ...".format(windowHours))
  final case class Res(win: Win, optionPnl: Double, hedgePnl: Double, totalPnl: Double, fills: Int, curve: Vector[EquityPoint])

  def runWin(w: Win): Res =
    val factory: (Exchange, Symbol, String, HedgeExecution) => Strategy =
      (ex, sym, c, exec) =>
        BreakoutHedgeStrategy(ex, sym, c, exec, optionPositionEth = math.abs(w.straddles),
          windowBars = breakoutBars, maxThresholdPct = None, minThresholdPct = thrPct)
    val params = ShortVolParams(
      start = w.wStart, end = w.wEnd, strategyFactory = factory,
      feeRate = feeRate, useMarket = true, impliedVol = w.rv, straddles = w.straddles,
    )
    val src = ShortVolHedgeRunner.tradeSourceFactory(backend, w.wStart, w.wEnd)
    val out = ShortVolHedgeRunner.run(symbolMetas, src, params)
    Res(w, out.optionPnl, out.hedgePnl, out.totalPnl, out.fills, out.equityCurve)

  val pool = Executors.newFixedThreadPool(math.min(windows.size, Runtime.getRuntime.availableProcessors))
  given ec: ExecutionContext = ExecutionContext.fromExecutor(pool)
  val results =
    try Await.result(Future.sequence(windows.map(w => Future(runWin(w)))), Duration.Inf)
    finally pool.shutdown()
  val sorted = results.sortBy(_.win.idx)

  // ==================== 输出: 逐月 + 累计曲线 CSV ====================
  println("[3/3] 落盘曲线 + 报表 ...")
  // 月度汇总
  val sumW = java.io.PrintWriter(s"$curveDir/monthly_summary.csv")
  sumW.println("idx,start,end,days,rvPct,strike,straddles,optionPnl,hedgePnl,totalPnl,retPct,fills")
  // 拼接累计曲线: 全局 ts, 累计净值 = 本金 + 之前各月已结 PnL + 本月浮动 PnL
  val curveW = java.io.PrintWriter(s"$curveDir/cumulative_curve.csv")
  curveW.println("ts,cumEquity,monthIdx,monthPnl,price,rvPct")
  var cumPrior = 0.0
  sorted.foreach { r =>
    val ret = r.totalPnl / initBalance * 100.0
    sumW.println(s"${r.win.idx},${r.win.wStart},${r.win.wEnd},${r.win.days},${r.win.rv * 100},${r.win.strike},${math.abs(r.win.straddles)},${r.optionPnl},${r.hedgePnl},${r.totalPnl},$ret,${r.fills}")
    r.curve.foreach { p =>
      val monthPnl = p.totalEquity - initBalance
      curveW.println(s"${p.ts},${initBalance + cumPrior + monthPnl},${r.win.idx},$monthPnl,${p.price},${r.win.rv * 100}")
    }
    cumPrior += r.totalPnl
  }
  sumW.close(); curveW.close()

  val totalPnl = sorted.map(_.totalPnl).sum
  val totalOpt = sorted.map(_.optionPnl).sum
  val totalHedge = sorted.map(_.hedgePnl).sum
  val wins = sorted.count(_.totalPnl > 0)

  println()
  println("==================== ShortVol 对冲 edge 隔离 (逐月 IV=RV) ====================")
  println(f"周期=[$start .. $end]  窗=$windowDays%d天  对冲=${windowHours}%.0fh/${(thrPct * 100).toInt}%%闸门  IV=各窗RV  期权名义=1×本金  执行=市价 fee=${feeRate * 100}%.3f%%")
  println("-" * 92)
  println(f"${"窗"}%3s ${"起"}%11s ${"止"}%11s ${"天"}%3s ${"RV=IV"}%7s ${"行权"}%7s ${"份"}%4s ${"期权腿"}%10s ${"对冲腿"}%10s ${"月PnL"}%10s ${"收益%"}%7s ${"对冲"}%5s")
  sorted.foreach { r =>
    val w = r.win; val ret = r.totalPnl / initBalance * 100.0
    println(f"${w.idx}%3d ${w.wStart.toString}%11s ${w.wEnd.toString}%11s ${w.days}%3d ${w.rv * 100}%6.1f%% ${w.strike}%7.0f ${math.abs(w.straddles)}%4.0f ${r.optionPnl}%+10.1f ${r.hedgePnl}%+10.1f ${r.totalPnl}%+10.1f ${ret}%+6.2f%% ${r.fills}%5d")
  }
  println("-" * 92)
  println(f"合计: 期权腿 ${totalOpt}%+.1f  对冲腿 ${totalHedge}%+.1f  完整 ${totalPnl}%+.1f  (累计 ${totalPnl / initBalance * 100}%+.2f%% on 单月10万基准)")
  println(f"逐月胜率: $wins%d/${sorted.size}%d 个月为正  | 月均 PnL ${totalPnl / sorted.size}%+.1f")
  println("判读: IV=RV 已清除一阶 VRP -> 残余净 PnL = 对冲策略路径 edge + 二阶凸性。系统性为正 => 对冲有 edge。")
  println("============================================================================")
