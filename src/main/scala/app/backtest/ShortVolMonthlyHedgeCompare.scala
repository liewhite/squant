package app.backtest

import hft.backtest.{BinanceDataKind, BinanceHistory}
import hft.domain.{Exchange, Symbol}
import hft.indicator.KlineSeries
import hft.messaging.EventData
import hft.strategy.Strategy
import strategy.research.{BreakoutHedgeStrategy, HedgeExecution, MacdBiasOverlay, TargetDeltaHedgeStrategy}
import sttp.client4.DefaultSyncBackend

import java.time.{LocalDate, ZoneOffset}
import java.time.temporal.ChronoUnit
import java.util.concurrent.Executors
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

/** **逐月三模式对冲对比** (IV=RV, 清除 VRP): 无对冲 / 纯动态对冲 / 带策略对冲。可选卖宽跨 (strangle)。
  *
  * 在 [[ShortVolMonthlyEdge]] 的 edge 隔离框架上, 每个 30 天窗 (IV=该窗实现 RV) 同时跑两种对冲, 派生三条序列:
  *   - **无对冲**: 只持空期权结构, 总 PnL = 期权腿 MTM (与对冲无关, 由 EquityPoint.optionPnl 直接取);
  *   - **纯动态对冲**: TargetDelta 死区 (band=|gamma|·S·1%), 见 delta 漂移即拉回中性 (常规连续对冲);
  *   - **带策略对冲**: 突破闸门 (8h Donchian / 10% 阈值, 区间内不对冲)。
  *
  * 因 IV=RV, 期权腿无系统性溢价, 三者差异只反映**对冲是否削减/放大了方向波动**。卖宽跨 (strangleWidthPct>0)
  * 时 call/put 行权外移, 近端 gamma 更低、两腿间有"死区", 对照其对三种对冲的影响。
  *
  * 运行: sbt "runMain hft.demo.ShortVolMonthlyHedgeCompare [strangleWidthPct] [start] [end] [windowDays] [bandRatio] [breakoutH] [thrPct] [fee]"
  *   strangleWidthPct: 0=ATM 跨式; 如 0.15=±15% 宽跨。默认 0。
  */
@main def ShortVolMonthlyHedgeCompare(args: String*): Unit =
  System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn")

  val strangleWidth = args.lift(0).map(_.toDouble).getOrElse(0.0)
  val start = args.lift(1).map(LocalDate.parse).getOrElse(LocalDate.parse("2025-04-10"))
  val end = args.lift(2).map(LocalDate.parse).getOrElse(LocalDate.parse("2026-06-15"))
  val windowDays = args.lift(3).map(_.toInt).getOrElse(30)
  val bandRatio = args.lift(4).map(_.toDouble).getOrElse(0.01)
  val breakoutH = args.lift(5).map(_.toDouble).getOrElse(8.0)
  val thrPct = args.lift(6).map(_.toDouble).getOrElse(0.10)
  val feeRate = args.lift(7).map(_.toDouble).getOrElse(0.0005)
  // arg8: 延迟模式 — "0"/"zero"=零延迟 (隔离延迟逆向成交); 否则默认 100/50ms
  val zeroLatency = args.lift(8).map(_.toLowerCase).exists(s => s == "0" || s == "zero")
  val (exDelay, ordDelay) = if zeroLatency then (0L, 0L) else (100L, 50L)
  // arg9: 方向 — "long"/"buy"=买方 (long vol); 否则默认卖方 (short vol)
  val isLong = args.lift(9).map(_.toLowerCase).exists(s => s == "long" || s == "buy")
  val sideSign = if isLong then 1.0 else -1.0
  val sideLabel = if isLong then "买方(long vol)" else "卖方(short vol)"
  val initBalance = ShortVolHedgeRunner.InitialBalanceUsdt
  val notionalMult = 1.0
  val symbol = ShortVolHedgeRunner.Symbol
  val breakoutBars = math.max(1, (breakoutH * 60).round.toInt)
  val kind = if strangleWidth > 0 then f"宽跨±${strangleWidth * 100}%.0f%%" else "ATM跨式"

  val curveDir = sys.env.getOrElse("CURVE_DIR", "/tmp/shortvol_hcmp")
  java.nio.file.Files.createDirectories(java.nio.file.Path.of(curveDir))

  val backend = DefaultSyncBackend()
  val symbolMetas = ShortVolHedgeRunner.fetchSymbolMetas(backend)

  // ===== 1. 预扫小时收盘 -> 逐窗 RV =====
  println(s"[1/3] 预扫小时收盘价 [$start .. $end] ...")
  val klines = new KlineSeries(3_600_000L, 24 * 500)
  val closes = scala.collection.mutable.ArrayBuffer.empty[(Long, Double)]
  var lastOpen = -1L
  BinanceHistory.source(backend, Seq(symbol), start, end, kinds = Seq(BinanceDataKind.Trades))
    .events().foreach { ev =>
      ev.data match
        case EventData.MarketTradeUpdate(t) if t.symbol == symbol =>
          klines.update(t.timestamp, t.price, t.qty)
          klines.bars.lastOption.foreach { b => if b.openTime != lastOpen then { lastOpen = b.openTime; closes += ((b.openTime, b.close)) } }
        case _ => ()
    }
  val closeArr = closes.toVector
  println(s"      小时收盘 ${closeArr.size} 根")

  def rvBetween(tsFrom: Long, tsTo: Long): Double =
    val ps = closeArr.filter((ts, _) => ts >= tsFrom && ts < tsTo).map(_._2)
    val rets = ps.sliding(2).collect { case Vector(a, b) if a > 0 && b > 0 => math.log(b / a) }.toVector
    if rets.sizeIs < 2 then 0.0
    else
      val mean = rets.sum / rets.size
      math.sqrt(rets.map(r => (r - mean) * (r - mean)).sum / (rets.size - 1)) * math.sqrt(365.0 * 24.0)
  def midnightMs(d: LocalDate): Long = d.atStartOfDay.toInstant(ZoneOffset.UTC).toEpochMilli

  // ===== 2. 切窗 =====
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
      val tsFrom = midnightMs(wStart); val tsTo = midnightMs(wEnd.plusDays(1))
      val rv = rvBetween(tsFrom, tsTo)
      val strike = closeArr.find((ts, _) => ts >= tsFrom).map(_._2).getOrElse(0.0)
      val n = if strike > 0 then math.max(1.0, (notionalMult * initBalance / strike).round.toDouble) else 0.0
      Option.when(strike > 0 && rv > 0)(Win(i, wStart, wEnd, days, rv, strike, sideSign * n))
  }

  // ===== 3. 每窗跑两种对冲 (band / breakout), 派生三序列 =====
  println(f"[2/3] 逐窗回测 ${windows.size}%d 窗 × 2 对冲 ($kind%s, IV=RV) ...")
  val bandFactory: Win => ((Exchange, Symbol, String, HedgeExecution) => Strategy) = _ =>
    (ex, sym, c, exec) => TargetDeltaHedgeStrategy(ex, sym, c, exec, overlay = MacdBiasOverlay(tiltMoveRatio = 0.0), bandMoveRatio = bandRatio)
  val brkFactory: Win => ((Exchange, Symbol, String, HedgeExecution) => Strategy) = w =>
    (ex, sym, c, exec) => BreakoutHedgeStrategy(ex, sym, c, exec, optionPositionEth = math.abs(w.straddles), windowBars = breakoutBars, maxThresholdPct = None, minThresholdPct = thrPct)

  def run(w: Win, factory: (Exchange, Symbol, String, HedgeExecution) => Strategy) =
    val params = ShortVolParams(w.wStart, w.wEnd, factory, feeRate, useMarket = true, impliedVol = w.rv, straddles = w.straddles, strangleWidthPct = strangleWidth,
      exchangeToStrategyDelayMs = exDelay, orderToExchangeDelayMs = ordDelay)
    ShortVolHedgeRunner.run(symbolMetas, ShortVolHedgeRunner.tradeSourceFactory(backend, w.wStart, w.wEnd), params)

  // 三序列 PnL + 拼接曲线点 (无对冲=本金+optionPnl(t); 动态=band totalEq; 策略=brk totalEq)
  final case class Res(win: Win, noHedge: Double, dynamic: Double, strategy: Double,
                       dynFills: Int, brkFills: Int,
                       curve: Vector[(Long, Double, Double, Double, Double)]) // ts, price, noHedgeEq, dynEq, strEq

  def runWin(w: Win): Res =
    val band = run(w, bandFactory(w))
    val brk = run(w, brkFactory(w))
    // optionPnl(t) 取自 band 曲线 (与对冲无关); 与 brk 曲线按 ts 对齐
    val brkByTs = brk.equityCurve.map(p => p.ts -> p.totalEquity).toMap
    val pts = band.equityCurve.map { p =>
      val strEq = brkByTs.getOrElse(p.ts, initBalance + p.optionPnl) // 对齐不上则退化为该点无对冲
      (p.ts, p.price, initBalance + p.optionPnl, p.totalEquity, strEq)
    }
    Res(w, band.optionPnl, band.totalPnl, brk.totalPnl, band.fills, brk.fills, pts)

  val pool = Executors.newFixedThreadPool(math.min(windows.size, Runtime.getRuntime.availableProcessors))
  given ec: ExecutionContext = ExecutionContext.fromExecutor(pool)
  val results =
    try Await.result(Future.sequence(windows.map(w => Future(runWin(w)))), Duration.Inf)
    finally pool.shutdown()
  val sorted = results.sortBy(_.win.idx)

  // ===== 输出 =====
  println("[3/3] 落盘 + 报表 ...")
  val sumW = java.io.PrintWriter(s"$curveDir/monthly_summary.csv")
  sumW.println("idx,start,end,days,rvPct,straddles,noHedge,dynamic,strategy,dynFills,brkFills")
  val curveW = java.io.PrintWriter(s"$curveDir/cumulative_curve.csv")
  curveW.println("ts,monthIdx,price,rvPct,cumNoHedge,cumDynamic,cumStrategy")
  var cN, cD, cS = 0.0
  sorted.foreach { r =>
    sumW.println(s"${r.win.idx},${r.win.wStart},${r.win.wEnd},${r.win.days},${r.win.rv * 100},${math.abs(r.win.straddles)},${r.noHedge},${r.dynamic},${r.strategy},${r.dynFills},${r.brkFills}")
    r.curve.foreach { (ts, px, nEq, dEq, sEq) =>
      curveW.println(s"$ts,${r.win.idx},$px,${r.win.rv * 100},${initBalance + cN + (nEq - initBalance)},${initBalance + cD + (dEq - initBalance)},${initBalance + cS + (sEq - initBalance)}")
    }
    cN += r.noHedge; cD += r.dynamic; cS += r.strategy
  }
  sumW.close(); curveW.close()

  def pct(x: Double) = x / initBalance * 100
  val (tN, tD, tS) = (sorted.map(_.noHedge).sum, sorted.map(_.dynamic).sum, sorted.map(_.strategy).sum)
  println()
  println(f"==================== 逐月三模式对冲对比 ($sideLabel%s, $kind%s, IV=RV) ====================")
  println(f"周期=[$start .. $end] 窗=$windowDays%d天 | 纯动态=band ${bandRatio * 100}%.1f%% | 带策略=突破${breakoutH}%.0fh/${(thrPct * 100).toInt}%% | 期权名义1×本金 fee=${feeRate * 100}%.3f%% | 延迟=$exDelay%d+$ordDelay%dms")
  println("-" * 100)
  println(f"${"窗"}%3s ${"起"}%11s ${"天"}%3s ${"RV=IV"}%7s ${"无对冲"}%11s ${"纯动态"}%11s ${"带策略"}%11s ${"动态fills"}%8s ${"策略fills"}%8s")
  sorted.foreach { r =>
    println(f"${r.win.idx}%3d ${r.win.wStart.toString}%11s ${r.win.days}%3d ${r.win.rv * 100}%6.1f%% ${r.noHedge}%+11.1f ${r.dynamic}%+11.1f ${r.strategy}%+11.1f ${r.dynFills}%8d ${r.brkFills}%8d")
  }
  println("-" * 100)
  println(f"合计 PnL : 无对冲 ${tN}%+.0f (${pct(tN)}%+.2f%%) | 纯动态 ${tD}%+.0f (${pct(tD)}%+.2f%%) | 带策略 ${tS}%+.0f (${pct(tS)}%+.2f%%)")
  println(f"逐月胜率 : 无对冲 ${sorted.count(_.noHedge > 0)}%d/${sorted.size}%d | 纯动态 ${sorted.count(_.dynamic > 0)}%d/${sorted.size}%d | 带策略 ${sorted.count(_.strategy > 0)}%d/${sorted.size}%d")
  // 波动率 (逐月 PnL 标准差) -> 看对冲是否削减离散度
  def std(xs: Seq[Double]) = { val m = xs.sum / xs.size; math.sqrt(xs.map(x => (x - m) * (x - m)).sum / xs.size) }
  println(f"逐月PnL波动: 无对冲 ${std(sorted.map(_.noHedge))}%.0f | 纯动态 ${std(sorted.map(_.dynamic))}%.0f | 带策略 ${std(sorted.map(_.strategy))}%.0f  (越小=对冲越稳)")
  println("=========================================================================")
