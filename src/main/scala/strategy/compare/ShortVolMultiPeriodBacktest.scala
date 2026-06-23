package strategy.compare
import strategy.utils.backtest.{ShortVolHedgeRunner, ShortVolParams}

import hft.domain.{Exchange, Symbol}
import hft.indicator.RealizedVol
import hft.option.BlackScholes
import hft.strategy.Strategy
import strategy.strategies.breakouthedge.logic.BreakoutHedgeStrategy
import strategy.strategies.targetdeltahedge.logic.TargetDeltaHedgeStrategy
import strategy.utils.hedge.{HedgeExecution, MacdBiasOverlay}
import sttp.client4.DefaultSyncBackend

import java.time.LocalDate
import java.time.temporal.ChronoUnit
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import java.util.concurrent.Executors

/** 卖方 (short straddle) + **中性动态 delta 对冲** 的**多周期对比**回测。
  *
  * 用同一套对冲配置 (空 10 份 ATM 跨式, IV=50%, 目标 delta=0 纯中性, band=|gamma|·S·1% 死区, 市价对冲)
  * 在多个给定时间段各跑一遍, 横向对比各维度 (净走势 / 实现波动 RV / 两腿损益 / 收益率 / 年化 / 对冲次数 /
  * 最大回撤), 并落各周期完整净值曲线 CSV 供画资金曲线对比。
  *
  * 对照核心: short-vol 损益 ≈ VRP = ½·Γ·S²·(σ_implied² − σ_realized²)·dt。IV 固定, 故各周期成败
  * 主要由该期**实现波动 RV** (越低卖方越赚) 与**对冲成本** (gamma 滑点 + 手续费) 决定。
  *
  * 对冲模式 (第 3 参):
  *   - `band`     : 中性目标 delta + |gamma|·S·band 死区 ([[TargetDeltaHedgeStrategy]])；
  *   - `breakout` : **纯突破闸门** ([[BreakoutHedgeStrategy]] maxThresholdPct=None)——只有价突破近 5h
  *                  Donchian 高/低点 且 |净 delta| 达阈值才对冲；区间内来回完全不对冲 (省 gamma 滑点/手续费)。
  *
  * 运行: sbt "runMain strategy.compare.ShortVolMultiPeriodBacktest [fee] [iv] [band|breakout] [param] [straddles] [windowH]"
  *   param  : band 模式=死区比例(默认 0.01); breakout 模式=delta 阈值占比(默认 0.10, ×|straddles|)
  *   windowH: breakout 模式的 Donchian 高/低窗口小时数 (默认 5h)
  */
@main def ShortVolMultiPeriodBacktest(args: String*): Unit =
  System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn")

  val feeRate = args.lift(0).map(_.toDouble).getOrElse(0.0005) // 市价 taker
  val impliedVol = args.lift(1).map(_.toDouble).getOrElse(0.5)
  val hedgeMode = args.lift(2).map(_.toLowerCase).getOrElse("breakout")
  val isBreakout = hedgeMode == "breakout"
  val param = args.lift(3).map(_.toDouble).getOrElse(if isBreakout then 0.10 else 0.01)
  val straddles = args.lift(4).map(_.toDouble).getOrElse(-10.0) // 负=卖方
  val windowHours = args.lift(5).map(_.toDouble).getOrElse(5.0) // breakout Donchian 窗口 (小时)
  val initBalance = ShortVolHedgeRunner.InitialBalanceUsdt
  val optionPositionEth = math.abs(straddles)
  val breakoutWindowBars = math.max(1, (windowHours * 60).round.toInt) // 1min bar

  // 待测时间段
  val periods = Seq(
    ("P1", LocalDate.parse("2025-08-10"), LocalDate.parse("2025-10-26")),
    ("P2", LocalDate.parse("2026-02-08"), LocalDate.parse("2026-03-14")),
    ("P3", LocalDate.parse("2026-01-26"), LocalDate.parse("2026-02-05")),
  )

  // 对冲工厂: breakout=纯突破闸门 (5h Donchian, 区间内不对冲); band=中性目标 delta + 死区
  val factory: (Exchange, Symbol, String, HedgeExecution) => Strategy =
    if isBreakout then
      (ex, sym, c, exec) =>
        BreakoutHedgeStrategy(
          ex, sym, c, exec,
          optionPositionEth = optionPositionEth,
          windowBars = breakoutWindowBars,
          maxThresholdPct = None,    // 纯闸门: 区间内不对冲
          minThresholdPct = param,   // 突破方向 + |净delta| 达 param·头寸 才对冲
        )
    else
      (ex, sym, c, exec) =>
        TargetDeltaHedgeStrategy(ex, sym, c, exec, overlay = MacdBiasOverlay(tiltMoveRatio = 0.0), bandMoveRatio = param)

  val curveDir = sys.env.getOrElse("CURVE_DIR", "/tmp/shortvol_curves")
  java.nio.file.Files.createDirectories(java.nio.file.Path.of(curveDir))

  val backend = DefaultSyncBackend()
  val symbolMetas = ShortVolHedgeRunner.fetchSymbolMetas(backend)

  /** 年化实现波动: 小时对数收益的样本标准差 × √(每年小时数) */
  def annualizedRv(prices: Vector[Double]): Double =
    RealizedVol.annualizedSampleStdFromPrices(prices, BlackScholes.HoursPerYear)

  /** 净值曲线最大回撤 (从运行峰值回落比例) */
  def maxDrawdown(equities: Vector[Double]): Double =
    var peak = Double.NegativeInfinity
    var mdd = 0.0
    equities.foreach { e =>
      if e > peak then peak = e
      if peak > 0 then mdd = math.max(mdd, (peak - e) / peak)
    }
    mdd

  final case class Dim(
      name: String, days: Long, netMovePct: Double, rvPct: Double,
      optionPnl: Double, hedgePnl: Double, totalPnl: Double,
      retPct: Double, annRetPct: Double, fills: Int, maxDdPct: Double,
  )

  def runOne(name: String, start: LocalDate, end: LocalDate): Dim =
    val params = ShortVolParams(
      start = start, end = end, strategyFactory = factory,
      feeRate = feeRate, useMarket = true, impliedVol = impliedVol, straddles = straddles,
    )
    val src = ShortVolHedgeRunner.tradeSourceFactory(backend, start, end)
    val out = ShortVolHedgeRunner.run(symbolMetas, src, params)
    // 落净值曲线 CSV
    val w = java.io.PrintWriter(s"$curveDir/equity_$name.csv")
    w.println("ts,totalEquity,price,optionPnl,hedgeEquity")
    out.equityCurve.foreach(p => w.println(s"${p.ts},${p.totalEquity},${p.price},${p.optionPnl},${p.hedgeEquity}"))
    w.close()
    val days = ChronoUnit.DAYS.between(start, end) + 1
    val ret = out.totalPnl / initBalance * 100.0
    val annRet = if days > 0 then ret * 365.0 / days else 0.0 // 简单线性年化 (非复利, 仅作量级对比)
    Dim(
      name = name, days = days, netMovePct = out.netMovePct,
      rvPct = annualizedRv(out.equityCurve.map(_.price)) * 100.0,
      optionPnl = out.optionPnl, hedgePnl = out.hedgePnl, totalPnl = out.totalPnl,
      retPct = ret, annRetPct = annRet, fills = out.fills,
      maxDdPct = maxDrawdown(out.equityCurve.map(_.totalEquity)) * 100.0,
    )

  // 各周期并行 (数据已缓存且流式)
  val pool = Executors.newFixedThreadPool(math.min(periods.size, Runtime.getRuntime.availableProcessors))
  given ec: ExecutionContext = ExecutionContext.fromExecutor(pool)
  val dims =
    try Await.result(Future.sequence(periods.map((n, s, e) => Future(runOne(n, s, e)))), Duration.Inf)
    finally pool.shutdown()

  println()
  println("==================== ShortVol 多周期对比 (卖方 + 中性动态 delta 对冲) ====================")
  val hedgeLabel =
    if isBreakout then f"突破闸门(${windowHours}%.0fh Donchian, 阈值=${param * 100}%.1f%%·头寸=${param * optionPositionEth}%.1fETH, 区间内不对冲)"
    else f"死区(|gamma|·S·${param * 100}%.2f%%)"
  println(f"配置: straddles=$straddles%.0f (short)  IV=${impliedVol * 100}%.0f%%  对冲=$hedgeLabel  执行=市价(taker)  fee=${feeRate * 100}%.3f%%  本金=$initBalance%.0f")
  println(s"曲线 CSV -> $curveDir/equity_{${periods.map(_._1).mkString(",")}}.csv")
  println("-" * 104)
  println(f"${"周期"}%-5s ${"起"}%10s ${"止"}%10s ${"天"}%4s ${"净走势"}%8s ${"RV年化"}%8s ${"期权腿"}%11s ${"对冲腿"}%11s ${"完整PnL"}%11s ${"收益率"}%8s ${"年化"}%9s ${"对冲数"}%7s ${"最大回撤"}%8s")
  periods.zip(dims).foreach { case ((_, s, e), d) =>
    println(
      f"${d.name}%-5s ${s.toString}%10s ${e.toString}%10s ${d.days}%4d ${d.netMovePct}%+7.2f%% ${d.rvPct}%7.1f%% ${d.optionPnl}%+11.1f ${d.hedgePnl}%+11.1f ${d.totalPnl}%+11.1f ${d.retPct}%+7.2f%% ${d.annRetPct}%+8.1f%% ${d.fills}%7d ${d.maxDdPct}%7.2f%%"
    )
  }
  println("-" * 104)
  println("说明: 期权腿=short跨式MTM(含theta收入, 只与价路径/IV有关); 对冲腿=gamma滑点+手续费(恒负); 完整=两腿和。")
  println("      VRP: IV=50%恒定, RV越低卖方越赚; RV>IV则期权腿亏。年化为线性外推(非复利), 仅作不同长度周期的量级对比。")
  println("==========================================================================================")
