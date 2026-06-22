package hft.demo

import hft.domain.{Exchange, Symbol}
import hft.strategy.Strategy
import strategy.research.{BreakoutHedgeStrategy, HedgeExecution}
import sttp.client4.DefaultSyncBackend

import java.time.LocalDate
import java.time.temporal.ChronoUnit
import java.util.concurrent.Executors
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

/** 卖方 + **突破闸门对冲** 的 (窗口 × 阈值) 组合扫描 (单一长周期)。
  *
  * 在同一长周期上, 对若干 (Donchian 窗口小时, delta 阈值占比) 组合各跑一遍突破闸门对冲
  * ([[BreakoutHedgeStrategy]] maxThresholdPct=None, 区间内不对冲), 横向对比对冲腿成本 / 完整 PnL /
  * 对冲次数 / 最大回撤, 并落各组合完整净值曲线 CSV。
  *
  * **组合设计逻辑** (非穷举, 隔离两轴 + 一个成本最小角):
  *   - 窗口轴 (噪声过滤): {3h, 5h, 8h} @ 阈值10% —— 窗口越长越只对真突破出手 (实测 1h≪5h);
  *   - 阈值轴 (delta 死区): {6%, 10%, 15%} @ 5h —— RV>IV 区制下对冲多为净成本, 阈值越松对冲越少;
  *   - 角点: 8h/15% (两轴都松) = "尽量少对冲" 的极端。
  *
  * **关键不变量**: 期权腿 MTM 与对冲规则无关 (同周期+IV 下各组合恒相等), 故组合差异**全部**落在对冲腿。
  *
  * 运行: sbt "runMain hft.demo.ShortVolBreakoutSweep [start] [end] [iv] [straddles] [fee]"
  *   默认 [2025-04-10 .. 2026-06-15], IV=70%, straddles=-10, fee=0.0005 (市价 taker)
  */
@main def ShortVolBreakoutSweep(args: String*): Unit =
  System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn")

  val start = args.lift(0).map(LocalDate.parse).getOrElse(LocalDate.parse("2025-04-10"))
  val end = args.lift(1).map(LocalDate.parse).getOrElse(LocalDate.parse("2026-06-15"))
  val impliedVol = args.lift(2).map(_.toDouble).getOrElse(0.70)
  val straddles = args.lift(3).map(_.toDouble).getOrElse(-10.0)
  val feeRate = args.lift(4).map(_.toDouble).getOrElse(0.0005)
  val initBalance = ShortVolHedgeRunner.InitialBalanceUsdt
  val optionPositionEth = math.abs(straddles)

  /** (标签, 窗口小时, delta 阈值占比) —— 固定阈值 10%, 沿窗口轴向长端延伸 (前测 8h 优于 3h/5h, 看是否继续单调) */
  val combos: Seq[(String, Double, Double)] = Seq(
    ("8h_10%", 8.0, 0.10),
    ("12h_10%", 12.0, 0.10),
    ("16h_10%", 16.0, 0.10),
    ("20h_10%", 20.0, 0.10),
  )

  val curveDir = sys.env.getOrElse("CURVE_DIR", "/tmp/shortvol_sweep")
  java.nio.file.Files.createDirectories(java.nio.file.Path.of(curveDir))

  val backend = DefaultSyncBackend()
  val symbolMetas = ShortVolHedgeRunner.fetchSymbolMetas(backend)

  def annualizedRv(prices: Vector[Double]): Double =
    val rets = prices.sliding(2).collect { case Vector(a, b) if a > 0 && b > 0 => math.log(b / a) }.toVector
    if rets.sizeIs < 2 then 0.0
    else
      val mean = rets.sum / rets.size
      val variance = rets.map(r => (r - mean) * (r - mean)).sum / (rets.size - 1)
      math.sqrt(variance) * math.sqrt(365.0 * 24.0)

  def maxDrawdown(equities: Vector[Double]): Double =
    var peak = Double.NegativeInfinity
    var mdd = 0.0
    equities.foreach { e =>
      if e > peak then peak = e
      if peak > 0 then mdd = math.max(mdd, (peak - e) / peak)
    }
    mdd

  final case class Row(
      label: String, windowH: Double, thrPct: Double,
      optionPnl: Double, hedgePnl: Double, totalPnl: Double,
      retPct: Double, fills: Int, maxDdPct: Double, rvPct: Double,
  )

  def runOne(label: String, windowH: Double, thrPct: Double): Row =
    val windowBars = math.max(1, (windowH * 60).round.toInt) // 1min bar
    val factory: (Exchange, Symbol, String, HedgeExecution) => Strategy =
      (ex, sym, c, exec) =>
        BreakoutHedgeStrategy(
          ex, sym, c, exec,
          optionPositionEth = optionPositionEth,
          windowBars = windowBars,
          maxThresholdPct = None,
          minThresholdPct = thrPct,
        )
    val params = ShortVolParams(
      start = start, end = end, strategyFactory = factory,
      feeRate = feeRate, useMarket = true, impliedVol = impliedVol, straddles = straddles,
    )
    val src = ShortVolHedgeRunner.tradeSourceFactory(backend, start, end)
    val out = ShortVolHedgeRunner.run(symbolMetas, src, params)
    val w = java.io.PrintWriter(s"$curveDir/equity_$label.csv")
    w.println("ts,totalEquity,price,optionPnl,hedgeEquity")
    out.equityCurve.foreach(p => w.println(s"${p.ts},${p.totalEquity},${p.price},${p.optionPnl},${p.hedgeEquity}"))
    w.close()
    Row(
      label = label, windowH = windowH, thrPct = thrPct,
      optionPnl = out.optionPnl, hedgePnl = out.hedgePnl, totalPnl = out.totalPnl,
      retPct = out.totalPnl / initBalance * 100.0, fills = out.fills,
      maxDdPct = maxDrawdown(out.equityCurve.map(_.totalEquity)) * 100.0,
      rvPct = annualizedRv(out.equityCurve.map(_.price)) * 100.0,
    )

  val days = ChronoUnit.DAYS.between(start, end) + 1
  println()
  println("==================== ShortVol 突破闸门 (窗口 × 阈值) 扫描 ====================")
  println(f"周期=[$start .. $end] ($days 天)  IV=${impliedVol * 100}%.0f%%  straddles=$straddles%.0f (short)  执行=市价(taker) fee=${feeRate * 100}%.3f%%  本金=$initBalance%.0f")
  println(s"曲线 CSV -> $curveDir/equity_{${combos.map(_._1).mkString(",")}}.csv")

  val pool = Executors.newFixedThreadPool(math.min(combos.size, Runtime.getRuntime.availableProcessors))
  given ec: ExecutionContext = ExecutionContext.fromExecutor(pool)
  val rows =
    try Await.result(Future.sequence(combos.map((l, w, t) => Future(runOne(l, w, t)))), Duration.Inf)
    finally pool.shutdown()

  // 期权腿不变性自检 (同周期+IV 下各组合期权腿应相等)
  val optLegs = rows.map(_.optionPnl)
  val optInvariant = optLegs.forall(o => math.abs(o - optLegs.head) < 1.0)
  val rv = rows.head.rvPct

  println("-" * 96)
  println(f"已实现波动 RV(年化) ≈ ${rv}%.1f%%  vs  IV=${impliedVol * 100}%.0f%%  | 期权腿(各组合恒等)≈${optLegs.head}%+.1f  一致性=${if optInvariant then "OK" else "!!不一致!!"}")
  println("-" * 96)
  println(f"${"组合"}%-8s ${"窗口"}%5s ${"阈值"}%6s ${"对冲腿"}%11s ${"完整PnL"}%11s ${"收益率"}%8s ${"对冲数"}%7s ${"最大回撤"}%8s")
  rows.sortBy(-_.totalPnl).foreach { r =>
    println(
      f"${r.label}%-8s ${r.windowH}%4.0fh ${r.thrPct * 100}%5.0f%% ${r.hedgePnl}%+11.1f ${r.totalPnl}%+11.1f ${r.retPct}%+7.2f%% ${r.fills}%7d ${r.maxDdPct}%7.2f%%"
    )
  }
  println("-" * 96)
  val best = rows.maxBy(_.totalPnl)
  println(f"best: ${best.label}%s (窗口${best.windowH}%.0fh 阈值${best.thrPct * 100}%.0f%%) -> 完整PnL ${best.totalPnl}%+.1f (${best.retPct}%+.2f%%)")
  println("说明: 期权腿与对冲规则无关(恒等, 可作一致性校验); 组合差异全在对冲腿(gamma滑点+手续费)。")
  println("============================================================================")
