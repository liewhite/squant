package hft.demo.edge

import hft.backtest.{BacktestEngine, BinanceDataKind, BinanceHistory, BsGreeksConfig, BsGreeksSource, TradePrintBboSource}
import hft.domain.*
import hft.engine.StrategyRunner
import hft.indicator.RealizedVol
import hft.messaging.{EventData, IncomeEvent}
import hft.sim.SimConfig
import hft.strategy.edge.{BandHedgeStrategy, SymmetricAtrBand}
import sttp.client4.DefaultSyncBackend

import java.nio.file.{Files, Path}
import java.time.{LocalDate, ZoneOffset}
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

/** **按周滚动 + IV 网格仓位** 的买方回测——检验"按上周 RV 定 IV、并按波动贵贱缩放仓位"的择时边际。
  *
  * 规则 (全部只用过去信息、不预测未来 IV)：
  *   - 每周独立开一份 ATM 长跨式, 持有到本周末。**IV = 上一周的已实现波动 RV**；首周无上周, 用种子 IV(默认 0.55)。
  *   - **仓位策略 [[WeeklyIvGrid.SizePolicy]]** (EDGE_SIZE_MODE)：
  *       `step`(默认)=两档网格 (波动↓买 [[gridUp]]×、↑买 [[gridDown]]×)；
  *       `drop`=只在波动下降周建仓、**越跌越买** (倍数=clamp(scale×相对跌幅,0,cap), 上升/首周不建仓)。
  *   - **对冲开关** (EDGE_HEDGE)：`on`(默认)=对称 ATR delta 对冲；`off`=纯买入持有到期、**不做 scalping**
  *     (无下单, total=期权腿)。不对冲时 minTenor 默认 0.02d 使终值≈内在价值——注意仍含约 minTenor 的残留
  *     时间价值, 对买方**轻微偏多**(非精确到期内在价值)。
  *
  * 保留详细数据供分析：周度汇总 / 小时净值曲线 / 逐笔对冲成交。汇总解析"建仓周内恒定1×"对照
  * (P&L 随份数线性, total/mult), 看越跌越买的缩放贡献。
  *
  * 运行: sbt "runMain hft.demo.edge.WeeklyIvGridBacktest"
  * 可调 env: EDGE_SIZE_MODE(step|drop) / EDGE_HEDGE(on|off) / EDGE_SEED_IV / EDGE_GRID_UP / EDGE_GRID_DOWN /
  *           EDGE_DROP_SCALE / EDGE_DROP_CAP / EDGE_ATR_MULT / EDGE_MIN_TENOR_DAYS / EDGE_STRADDLES /
  *           EDGE_TAKER_FEE / EDGE_DELAY_MS / EDGE_MAX_WEEKS / EDGE_WEEKLY_CSV / EDGE_CURVE_CSV / EDGE_FILLS_CSV / EDGE_PAR / DATA_CACHE
  */
@main def WeeklyIvGridBacktest(args: String*): Unit =
  System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn")

  val symbol = "ETHUSDT"
  val ccy = "ETH"
  val cacheDir = sys.env.getOrElse("DATA_CACHE", "data-cache")
  val baseStraddles = sys.env.get("EDGE_STRADDLES").map(_.toDouble).getOrElse(10.0)
  val seedIv = sys.env.get("EDGE_SEED_IV").map(_.toDouble).getOrElse(0.55)
  val gridUp = sys.env.get("EDGE_GRID_UP").map(_.toDouble).getOrElse(1.5)    // 波动下降 -> 多买
  val gridDown = sys.env.get("EDGE_GRID_DOWN").map(_.toDouble).getOrElse(0.75) // 波动上升 -> 少买
  val dropScale = sys.env.get("EDGE_DROP_SCALE").map(_.toDouble).getOrElse(5.0)
  val dropCap = sys.env.get("EDGE_DROP_CAP").map(_.toDouble).getOrElse(3.0)
  // 仓位策略: step(两档网格) | drop(只买波动下降周, 越跌越买)
  val sizePolicy: WeeklyIvGrid.SizePolicy = sys.env.getOrElse("EDGE_SIZE_MODE", "step") match
    case "step" => WeeklyIvGrid.StepGrid(gridUp, gridDown)
    case "drop" => WeeklyIvGrid.DropOnly(dropScale, dropCap)
    case other  => sys.error(s"unknown EDGE_SIZE_MODE='$other' (step|drop)")
  val atrMult = sys.env.get("EDGE_ATR_MULT").map(_.toDouble).getOrElse(2.0)
  // 对冲开关: on=对称ATR delta 对冲; off=纯买入持有到期不对冲 (无 scalping)
  val hedgeOn = sys.env.getOrElse("EDGE_HEDGE", "on") != "off"
  // 剩余期限下限 (天): 不对冲时设小 -> 终值≈内在价值 (真实持有到期); 对冲时保 1 天避奇点
  val minTenorDays = sys.env.get("EDGE_MIN_TENOR_DAYS").map(_.toDouble).getOrElse(if hedgeOn then 1.0 else 0.02)
  val initialBalance = 1_000_000.0
  val takerFee = sys.env.get("EDGE_TAKER_FEE").map(_.toDouble).getOrElse(0.0)
  val delayMs = sys.env.get("EDGE_DELAY_MS").map(_.toLong).getOrElse(0L)
  val weeklyCsv = sys.env.getOrElse("EDGE_WEEKLY_CSV", "/tmp/weekly_grid.csv")
  val curveCsv = sys.env.getOrElse("EDGE_CURVE_CSV", "/tmp/weekly_curve.csv")
  val fillsCsv = sys.env.getOrElse("EDGE_FILLS_CSV", "/tmp/weekly_fills.csv")

  val meta = SymbolMeta(Exchange.Binance, symbol, tickSize = 0.01, sizeStep = 0.001, minOrderSize = 0.001, contractSize = 1.0)
  val symbolMetas = Map((Exchange.Binance, symbol) -> meta)

  val maxWeeks = sys.env.get("EDGE_MAX_WEEKS").map(_.toInt).getOrElse(Int.MaxValue)
  val weeks = completeWeeks(cacheDir, symbol).take(maxWeeks)
  if weeks.isEmpty then sys.error(s"no complete cached weeks for $symbol under $cacheDir")

  val sizeDesc = sys.env.getOrElse("EDGE_SIZE_MODE", "step") match
    case "drop" => f"drop(只买波动↓周, scale=$dropScale%.1f cap=$dropCap%.1f)"
    case _      => f"step(波动↓买${gridUp}×/↑买${gridDown}×)"
  println("==================== 周度 IV 网格回测 (买方) ====================")
  println(f"symbol=$symbol  周数=${weeks.size} (${weeks.head._1}..${weeks.last._2})  种子IV=$seedIv%.2f  仓位=$sizeDesc")
  println(f"基准份数=$baseStraddles  对冲=${if hedgeOn then f"对称ATR(${atrMult}%.1f)" else "无(买入持有到期)"}  minTenor=${minTenorDays}d  takerFee=${takerFee * 100}%.3f%%  delay=${delayMs}ms")
  if !hedgeOn then println(f"注: 持有到期为近似——终值含约 ${minTenorDays}%.2fd 残留时间价值, 对买方轻微偏多 (非精确到期内在价值)")

  def tradeSource(backend: sttp.client4.SyncBackend, start: LocalDate, end: LocalDate) =
    BinanceHistory.source(backend, Seq(symbol), start, end, kinds = Seq(BinanceDataKind.Trades), cacheDir = cacheDir)

  /** 周度预扫：小时采样年化 RV + 首价 */
  def prepass(start: LocalDate, end: LocalDate): (Double, Double) =
    val backend = DefaultSyncBackend()
    try
      val samples = ArrayBuffer.empty[Double]
      var lastTs = 0L
      var first = 0.0
      val it = tradeSource(backend, start, end).events()
      while it.hasNext do
        it.next().data match
          case EventData.MarketTradeUpdate(t) =>
            if first == 0.0 then first = t.price
            if t.timestamp - lastTs >= 3_600_000L then { lastTs = t.timestamp; samples += t.price }
          case _ => ()
      val rv = RealizedVol.annualizedFromPrices(samples.toVector, 365.0 * 24.0)
      if rv <= 0.0 || samples.sizeIs < 25 then
        System.err.println(f"[WARN] [$start..$end] 周采样异常: 小时采样=${samples.size} RV=$rv%.4f -> 作为下周 IV 不可信")
      (rv, first)
    finally backend.close()

  /** 单周回测 (对称 ATR 对冲)，捕获曲线 + 逐笔成交 */
  def runWeek(start: LocalDate, end: LocalDate, iv: Double, straddles: Double): WeekResult =
    val backend = DefaultSyncBackend()
    try
      val expiryMs = end.plusDays(1).atStartOfDay.toInstant(ZoneOffset.UTC).toEpochMilli
      val cfg = BsGreeksConfig(
        exchange = Exchange.Binance, ccy = ccy, underlyingSymbol = symbol,
        straddles = straddles, impliedVol = iv, expiry = expiryMs,
        riskFreeRate = 0.0, spotHolding = 0.0, emitIntervalMs = 1000, minTenorDays = minTenorDays,
      )
      val withGreeks = BsGreeksSource(tradeSource(backend, start, end), cfg)
      val source = TradePrintBboSource(withGreeks)
      // 对冲关 -> 无 runner (纯期权腿持有到期); 开 -> 对称 ATR delta 对冲
      val runners =
        if hedgeOn then Seq(StrategyRunner.backtest(BandHedgeStrategy(Exchange.Binance, symbol, ccy, SymmetricAtrBand(atrMult)), symbolMetas))
        else Seq.empty

      var lastMid = 0.0; var lastTs = 0L
      var curveLastTs = 0L
      val curve = ArrayBuffer.empty[(Long, Double, Double)]
      val fillRecs = ArrayBuffer.empty[(Long, Side, Double, Double)]
      val obs: IncomeEvent => Unit = ev =>
        ev.data match
          case EventData.BboUpdate(b) => lastMid = b.midPrice; lastTs = b.timestamp
          case EventData.FillUpdate(f) => fillRecs += ((f.timestamp, f.side, f.price, f.size))
          case EventData.AccountInfoUpdate(_, info) =>
            if lastTs > 0 && ev.exchangeTs - curveLastTs >= 3_600_000L then
              curveLastTs = ev.exchangeTs
              curve += ((ev.exchangeTs, withGreeks.optionPnl(lastMid, lastTs), info.equity - initialBalance))
          case _ => ()

      val engine = BacktestEngine(
        exchange = Exchange.Binance, source = source, runners = runners,
        config = SimConfig(
          exchangeToStrategyDelayMs = 0, orderToExchangeDelayMs = delayMs,
          initialBalanceUsdt = initialBalance, makerFeeRate = 0.0, takerFeeRate = takerFee,
        ),
        observers = Seq(obs),
      )
      val result = engine.run()
      val optionPnl = withGreeks.optionPnl(lastMid, lastTs)
      val hedgePnl = result.finalEquity - result.initialBalance
      WeekResult(optionPnl, hedgePnl, optionPnl + hedgePnl, withGreeks.enteredPremium, result.fills,
        withGreeks.strikePrice, lastMid, curve.toVector, fillRecs.toVector)
    finally backend.close()

  val parallelism = sys.env.get("EDGE_PAR").map(_.toInt).getOrElse(math.min(Runtime.getRuntime.availableProcessors, 6))
  val pool = Executors.newFixedThreadPool(parallelism)
  given ec: ExecutionContext = ExecutionContext.fromExecutor(pool)
  val t0 = System.nanoTime()

  try
    // ① 并行预扫每周 RV
    val rvByWeek: Map[Int, (Double, Double)] =
      Await.result(
        Future.sequence(weeks.zipWithIndex.map { case ((s, e), i) => Future(i -> prepass(s, e)) }),
        Duration.Inf,
      ).toMap

    // ② 顺序定每周 IV 与仓位倍数 (纯逻辑见 WeeklyIvGrid.planWeek, 只用过去周 RV, 无前视)
    val plans: Seq[WeekPlan] = weeks.indices.map { i =>
      val p = WeeklyIvGrid.planWeek(i, seedIv, j => rvByWeek(j)._1, sizePolicy)
      WeekPlan(i, weeks(i)._1, weeks(i)._2, rvByWeek(i)._1, p.iv, p.ivPrev, p.mult, baseStraddles * p.mult)
    }
    val active = plans.filter(_.straddles > 0.0) // 倍数=0 的周不建仓 -> 跳过实跑 (盈亏 0)

    // ③ 并行跑建仓周 (跳过的周记零结果)
    val done = AtomicInteger(0)
    val activeRecs = Await.result(
      Future.sequence(active.map { p =>
        Future {
          val r = runWeek(p.start, p.end, p.iv, p.straddles)
          val k = done.incrementAndGet()
          println(f"  [$k%2d/${active.size}] ${p.start} iv=${p.iv}%.3f ×${p.mult}%.2f 总=${r.total}%+9.1f (${pct(r.total, r.premium)}%+6.2f%%) fills=${r.fills}")
          WeekRec(p, r)
        }
      }),
      Duration.Inf,
    ).map(rc => rc.plan.idx -> rc).toMap
    val recs = plans.map(p => activeRecs.getOrElse(p.idx, WeekRec(p, WeekResult(0, 0, 0, 0, 0, 0, 0, Vector.empty, Vector.empty))))

    writeWeekly(weeklyCsv, recs)
    writeCurve(curveCsv, recs)
    writeFills(fillsCsv, recs)
    summarize(recs)
    println(f"\n周度=$weeklyCsv  曲线=$curveCsv  成交=$fillsCsv   用时 ${(System.nanoTime() - t0) / 1e9}%.1fs (并行度 $parallelism)")
  finally pool.shutdown()

final case class WeekPlan(idx: Int, start: LocalDate, end: LocalDate, rv: Double, iv: Double, ivPrev: Double, mult: Double, straddles: Double)
final case class WeekResult(
    optionPnl: Double, hedgePnl: Double, total: Double, premium: Double, fills: Int, startPx: Double, endPx: Double,
    curve: Vector[(Long, Double, Double)], fillRecs: Vector[(Long, Side, Double, Double)],
)
final case class WeekRec(plan: WeekPlan, r: WeekResult)

// 以权利金**绝对值**为基准 (卖方 enteredPremium 为负=收到的权利金, 取绝对值得回报率)
private def pct(x: Double, base: Double): Double = if base != 0.0 then x / math.abs(base) * 100.0 else 0.0

private def writeWeekly(path: String, recs: Seq[WeekRec]): Unit =
  val pw = java.io.PrintWriter(path)
  try
    pw.println("idx,weekStart,weekEnd,rvThisWeek,ivUsed,ivPrev,mult,straddles,premium,optionPnl,hedgePnl,total,totalPctPrem,fills,startPx,endPx")
    recs.foreach { case WeekRec(p, r) =>
      pw.println(f"${p.idx},${p.start},${p.end},${p.rv}%.4f,${p.iv}%.4f,${p.ivPrev}%.4f,${p.mult}%.2f,${p.straddles}%.3f,${r.premium}%.2f,${r.optionPnl}%.2f,${r.hedgePnl}%.2f,${r.total}%.2f,${pct(r.total, r.premium)}%.2f,${r.fills},${r.startPx}%.2f,${r.endPx}%.2f")
    }
  finally pw.close()

/** 跨周拼接的累计净值曲线 (期权腿 / 对冲腿 / 总)，最旧->最新 */
private def writeCurve(path: String, recs: Seq[WeekRec]): Unit =
  val pw = java.io.PrintWriter(path)
  try
    pw.println("ts,date,cumOption,cumHedge,cumTotal")
    var carryOpt = 0.0; var carryHed = 0.0
    recs.foreach { case WeekRec(_, r) =>
      r.curve.foreach { case (ts, opt, hed) =>
        val co = carryOpt + opt; val ch = carryHed + hed
        pw.println(f"$ts,${java.time.Instant.ofEpochMilli(ts)},$co%.2f,$ch%.2f,${co + ch}%.2f")
      }
      carryOpt += r.optionPnl; carryHed += r.hedgePnl
    }
  finally pw.close()

/** 逐笔对冲成交 (供分析换手/滑点) */
private def writeFills(path: String, recs: Seq[WeekRec]): Unit =
  val pw = java.io.PrintWriter(path)
  try
    pw.println("weekStart,ts,date,side,price,size")
    recs.foreach { case WeekRec(p, r) =>
      r.fillRecs.foreach { case (ts, side, px, sz) =>
        pw.println(f"${p.start},$ts,${java.time.Instant.ofEpochMilli(ts)},$side,$px%.2f,$sz%.4f")
      }
    }
  finally pw.close()

/** 汇总：整体盈亏/占比/胜率, 建仓周占比, 及"建仓周内"的仓位缩放贡献 (flat=Σ建仓周盈亏/倍数, 仅 mult>0) */
private def summarize(recs: Seq[WeekRec]): Unit =
  val traded = recs.filter(_.plan.mult > 0.0)
  val total = recs.map(_.r.total).sum
  val prem = recs.map(_.r.premium).sum
  val wins = traded.count(_.r.total > 0)
  // 建仓周内的"恒定1×"等价 (P&L 随份数线性, total/mult), 与实际加权对比看缩放贡献
  val flatTotal = traded.map(rc => rc.r.total / rc.plan.mult).sum
  val flatPrem = traded.map(rc => rc.r.premium / rc.plan.mult).sum
  println("\n==================== 汇总 ====================")
  println(f"周数=${recs.size}  建仓周=${traded.size}  胜周(建仓内)=$wins/${traded.size}")
  println(f"实际仓位 : Σ总=$total%+.1f  Σ权利金=$prem%.1f  占权利金=${pct(total, prem)}%+.2f%%")
  if traded.nonEmpty then
    println(f"建仓周内恒定1× : Σ总=$flatTotal%+.1f  占权利金=${pct(flatTotal, flatPrem)}%+.2f%%  (与实际比看越跌越买的缩放贡献)")

/** 缓存中该 symbol 连续完整 (每天都有 trades 文件) 的 7 天窗口 (窗口切分纯逻辑见 [[WeeklyIvGrid.weekWindows]]) */
private def completeWeeks(cacheDir: String, symbol: Symbol): Seq[(LocalDate, LocalDate)] =
  val dir = Path.of(cacheDir, "futures", "um", "daily", "trades", symbol)
  if !Files.isDirectory(dir) then Seq.empty
  else
    import scala.jdk.CollectionConverters.*
    val stream = Files.list(dir)
    try
      val dates = stream.iterator.asScala
        .map(_.getFileName.toString)
        .flatMap(f => raw"(\d{4}-\d{2}-\d{2})".r.findFirstIn(f))
        .map(LocalDate.parse)
        .toSet
      WeeklyIvGrid.weekWindows(dates)
    finally stream.close()
