package hft.demo.edge

import hft.backtest.{BacktestEngine, BinanceDataKind, BinanceHistory, BsGreeksConfig, BsGreeksSource, TradePrintBboSource}
import hft.domain.*
import hft.engine.StrategyRunner
import hft.indicator.RealizedVol
import hft.messaging.{EventData, IncomeEvent}
import hft.sim.SimConfig
import hft.strategy.edge.{CompositeBand, DirectionalBand, HedgeBand, MaSideBand, SymmetricAtrBand, VolRegimeBand}
import sttp.client4.DefaultSyncBackend

import java.nio.file.{Files, Path}
import java.time.{LocalDate, YearMonth, ZoneOffset}
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

/** 买方 edge 月度滚动回测——在已缓存的整段时间上逐月回测，对照基线与各 edge 变体。
  *
  * 实验设计：
  *   - 标的 ETHUSDT，每月独立开一份 ATM 长跨式持有到月末 (期权腿 BS 估值)，对冲腿走撮合 (fee=0、delay=0)。
  *   - **IV 取上一月的实现波动 (trailing RV)**——可实现、不前视，且因波动均值回复天然产生 IV>RV / IV<RV
  *     混合月份，正适合检验"IV>RV 少亏、IV<RV 多赚、整体期望为正"。可改 `fair`(IV=本月RV) /
  *     `inflated:<f>`(IV=本月RV×f，模拟买方溢价超买的压力测试)。
  *   - 各策略**同月用同一 IV** (IV 是市场输入)，仅对冲腿不同，便于把 edge 归因到对冲带本身。
  *
  * 对照变体 (均价格主导触发、delta 定量 take、成交后中心重置；区别只在[[HedgeBand]])：
  *   baseline-sym (对称基线) / vol-regime (波动放大收带) / directional (MACD 方向不对称) / composite (二者叠加)。
  *
  * 运行: sbt "runMain hft.demo.edge.EdgeRollingBacktest [trailing|fair|inflated:1.2] [startMonth yyyy-MM] [endMonth yyyy-MM]"
  */
@main def EdgeRollingBacktest(args: String*): Unit =
  System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn")

  val symbol = "ETHUSDT"
  val ccy = "ETH"
  val cacheDir = sys.env.getOrElse("DATA_CACHE", "data-cache")
  val csvPath = sys.env.getOrElse("EDGE_CSV", "/tmp/edge_rolling.csv")
  val straddles = sys.env.get("EDGE_STRADDLES").map(_.toDouble).getOrElse(10.0)
  val atrMult = 2.0
  val initialBalance = 1_000_000.0

  val ivMode = IvMode.parse(args.lift(0).getOrElse("trailing"))

  val meta = SymbolMeta(Exchange.Binance, symbol, tickSize = 0.01, sizeStep = 0.001, minOrderSize = 0.001, contractSize = 1.0)
  val symbolMetas = Map((Exchange.Binance, symbol) -> meta)

  // edge 变体 (基线在首，作对照)；EDGE_VARIANTS 逗号列表可子集过滤 (默认全部)
  val allVariants: Seq[(String, HedgeBand)] = Seq(
    "baseline-sym" -> SymmetricAtrBand(atrMult),
    "vol-regime"   -> VolRegimeBand(atrMult, minFactor = 0.5, maxFactor = 2.0),
    "directional"  -> DirectionalBand(atrMult, skew = 0.5),
    "composite"    -> CompositeBand(atrMult, minFactor = 0.5, maxFactor = 2.0, skew = 0.5),
    "ma-side"      -> MaSideBand(atrMult, skew = 0.5),
  )
  val variants: Seq[(String, HedgeBand)] = sys.env.get("EDGE_VARIANTS") match
    case Some(csv) =>
      val keep = csv.split(",").map(_.trim).toSet
      val picked = allVariants.filter((n, _) => keep(n))
      if picked.isEmpty then sys.error(s"EDGE_VARIANTS='$csv' 未匹配任何变体 (${allVariants.map(_._1).mkString(",")})")
      picked
    case None => allVariants

  // 缓存中完整的月份 (该月每一天都有 trades 文件)
  val allMonths = completeMonths(cacheDir, symbol)
  val startBound = args.lift(1).map(YearMonth.parse)
  val endBound = args.lift(2).map(YearMonth.parse)
  val months = allMonths.filter(m => startBound.forall(!m.isBefore(_)) && endBound.forall(!m.isAfter(_)))
  if months.isEmpty then sys.error(s"no complete cached months for $symbol under $cacheDir (filter $startBound..$endBound)")

  println(s"==================== Edge 月度滚动回测 (买方 long-gamma) ====================")
  println(s"symbol=$symbol  月份=${months.head}..${months.last} (${months.size} 个)  IV=$ivMode  straddles=$straddles  fee=0 delay=0")
  println(s"变体: ${variants.map(_._1).mkString(", ")}")

  // 每个并行任务各建独立 backend (sttp SyncBackend 不保证可并发复用)，try/finally 关闭——
  // 月份/变体已缓存数据相互独立, 并发只读不同/相同文件均安全, 不共享可变状态。
  def tradeSource(backend: sttp.client4.SyncBackend, start: LocalDate, end: LocalDate) =
    BinanceHistory.source(backend, Seq(symbol), start, end, kinds = Seq(BinanceDataKind.Trades), cacheDir = cacheDir)

  /** 月度预扫：小时采样年化实现波动 RV + 首笔成交价 (ATM 行权) */
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
      val rets = samples.toVector.sliding(2).collect { case Vector(a, b) if a > 0 && b > 0 => math.log(b / a) }.toVector
      val rv = RealizedVol.annualized(rets, 365.0 * 24.0)
      if rv <= 0.0 || rets.sizeIs < 24 then
        System.err.println(f"[WARN] [$start..$end] 预扫样本异常: 小时采样=${samples.size} 收益=${rets.size} RV=$rv%.4f -> IV 可能退化, 该月结果不可信")
      (rv, first)
    finally backend.close()

  /** 单月单变体回测，返回 (期权腿, 对冲腿, 总, 入场权利金, fills, 末价) */
  def runMonth(band: HedgeBand, start: LocalDate, end: LocalDate, iv: Double): RunResult =
    val backend = DefaultSyncBackend()
    try
      val expiryMs = end.plusDays(1).atStartOfDay.toInstant(ZoneOffset.UTC).toEpochMilli
      val cfg = BsGreeksConfig(
        exchange = Exchange.Binance, ccy = ccy, underlyingSymbol = symbol,
        straddles = straddles, impliedVol = iv, expiry = expiryMs,
        riskFreeRate = 0.0, spotHolding = 0.0, emitIntervalMs = 1000,
      )
      val withGreeks = BsGreeksSource(tradeSource(backend, start, end), cfg)
      val source = TradePrintBboSource(withGreeks)
      val strategy = hft.strategy.edge.BandHedgeStrategy(Exchange.Binance, symbol, ccy, band)
      val runner = StrategyRunner.backtest(strategy, symbolMetas)

      var lastMid = 0.0; var lastTs = 0L
      val obs: IncomeEvent => Unit = ev =>
        ev.data match
          case EventData.BboUpdate(b) => lastMid = b.midPrice; lastTs = b.timestamp
          case _                      => ()

      val engine = BacktestEngine(
        exchange = Exchange.Binance, source = source, runners = Seq(runner),
        config = SimConfig(
          exchangeToStrategyDelayMs = 0, orderToExchangeDelayMs = 0,
          initialBalanceUsdt = initialBalance, makerFeeRate = 0.0, takerFeeRate = 0.0,
        ),
        observers = Seq(obs),
      )
      val result = engine.run()

      val optionPnl = withGreeks.optionPnl(lastMid, lastTs)
      val hedgePnl = result.finalEquity - result.initialBalance
      // 入场权利金取数据源内部真实值 (SSOT, 与 optionPnl 同源, 口径一致)
      RunResult(optionPnl, hedgePnl, optionPnl + hedgePnl, withGreeks.enteredPremium, result.fills, lastMid)
    finally backend.close()

  // 月份/变体相互独立、每个回测确定性 -> 可并行 (结果与并行顺序无关)。
  // 并行度默认 = min(核数, 6) 兼顾吞吐与磁盘/内存; EDGE_PAR 可覆盖。
  val parallelism = sys.env.get("EDGE_PAR").map(_.toInt).getOrElse(math.min(Runtime.getRuntime.availableProcessors, 6))
  val pool = Executors.newFixedThreadPool(parallelism)
  given ec: ExecutionContext = ExecutionContext.fromExecutor(pool)
  val t0 = System.nanoTime()

  try
    // ① 并行预扫各月 RV (+ 首价)，trailing IV 需先有全部 RV
    val prepassByMonth: Map[YearMonth, (Double, Double)] =
      Await.result(Future.sequence(months.map(ym => Future(ym -> prepass(ym.atDay(1), ym.atEndOfMonth())))), Duration.Inf).toMap
    val rvOf: YearMonth => Double = prepassByMonth(_)._1

    // 按 IV 政策定每月 IV (trailing 用上一月 RV；首月退化为本月 RV)
    val ivByMonth: Map[YearMonth, Double] = months.zipWithIndex.map { case (ym, i) =>
      val iv = ivMode match
        case IvMode.Trailing    => if i == 0 then rvOf(ym) else rvOf(months(i - 1))
        case IvMode.Fair        => rvOf(ym)
        case IvMode.Inflated(f) => rvOf(ym) * f
      ym -> iv
    }.toMap

    // ② 并行跑 (月 × 变体) 网格
    val jobs = for ym <- months; (name, band) <- variants yield (ym, name, band)
    val done = AtomicInteger(0)
    val records = Await.result(
      Future.sequence(jobs.map { case (ym, name, band) =>
        Future {
          val r = runMonth(band, ym.atDay(1), ym.atEndOfMonth(), ivByMonth(ym))
          val k = done.incrementAndGet()
          println(f"  [$k%2d/${jobs.size}] $ym $name%-13s 总=${r.total}%+10.1f (${r.totalPct}%+6.2f%%) fills=${r.fills}")
          Record(ym, rvOf(ym), ivByMonth(ym), name, r)
        }
      }),
      Duration.Inf,
    )

    // 按月、变体顺序汇总打印
    val variantOrder = variants.map(_._1).zipWithIndex.toMap
    val sorted = records.sortBy(rec => (rec.month, variantOrder(rec.variant)))
    println("\n-------------------- 明细 (按月) --------------------")
    sorted.groupBy(_.month).toSeq.sortBy(_._1).foreach { case (ym, rs) =>
      val rv = rs.head.rv; val iv = rs.head.iv
      println(f"[$ym] RV=$rv%.4f IV=$iv%.4f (IV-RV=${iv - rv}%+.4f)")
      rs.sortBy(r => variantOrder(r.variant)).foreach { rec =>
        val r = rec.r
        println(f"  ${rec.variant}%-13s 期权=${r.optionPnl}%+10.1f 对冲=${r.hedgePnl}%+10.1f 总=${r.total}%+10.1f (${r.totalPct}%+6.2f%%) fills=${r.fills}")
      }
    }

    writeCsv(csvPath, sorted)
    printSummary(variants.map(_._1), sorted)
    println(f"\n明细 CSV (${sorted.size} 行) -> $csvPath   用时 ${(System.nanoTime() - t0) / 1e9}%.1fs (并行度 $parallelism)")
  finally pool.shutdown()

/** 单月单变体结果 */
final case class RunResult(optionPnl: Double, hedgePnl: Double, total: Double, premium: Double, fills: Int, endPx: Double):
  /** 总盈亏占入场权利金的比例 (%)，权利金<=0 时为 0 */
  def totalPct: Double = if premium > 0 then total / premium * 100.0 else 0.0

final case class Record(month: YearMonth, rv: Double, iv: Double, variant: String, r: RunResult)

/** IV 政策 */
enum IvMode:
  case Trailing            // IV = 上一月 RV (默认, 可实现)
  case Fair                // IV = 本月 RV
  case Inflated(factor: Double) // IV = 本月 RV × factor (买方溢价压力测试)
object IvMode:
  def parse(s: String): IvMode = s match
    case "trailing"                          => Trailing
    case "fair"                              => Fair
    case x if x.startsWith("inflated:")      => Inflated(x.stripPrefix("inflated:").toDouble)
    case other                               => sys.error(s"unknown ivMode '$other' (trailing|fair|inflated:<f>)")
  given Conversion[IvMode, String] = _.toString

/** 缓存中该 symbol 完整 (每天都有 trades 文件) 的月份，升序 */
private def completeMonths(cacheDir: String, symbol: Symbol): Seq[YearMonth] =
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
        .toSeq
      dates.groupBy(YearMonth.from)
        .collect { case (ym, ds) if ds.toSet.size == ym.lengthOfMonth() => ym }
        .toSeq.sorted
    finally stream.close()

private def writeCsv(path: String, records: Seq[Record]): Unit =
  val pw = java.io.PrintWriter(path)
  try
    pw.println("month,rv,iv,iv_minus_rv,variant,optionPnl,hedgePnl,total,premium,totalPct,fills,endPx")
    records.foreach { rec =>
      val r = rec.r
      pw.println(f"${rec.month},${rec.rv}%.4f,${rec.iv}%.4f,${rec.iv - rec.rv}%.4f,${rec.variant},${r.optionPnl}%.2f,${r.hedgePnl}%.2f,${r.total}%.2f,${r.premium}%.2f,${r.totalPct}%.2f,${r.fills},${r.endPx}%.2f")
    }
  finally pw.close()

/** 汇总：每个变体的总盈亏、占权利金比例、胜率，并按 IV>RV / IV<RV 月份分组 (回答"超买少亏、低估多赚")。 */
private def printSummary(variantNames: Seq[String], records: Seq[Record]): Unit =
  println("\n==================== 汇总 (期望) ====================")
  println(f"${"变体"}%-13s ${"Σ总盈亏"}%12s ${"Σ权利金"}%12s ${"Σ占比%"}%9s ${"胜月"}%7s ${"IV>RV Σ总"}%12s ${"IV<RV Σ总"}%12s")
  for name <- variantNames do
    val rs = records.filter(_.variant == name)
    val sumTotal = rs.map(_.r.total).sum
    val sumPrem = rs.map(_.r.premium).sum
    val wins = rs.count(_.r.total > 0)
    val overpaid = rs.filter(r => r.iv > r.rv).map(_.r.total).sum   // IV>RV: 买方超买, 应"少亏"
    val cheap = rs.filter(r => r.iv <= r.rv).map(_.r.total).sum      // IV<=RV: 买方占便宜, 应"多赚"
    val pct = if sumPrem > 0 then sumTotal / sumPrem * 100.0 else 0.0
    println(f"$name%-13s $sumTotal%+12.1f $sumPrem%12.1f $pct%+8.2f%% ${s"$wins/${rs.size}"}%7s $overpaid%+12.1f $cheap%+12.1f")
  println("说明: Σ占比 = Σ总盈亏 / Σ入场权利金; IV>RV 列应越大越好(超买仍少亏), IV<RV 列应为正(低估多赚)。")
