package app.backtest.edge

import hft.backtest.{BacktestEngine, BinanceDataKind, BinanceHistory, BsGreeksConfig, BsGreeksSource, TradePrintBboSource}
import hft.domain.*
import hft.engine.StrategyRunner
import hft.indicator.RealizedVol
import hft.option.BlackScholes
import hft.messaging.{EventData, IncomeEvent}
import hft.sim.SimConfig
import strategy.live.{MaAsymHedgeBand, MakerHedgeStrategy}
import strategy.research.BandHedgeStrategy
import sttp.client4.DefaultSyncBackend

import java.nio.file.{Files, Path}
import java.time.{LocalDate, ZoneOffset}
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

/** **卖方 short-vol** 周度滚动回测 (镜像买方实验, 收 theta / 赚波动均值回复)。
  *
  * 规则 (只用过去信息)：
  *   - **每周开始卖出一份 21 天 (3 周) ATM 空头跨式**, 持有到该 tranche 到期 (窗口重叠, 每个 tranche 独立模拟)。
  *   - **仓位**: 本周 RV 较上周**上升**→卖 [[gridHigh]]×(默认 2), **下降**→卖 [[gridLow]]×(默认 1)。
  *     (波动刚涨→IV 定得高→卖更多, 赚回落) 用 [[WeeklyIvGrid.StepGrid]](up=低倍, down=高倍) 作用于**负** straddles。
  *   - **对冲**: [[MaAsymHedgeBand]] —— 均线上对上涨紧对冲 (1ATR)、回落松 (2ATR); 均线下反之。
  *   - IV = 上一周已实现 RV (滞后, 不预测未来)。
  *
  * 卖方追求**稳定为正 (高胜率)**, 故重点报: 胜率 / 最差 tranche / 最大回撤 / 均值, 而非仅总收益。
  * 详细数据: tranche 汇总 / 逐笔对冲成交 / 按到期日的累计净值曲线。
  *
  * 运行: sbt "runMain hft.demo.edge.WeeklySellVolBacktest"
  * env: EDGE_STRADDLES(基准份数,取绝对值做空) / EDGE_GRID_HIGH / EDGE_GRID_LOW / EDGE_TIGHT_ATR / EDGE_LOOSE_ATR /
  *      EDGE_TENOR_DAYS(默认21) / EDGE_TAKER_FEE / EDGE_DELAY_MS / EDGE_MAX_TRANCHES / EDGE_*_CSV / EDGE_PAR / DATA_CACHE
  */
@main def WeeklySellVolBacktest(args: String*): Unit =
  System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn")

  val symbol = "ETHUSDT"
  val ccy = "ETH"
  val cacheDir = sys.env.getOrElse("DATA_CACHE", "data-cache")
  val baseStraddles = math.abs(sys.env.get("EDGE_STRADDLES").map(_.toDouble).getOrElse(10.0)) // 卖方→负
  val gridHigh = sys.env.get("EDGE_GRID_HIGH").map(_.toDouble).getOrElse(2.0) // 波动上升 -> 卖更多
  val gridLow = sys.env.get("EDGE_GRID_LOW").map(_.toDouble).getOrElse(1.0)   // 波动下降 -> 卖更少
  val tightAtr = sys.env.get("EDGE_TIGHT_ATR").map(_.toDouble).getOrElse(1.0)
  val looseAtr = sys.env.get("EDGE_LOOSE_ATR").map(_.toDouble).getOrElse(2.0)
  val tenorDays = sys.env.get("EDGE_TENOR_DAYS").map(_.toInt).getOrElse(21)
  val takerFee = sys.env.get("EDGE_TAKER_FEE").map(_.toDouble).getOrElse(0.0)
  val makerFee = sys.env.get("EDGE_MAKER_FEE").map(_.toDouble).getOrElse(0.0)
  val delayMs = sys.env.get("EDGE_DELAY_MS").map(_.toLong).getOrElse(0L)
  // 对冲执行: take(市价吃单) | maker(现价外挂被动单, 省费+赚价差, 5s重挂)
  val hedgeExec = sys.env.getOrElse("EDGE_HEDGE_EXEC", "take")
  val makerOffset = sys.env.get("EDGE_MAKER_OFFSET").map(_.toDouble).getOrElse(0.0002)
  val requoteMs = sys.env.get("EDGE_REQUOTE_MS").map(_.toLong).getOrElse(5000L)
  val maxTranches = sys.env.get("EDGE_MAX_TRANCHES").map(_.toInt).getOrElse(Int.MaxValue)
  val initialBalance = 1_000_000.0
  val weeklyCsv = sys.env.getOrElse("EDGE_WEEKLY_CSV", "/tmp/sellvol_tranches.csv")
  val curveCsv = sys.env.getOrElse("EDGE_CURVE_CSV", "/tmp/sellvol_curve.csv")
  val fillsCsv = sys.env.getOrElse("EDGE_FILLS_CSV", "/tmp/sellvol_fills.csv")
  val tenorWeeks = math.max(1, math.round(tenorDays / 7.0).toInt) // 21d -> 3 周

  val meta = SymbolMeta(Exchange.Binance, symbol, tickSize = 0.01, sizeStep = 0.001, minOrderSize = 0.001, contractSize = 1.0)
  val symbolMetas = Map((Exchange.Binance, symbol) -> meta)
  // 仓位: 波动下降(iv<ivPrev)→gridLow, 上升→gridHigh; StepGrid(up=低,down=高)
  val sizePolicy = WeeklyIvGrid.StepGrid(up = gridLow, down = gridHigh)
  val band = MaAsymHedgeBand(tightMult = tightAtr, looseMult = looseAtr)

  val weeks = WeeklyIvGrid.weekWindows(weekDates(cacheDir, symbol))
  if weeks.sizeIs < tenorWeeks + 2 then sys.error(s"缓存周数不足 (${weeks.size}, 需 >= ${tenorWeeks + 2})")

  println("==================== 卖方 short-vol 周度滚动回测 ====================")
  println(f"symbol=$symbol  周数=${weeks.size}  tenor=${tenorDays}d(${tenorWeeks}周)  基准卖=${baseStraddles}份")
  val execDesc = if hedgeExec == "maker" then f"maker(现价外${makerOffset * 100}%.3f%%, ${requoteMs}ms重挂, makerFee=${makerFee * 100}%.3f%%)" else f"take(taker费${takerFee * 100}%.3f%%)"
  println(f"仓位: 波动↑卖${gridHigh}×/↓卖${gridLow}×   对冲带: 均线上 上${tightAtr}%.1fATR/下${looseAtr}%.1fATR, 均线下反之   执行=$execDesc  delay=${delayMs}ms")

  def tradeSource(backend: sttp.client4.SyncBackend, s: LocalDate, e: LocalDate) =
    BinanceHistory.source(backend, Seq(symbol), s, e, kinds = Seq(BinanceDataKind.Trades), cacheDir = cacheDir)

  def prepass(s: LocalDate, e: LocalDate): Double =
    val backend = DefaultSyncBackend()
    try
      val samples = ArrayBuffer.empty[Double]; var lastTs = 0L
      val it = tradeSource(backend, s, e).events()
      while it.hasNext do
        it.next().data match
          case EventData.MarketTradeUpdate(t) => if t.timestamp - lastTs >= 3_600_000L then { lastTs = t.timestamp; samples += t.price }
          case _                              => ()
      RealizedVol.annualizedFromPrices(samples.toVector, BlackScholes.HoursPerYear)
    finally backend.close()

  /** 单 tranche: 卖空头跨式(负 straddles) + MaAsym 对冲, 持有到 21 天到期 */
  def runTranche(start: LocalDate, end: LocalDate, iv: Double, straddles: Double): WeekResult =
    val backend = DefaultSyncBackend()
    try
      val expiryMs = end.plusDays(1).atStartOfDay.toInstant(ZoneOffset.UTC).toEpochMilli
      val cfg = BsGreeksConfig(
        exchange = Exchange.Binance, ccy = ccy, underlyingSymbol = symbol,
        straddles = straddles, impliedVol = iv, expiry = expiryMs,
        riskFreeRate = 0.0, spotHolding = 0.0, emitIntervalMs = 1000, minTenorDays = 1.0,
      )
      val withGreeks = BsGreeksSource(tradeSource(backend, start, end), cfg)
      val source = TradePrintBboSource(withGreeks)
      val hedgeStrat =
        if hedgeExec == "maker" then MakerHedgeStrategy(Exchange.Binance, symbol, ccy, band, offsetPct = makerOffset, requoteMs = requoteMs)
        else BandHedgeStrategy(Exchange.Binance, symbol, ccy, band)
      val runner = StrategyRunner.backtest(hedgeStrat, symbolMetas)
      var lastMid = 0.0; var lastTs = 0L; var curveLastTs = 0L
      val curve = ArrayBuffer.empty[(Long, Double, Double)]
      val fillRecs = ArrayBuffer.empty[(Long, Side, Double, Double)]
      val obs: IncomeEvent => Unit = ev =>
        ev.data match
          case EventData.BboUpdate(b)  => lastMid = b.midPrice; lastTs = b.timestamp
          case EventData.FillUpdate(f) => fillRecs += ((f.timestamp, f.side, f.price, f.size))
          case EventData.AccountInfoUpdate(_, info) =>
            if lastTs > 0 && ev.exchangeTs - curveLastTs >= 3_600_000L then
              curveLastTs = ev.exchangeTs
              curve += ((ev.exchangeTs, withGreeks.optionPnl(lastMid, lastTs), info.equity - initialBalance))
          case _ => ()
      val engine = BacktestEngine(
        exchange = Exchange.Binance, source = source, runners = Seq(runner),
        config = SimConfig(exchangeToStrategyDelayMs = 0, orderToExchangeDelayMs = delayMs,
          initialBalanceUsdt = initialBalance, makerFeeRate = makerFee, takerFeeRate = takerFee),
        observers = Seq(obs),
      )
      val result = engine.run()
      val optionPnl = withGreeks.optionPnl(lastMid, lastTs)
      val hedgePnl = result.finalEquity - result.initialBalance
      WeekResult(optionPnl, hedgePnl, optionPnl + hedgePnl, withGreeks.enteredPremium, result.fills, withGreeks.strikePrice, lastMid, curve.toVector, fillRecs.toVector)
    finally backend.close()

  val parallelism = sys.env.get("EDGE_PAR").map(_.toInt).getOrElse(math.min(Runtime.getRuntime.availableProcessors, 6))
  val pool = Executors.newFixedThreadPool(parallelism)
  given ec: ExecutionContext = ExecutionContext.fromExecutor(pool)
  val t0 = System.nanoTime()
  try
    // 周 RV (并行预扫)：tranche i 用 RV(i-1)/RV(i-2)
    val rvByWeek = Await.result(Future.sequence(weeks.zipWithIndex.map { case ((s, e), i) => Future(i -> prepass(s, e)) }), Duration.Inf).toMap
    // tranche i: 起于第 i 周, 跨 tenorWeeks 周; 需 i>=2 (RV(i-2)) 且 i+tenorWeeks-1 <= 末周
    val trancheIdx = (2 to (weeks.size - tenorWeeks)).take(maxTranches)
    val plans = trancheIdx.map { i =>
      val p = WeeklyIvGrid.planWeek(i, 0.55, j => rvByWeek(j), sizePolicy)
      val start = weeks(i)._1
      val end = weeks(i + tenorWeeks - 1)._2
      WeekPlan(i, start, end, rvByWeek(i), p.iv, p.ivPrev, p.mult, -baseStraddles * p.mult) // 负=做空
    }
    val done = AtomicInteger(0)
    val recs = Await.result(
      Future.sequence(plans.map { p =>
        Future {
          val r = runTranche(p.start, p.end, p.iv, p.straddles)
          val k = done.incrementAndGet()
          println(f"  [$k%2d/${plans.size}] ${p.start}->${p.end} iv=${p.iv}%.3f 卖${-p.straddles}%.0f份 总=${r.total}%+9.1f (${pct(r.total, r.premium)}%+6.2f%%) fills=${r.fills}")
          WeekRec(p, r)
        }
      }),
      Duration.Inf,
    ).sortBy(_.plan.idx)

    writeWeekly(weeklyCsv, recs)
    writeFills(fillsCsv, recs)
    writeSellCurve(curveCsv, recs)
    summarizeSell(recs)
    println(f"\ntranche=$weeklyCsv  曲线=$curveCsv  成交=$fillsCsv   用时 ${(System.nanoTime() - t0) / 1e9}%.1fs (并行度 $parallelism)")
  finally pool.shutdown()

/** 按到期日排序的累计已实现净值 (每 tranche 一点；tranche 周步进、到期亦有序) */
private def writeSellCurve(path: String, recs: Seq[WeekRec]): Unit =
  val pw = java.io.PrintWriter(path)
  try
    pw.println("trancheStart,trancheEnd,cumTotal,trancheTotal")
    var cum = 0.0
    recs.sortBy(_.plan.end).foreach { case WeekRec(p, r) =>
      cum += r.total
      pw.println(f"${p.start},${p.end},$cum%.2f,${r.total}%.2f")
    }
  finally pw.close()

/** 卖方汇总：稳定性指标 (胜率/最差/回撤/均值) 优先 */
private def summarizeSell(recs: Seq[WeekRec]): Unit =
  val n = recs.size
  val tot = recs.map(_.r.total).sum
  val premAbs = recs.map(rc => math.abs(rc.r.premium)).sum
  val wins = recs.count(_.r.total > 0)
  val worst = recs.minByOption(_.r.total).map(_.r.total).getOrElse(0.0)
  val best = recs.maxByOption(_.r.total).map(_.r.total).getOrElse(0.0)
  val avg = if n > 0 then tot / n else 0.0
  // 按到期日累计, 最大回撤
  var cum = 0.0; var peak = 0.0; var maxDD = 0.0
  recs.sortBy(_.plan.end).foreach { rc => cum += rc.r.total; peak = math.max(peak, cum); maxDD = math.max(maxDD, peak - cum) }
  println("\n==================== 汇总 (卖方稳定性优先) ====================")
  println(f"tranche 数=$n  胜率=${wins}/$n (${if n > 0 then wins * 100.0 / n else 0.0}%.0f%%)")
  println(f"Σ总盈亏=$tot%+.1f  Σ权利金(收)=$premAbs%.1f  占比=${pct(tot, premAbs)}%+.2f%%  均值/笔=$avg%+.1f")
  println(f"最好 tranche=$best%+.1f   最差 tranche=$worst%+.1f   最大回撤=$maxDD%.1f")
  println("注: 收益形态应高胜率+小幅为主; 最差 tranche/回撤反映负 gamma 尾部风险 (越小越稳)。")

/** 缓存中该 symbol 有 trades 文件的日期集合 */
private def weekDates(cacheDir: String, symbol: Symbol): Set[LocalDate] =
  val dir = Path.of(cacheDir, "futures", "um", "daily", "trades", symbol)
  if !Files.isDirectory(dir) then Set.empty
  else
    import scala.jdk.CollectionConverters.*
    val stream = Files.list(dir)
    try
      stream.iterator.asScala
        .map(_.getFileName.toString)
        .flatMap(f => raw"(\d{4}-\d{2}-\d{2})".r.findFirstIn(f))
        .map(LocalDate.parse)
        .toSet
    finally stream.close()
