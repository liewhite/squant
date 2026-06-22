package app.backtest

import hft.backtest.{BacktestEngine, BinanceDataKind, BinanceHistory, MinuteBars, MinuteBarReplaySource, TradeBboAugmentSource}
import hft.domain.{Exchange, Side}
import hft.engine.StrategyRunner
import hft.messaging.{EventData, IncomeEvent}
import hft.sim.SimConfig
import strategy.research.TrendRideStrategy
import sttp.client4.DefaultSyncBackend

import java.time.{LocalDate, ZoneOffset}
import java.util.concurrent.Executors
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

/** 两周期定义 (SSOT)：聚合与回放共用同一区间与预热, 避免 bar 数据与 baseEq 起点错配。 */
object TrendRidePeriods:
  val warmupDays = 8L
  /** (标签, 正式区间起, 正式区间止) */
  val ranges: Seq[(String, LocalDate, LocalDate)] = Seq(
    ("trend", LocalDate.parse("2025-10-06"), LocalDate.parse("2025-11-17")),
    ("chop", LocalDate.parse("2026-02-16"), LocalDate.parse("2026-05-25")),
  )
  def barsPath(label: String): String = s"/tmp/trendride/bars_${label}.csv"

/** 一次性把两周期的缓存 trades 聚合成 1 分钟 bar 落盘 (供 [[TrendRideSweep]] 快速回放)。
  * 运行: sbt "runMain app.backtest.TrendRideAggregate" (约 10+ 分钟, 只需跑一次)。 */
@main def TrendRideAggregate(args: String*): Unit =
  System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn")
  val symbol = args.lift(0).getOrElse("ETHUSDT")
  val backend = DefaultSyncBackend()
  java.nio.file.Files.createDirectories(java.nio.file.Path.of("/tmp/trendride"))
  TrendRidePeriods.ranges.foreach { (label, start, end) =>
    val src = BinanceHistory.source(backend, Seq(symbol), start.minusDays(TrendRidePeriods.warmupDays), end, kinds = Seq(BinanceDataKind.Trades))
    val n = MinuteBars.aggregate(src, TrendRidePeriods.barsPath(label))
    println(s"aggregated $label: $n minute bars -> ${TrendRidePeriods.barsPath(label)}")
  }

/** TrendRide 参数/框架扫描 —— 在内存分钟 bar 上快速回放多套配置，按两周期表现排序。
  *
  * 运行: sbt "runMain app.backtest.TrendRideSweep" (依赖 TrendRideAggregate 已生成 bar CSV)。
  */
@main def TrendRideSweep(args: String*): Unit =
  System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn")
  val symbol = args.lift(0).getOrElse("ETHUSDT")
  val initBalance = 100_000.0
  val makerFee = 0.0002
  val takerFee = 0.0005

  final case class Period(label: String, startMs: Long, bars: Vector[hft.backtest.MinuteBar])
  def epochMs(d: LocalDate) = d.atStartOfDay(ZoneOffset.UTC).toInstant.toEpochMilli
  val periods = TrendRidePeriods.ranges.map { (label, start, _) =>
    Period(label, epochMs(start), MinuteBars.load(TrendRidePeriods.barsPath(label)))
  }

  /** 一套待测配置。仅列出要扫的维度, 其余用 TrendRideStrategy 默认。 */
  final case class Cfg(
      name: String,
      mMax: Double = 12.0,
      rMax: Double = 3.0,
      band: Double = 0.5,
      convDead: Double = 0.05,
      mrTrendDecay: Double = 0.0,
      kSnr: Double = 1.5,
      stepQty: Double = 1.5,
      trendEntryTaker: Boolean = false,
  )

  // ====== 能否让 MR 账在震荡里盈利? 宽死区(多留在高抛低吸模式) × 放大 MR 账(rMax)。taker on, k2.5 m12 ======
  val configs: Seq[Cfg] =
    Cfg("TK k2.5 m12 base", mMax = 12, band = 2.0, convDead = 0.1, mrTrendDecay = 1.0, kSnr = 2.5, stepQty = 2.0, trendEntryTaker = true) +: (
      for
        convDead <- Seq(0.1, 0.2, 0.35)
        rMax <- Seq(3.0, 6.0, 9.0)
      yield Cfg(
        name = f"d$convDead%.2f r${rMax.toInt}",
        mMax = 12, rMax = rMax, band = 2.0, convDead = convDead, mrTrendDecay = 1.0, kSnr = 2.5,
        stepQty = 2.0, trendEntryTaker = true,
      )
    )

  final case class Res(name: String, period: String, retPct: Double, bhPct: Double, maxDdPct: Double, fills: Int, avgPos: Double)

  // symbolMetas 在并行前 eager 拉取一次 (不把网络 IO 藏在 lazy + 并发首访路径), 各回放线程只读共享
  val symbolMetasCache =
    val backend = DefaultSyncBackend()
    hft.exchange.binance.BinanceClient(backend, credentials = None)
      .fetchAllSymbolMetas()
      .fold(e => sys.error(s"fetch metas failed: ${e.message}"), _.map(m => (m.exchange, m.symbol) -> m).toMap)

  def runOne(p: Period, cfg: Cfg): Res =
    val strat = TrendRideStrategy(
      exchange = Exchange.Binance, symbol = symbol,
      mMax = cfg.mMax, rMax = cfg.rMax, band = cfg.band, convDead = cfg.convDead,
      mrTrendDecay = cfg.mrTrendDecay, kSnrP = cfg.kSnr, stepQty = cfg.stepQty,
      trendEntryTaker = cfg.trendEntryTaker,
    )
    val runner = StrategyRunner.backtest(strat, symbolMetasCache)
    var lastPx, runPos, baseEq, peakEq, maxDd, firstPx, posSum = 0.0
    var baseSet = false; var posN = 0L
    val obs: IncomeEvent => Unit = ev =>
      ev.data match
        case EventData.MarketTradeUpdate(t) =>
          lastPx = t.price
          if t.timestamp >= p.startMs && firstPx == 0.0 then firstPx = t.price
        case EventData.FillUpdate(f) => runPos += (if f.side == Side.Long then f.size else -f.size)
        case EventData.AccountInfoUpdate(_, info) =>
          if ev.exchangeTs >= p.startMs && info.equity > 0 then
            if !baseSet then { baseEq = info.equity; peakEq = info.equity; baseSet = true }
            if info.equity > peakEq then peakEq = info.equity
            val dd = (peakEq - info.equity) / peakEq
            if dd > maxDd then maxDd = dd
            posSum += math.abs(runPos); posN += 1
        case _ => ()
    val engine = BacktestEngine(
      exchange = Exchange.Binance,
      source = TradeBboAugmentSource(MinuteBarReplaySource(p.bars, Exchange.Binance, symbol)),
      runners = Seq(runner),
      config = SimConfig(100, 50, initBalance, makerFeeRate = makerFee, takerFeeRate = takerFee),
      observers = Seq(obs),
    )
    val r = engine.run()
    val base = if baseSet then baseEq else initBalance
    Res(cfg.name, p.label, (r.finalEquity - base) / base * 100,
      if firstPx > 0 then (lastPx - firstPx) / firstPx * 100 else 0.0, maxDd * 100, r.fills,
      if posN > 0 then posSum / posN else 0.0)

  println(s"==================== TrendRide Sweep (minute-bar replay) ====================")
  println(s"symbol=$symbol  configs=${configs.size}  periods=${periods.map(_.label).mkString(",")}")
  val pool = Executors.newFixedThreadPool(math.min(8, Runtime.getRuntime.availableProcessors))
  given ec: ExecutionContext = ExecutionContext.fromExecutor(pool)
  val jobs = for c <- configs; p <- periods yield Future((c, p, runOne(p, c)))
  val results = try Await.result(Future.sequence(jobs), Duration.Inf).map(_._3) finally pool.shutdown()

  val byCfg = results.groupBy(_.name)
  println("-" * 96)
  println(f"${"config"}%22s | ${"trend ret%"}%10s ${"trend DD%"}%9s | ${"chop ret%"}%10s ${"chop DD%"}%9s | ${"sum ret%"}%9s ${"fills"}%8s")
  configs
    .map { c =>
      val rs = byCfg(c.name)
      val tr = rs.find(_.period == "trend").get
      val ch = rs.find(_.period == "chop").get
      (c.name, tr, ch, tr.retPct + ch.retPct, tr.fills + ch.fills)
    }
    .sortBy(-_._4)
    .foreach { (name, tr, ch, sum, fills) =>
      println(f"$name%22s | ${tr.retPct}%+9.2f%% ${tr.maxDdPct}%8.2f%% | ${ch.retPct}%+9.2f%% ${ch.maxDdPct}%8.2f%% | ${sum}%+8.2f%% ${fills}%8d")
    }
  println("-" * 96)
  val bh = periods.map(p => f"${p.label} b&h=${results.find(r => r.period==p.label).get.bhPct}%+.1f%%").mkString("  ")
  println(s"参考 buy&hold: $bh")
  println("============================================================================")
