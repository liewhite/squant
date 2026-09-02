package strategy.compare
import strategy.utils.backtest.{WeeklyIvGrid, WeekPlan, WeekResult, WeekRec, pct, writeWeekly, writeFills, summarize}

import hft.backtest.binance.BinanceMarketDataProvider
import hft.backtest.{BacktestEngine, BsGreeksConfig, BsGreeksSource, MarketDataKind, SyntheticBboSource}
import hft.domain.*
import hft.engine.StrategyRunner
import hft.indicator.RealizedVol
import hft.option.BlackScholes
import hft.event.{AnyEvent, Topics}
import hft.sim.SimConfig
import strategy.strategies.makerhedge.logic.{AsymHedgeBand, MakerHedgeStrategy}
import strategy.strategies.bandhedge.logic.BandHedgeStrategy
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
  *   - **每周开始卖出一份下周到期 (7 天) ATM 空头跨式**, 持有到该 tranche 到期 (tenor 默认 1 周, 可 EDGE_TENOR_DAYS 调)。
  *   - **仓位**: 本周 RV 较上周**上升**→卖 [[gridHigh]]×(默认 2), **下降**→卖 [[gridLow]]×(默认 1)。
  *     (波动刚涨→IV 定得高→卖更多, 赚回落) 用 [[WeeklyIvGrid.StepGrid]](up=低倍, down=高倍) 作用于**负** straddles。
  *   - **对冲**: [[AsymHedgeBand]].byMa —— 价在 **MA20 上方** (看多)→上带紧 1ATR(负 delta 1ATR 对冲到 0)、
  *     下带松 2ATR(正 delta 2ATR 对冲到 0); 价在 **MA20 下方** (看空) 则镜像。执行: maker 在 BBO 外 0.01% 挂被动单, 5s 未成交撤单重挂。
  *   - IV = 上一周已实现 RV (滞后, 不预测未来)。
  *
  * 卖方追求**稳定为正 (高胜率)**, 故重点报: 胜率 / 最差 tranche / 最大回撤 / 均值, 而非仅总收益。
  * 详细数据: tranche 汇总 / 逐笔对冲成交 / 按到期日的累计净值曲线。
  *
  * 运行: sbt "runMain strategy.compare.WeeklySellVolBacktest"
  * env: EDGE_STRADDLES(基准份数,取绝对值做空) / EDGE_GRID_HIGH / EDGE_GRID_LOW / EDGE_TIGHT_ATR / EDGE_LOOSE_ATR /
  *      EDGE_TENOR_DAYS(默认7) / EDGE_MAKER_OFFSET(默认0.0001) / EDGE_REQUOTE_MS(默认5000) /
  *      EDGE_MAKER_FEE / EDGE_TAKER_FEE / EDGE_DELAY_MS / EDGE_MAX_TRANCHES / EDGE_*_CSV / EDGE_PAR / DATA_CACHE
  */
/** 对冲执行方式。 */
enum HedgeExec(val envValue: String):
  case Maker extends HedgeExec("maker")
  case Take extends HedgeExec("take")

/** 对冲带形态。 */
enum BandMode(val envValue: String):
  case Ma extends BandMode("ma")
  case Symmetric extends BandMode("sym")

/** 卖出 IV 的取数口径。 */
enum IvMode(val envValue: String):
  case LaggedRv extends IvMode("lag")
  case WindowRv extends IvMode("rv")

/** 读一个**枚举型**环境变量。
  *
  * 不认识的取值一律报错并列出合法选项：`EDGE_HEDGE_EXEC=taker` (少了个 r) 从前会静默落进
  * else 分支跑成 maker，于是一份实验数据被归到了另一个配置名下 —— 这种错在结果里看不出来。 */
private def envChoice[A](name: String, default: String, choices: Map[String, A]): A =
  val raw = sys.env.getOrElse(name, default)
  choices.getOrElse(raw, sys.error(s"$name=$raw 不是合法取值, 可选: ${choices.keys.toVector.sorted.mkString("|")}"))

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
  val tenorDays = sys.env.get("EDGE_TENOR_DAYS").map(_.toInt).getOrElse(7)  // 卖下周期权 = 1 周 tenor
  val takerFee = sys.env.get("EDGE_TAKER_FEE").map(_.toDouble).getOrElse(0.0)
  val makerFee = sys.env.get("EDGE_MAKER_FEE").map(_.toDouble).getOrElse(0.0)
  val delayMs = sys.env.get("EDGE_DELAY_MS").map(_.toLong).getOrElse(0L)
  // 对冲执行: maker(BBO 外被动 PostOnly, 5s 重挂) | take(市价 at-touch 立即成交, 理想离散对冲基准)
  val hedgeExec = envChoice("EDGE_HEDGE_EXEC", "maker", HedgeExec.values.map(v => v.envValue -> v).toMap)
  val makerOffset = sys.env.get("EDGE_MAKER_OFFSET").map(_.toDouble).getOrElse(0.0001) // BBO 外 0.01%
  val requoteMs = sys.env.get("EDGE_REQUOTE_MS").map(_.toLong).getOrElse(5000L)
  val maxTranches = sys.env.get("EDGE_MAX_TRANCHES").map(_.toInt).getOrElse(Int.MaxValue)
  // IV 口径: lag=上周已实现 RV (滞后, 可交易) | rv=期权存续期实际 RV (iv=rv, 完美预知, **不可交易**, 仅检验对冲腿 edge)
  val ivMode = envChoice("EDGE_IV_MODE", "lag", IvMode.values.map(v => v.envValue -> v).toMap)
  val initialBalance = 1_000_000.0
  val weeklyCsv = sys.env.getOrElse("EDGE_WEEKLY_CSV", "/tmp/sellvol_tranches.csv")
  val curveCsv = sys.env.getOrElse("EDGE_CURVE_CSV", "/tmp/sellvol_curve.csv")
  val fillsCsv = sys.env.getOrElse("EDGE_FILLS_CSV", "/tmp/sellvol_fills.csv")
  val tenorWeeks = math.max(1, math.round(tenorDays / 7.0).toInt) // 7d -> 1 周

  val meta = SymbolMeta(Exchange.Binance, symbol, tickSize = 0.01, sizeStep = 0.001, minOrderSize = 0.001, contractSize = 1.0)
  val symbolMetas = Map((Exchange.Binance, symbol) -> meta)
  // 仓位: 波动下降(iv<ivPrev)→gridLow, 上升→gridHigh; StepGrid(up=低,down=高)
  val sizePolicy = WeeklyIvGrid.StepGrid(up = gridLow, down = gridHigh)
  // 对冲带: ma=MA20 不对称(顺势紧逆势松) | sym=对称(上下均 tightAtr×ATR, 无方向, iv=rv 应盈亏平衡的基准)
  val bandMode = envChoice("EDGE_BAND_MODE", "ma", BandMode.values.map(v => v.envValue -> v).toMap)
  val band = bandMode match
    case BandMode.Symmetric => AsymHedgeBand.symmetric(tightAtr)                             // 纯 1ATR delta 对冲基准
    case BandMode.Ma        => AsymHedgeBand.byMa(trendSideMult = tightAtr, counterTrendMult = looseAtr) // MA20: 价在上→上带紧

  val weeks = WeeklyIvGrid.weekWindows(weekDates(cacheDir, symbol))
  if weeks.sizeIs < tenorWeeks + 2 then sys.error(s"缓存周数不足 (${weeks.size}, 需 >= ${tenorWeeks + 2})")

  val ivDesc = ivMode match
    case IvMode.WindowRv => "iv=rv(存续期实际RV, 完美预知/不可交易)"
    case IvMode.LaggedRv => "iv=上周RV(滞后/可交易)"
  println("==================== 卖方 short-vol 周度滚动回测 ====================")
  println(f"symbol=$symbol  周数=${weeks.size}  tenor=${tenorDays}d(${tenorWeeks}周)  基准卖=${baseStraddles}份  IV口径: $ivDesc")
  val execDesc = hedgeExec match
    case HedgeExec.Take  => f"take(市价at-touch, takerFee=${takerFee * 100}%.3f%%)"
    case HedgeExec.Maker => f"maker(BBO外${makerOffset * 100}%.3f%%, ${requoteMs}ms重挂, makerFee=${makerFee * 100}%.3f%%)"
  val bandDesc = bandMode match
    case BandMode.Symmetric => f"对称${tightAtr}%.1fATR(无方向, 基准)"
    case BandMode.Ma        => f"MA20上 上${tightAtr}%.1fATR/下${looseAtr}%.1fATR, MA20下反之"
  println(f"仓位: 波动↑卖${gridHigh}×/↓卖${gridLow}×   对冲带: $bandDesc   执行=$execDesc  delay=${delayMs}ms")

  def tradeSource(backend: sttp.client4.SyncBackend, s: LocalDate, e: LocalDate) =
    BinanceMarketDataProvider(backend, cacheDir).source(Seq(symbol), s, e, Set(MarketDataKind.Trades))

  def prepass(s: LocalDate, e: LocalDate): Double =
    val backend = DefaultSyncBackend()
    try
      val samples = ArrayBuffer.empty[Double]; var lastTs = 0L
      val it = tradeSource(backend, s, e).events()
      while it.hasNext do
        it.next().as(Topics.Trade).foreach { t =>
          if t.timestamp - lastTs >= 3_600_000L then { lastTs = t.timestamp; samples += t.price.value }
        }
      // 估不出即抛: 这一周的 RV 是后面所有 tranche 的 IV 来源, 以 0 顶替会让 BS 把期权定价成内在价值
      RealizedVol
        .annualizedFromPrices(samples.toVector, BlackScholes.HoursPerYear)
        .getOrElse(sys.error(s"$s..$e 只采到 ${samples.size} 个小时采样, 估不出周 RV (数据缺口?)"))
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
      val withGreeks = BsGreeksSource(tradeSource(backend, start, end), AccountId.Live, cfg)
      // 对冲策略订阅盘口, 而该区间币安无 bookTicker 历史 -> 显式合成零价差盘口 (低估点差成本)
      val source = SyntheticBboSource(withGreeks)
      val hedgeStrat = hedgeExec match
        case HedgeExec.Take  => BandHedgeStrategy(Exchange.Binance, symbol, ccy, band) // 市价 at-touch
        case HedgeExec.Maker => MakerHedgeStrategy(Exchange.Binance, symbol, ccy, band, offsetPct = makerOffset, requoteMs = requoteMs)
      val runner = StrategyRunner.backtest(hedgeStrat)
      var lastMid = 0.0; var lastTs = 0L; var curveLastTs = 0L
      val curve = ArrayBuffer.empty[(Long, Double, Double)]
      val fillRecs = ArrayBuffer.empty[(Long, Side, Double, Double)]
      val obs: AnyEvent => Unit = ev =>
        ev.as(Topics.Bbo).foreach { b => lastMid = b.midPrice.value; lastTs = b.timestamp }
        ev.as(Topics.Fill).foreach(f => fillRecs += ((f.timestamp, f.side, f.price.value, f.size.value)))
        ev.as(Topics.AccountInfo).foreach { info =>
          if lastTs > 0 && ev.exchangeTs - curveLastTs >= 3_600_000L then
            curveLastTs = ev.exchangeTs
            curve += ((ev.exchangeTs, withGreeks.optionPnl(lastMid, lastTs), info.equity - initialBalance))
        }
      val engine = BacktestEngine(
        exchange = Exchange.Binance, source = source, runners = Seq(runner),
        config = SimConfig(exchangeToStrategyDelayMs = 0, orderToExchangeDelayMs = delayMs,
          initialBalanceUsdt = initialBalance, makerFeeRate = makerFee, takerFeeRate = takerFee),
        symbolMetas = symbolMetas,
    observers = Seq(obs),
      )
      val result = engine.run()
      // 期权腿要按"最后见到的中间价"结算; 一条盘口都没见过就没有结算价, 以 0 结算会凭空造出一份损益
      if lastTs == 0L then sys.error(s"$start..$end 全程没有盘口事件, 无法给期权腿定收盘价")
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
    // iv=rv: 卖出 IV = 期权存续期实际 RV (完美预知, 不可交易) -> 用 windowRvRms 作可观测 IV 喂 planObservedIv
    def windowRv(j: Int): Double = WeeklyIvGrid.windowRvRms(j, tenorWeeks, rvByWeek)
    val plans = trancheIdx.map { i =>
      val p = ivMode match
        case IvMode.WindowRv => WeeklyIvGrid.planObservedIv(i, windowRv, sizePolicy)
        case IvMode.LaggedRv => WeeklyIvGrid.planWeek(i, 0.55, j => rvByWeek(j), sizePolicy)
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
  // 空集没有胜率、没有最差 tranche, 也没有均值。打一行 0 出来等于报告了一个没做过的测量。
  require(recs.nonEmpty, "没有任何 tranche 结果可汇总 (区间/EDGE_MAX_TRANCHES 是否为空?)")
  val n = recs.size
  val tot = recs.map(_.r.total).sum
  val premAbs = recs.map(rc => math.abs(rc.r.premium)).sum
  val wins = recs.count(_.r.total > 0)
  val worst = recs.map(_.r.total).min
  val best = recs.map(_.r.total).max
  val avg = tot / n
  // 按到期日累计, 最大回撤
  var cum = 0.0; var peak = 0.0; var maxDD = 0.0
  recs.sortBy(_.plan.end).foreach { rc => cum += rc.r.total; peak = math.max(peak, cum); maxDD = math.max(maxDD, peak - cum) }
  println("\n==================== 汇总 (卖方稳定性优先) ====================")
  println(f"tranche 数=$n  胜率=${wins}/$n (${wins * 100.0 / n}%.0f%%)")
  println(f"Σ总盈亏=$tot%+.1f  Σ权利金(收)=$premAbs%.1f  占比=${pct(tot, premAbs)}%+.2f%%  均值/笔=$avg%+.1f")
  println(f"最好 tranche=$best%+.1f   最差 tranche=$worst%+.1f   最大回撤=$maxDD%.1f")
  println("注: 收益形态应高胜率+小幅为主; 最差 tranche/回撤反映负 gamma 尾部风险 (越小越稳)。")

/** 缓存中该 symbol 有 trades 文件的日期集合 */
private def weekDates(cacheDir: String, symbol: Symbol): Set[LocalDate] =
  val dir = Path.of(cacheDir, "futures", "um", "daily", "trades", symbol)
  // 目录不存在是配置错误 (DATA_CACHE 指错 / 还没下载), 报出路径; 返回空集会让它伪装成"缓存周数不足"
  if !Files.isDirectory(dir) then sys.error(s"逐笔缓存目录不存在: ${dir.toAbsolutePath} (DATA_CACHE 是否指对?)")
  import scala.jdk.CollectionConverters.*
  val stream = Files.list(dir)
  try
    stream.iterator.asScala
      .map(_.getFileName.toString)
      .flatMap(f => raw"(\d{4}-\d{2}-\d{2})".r.findFirstIn(f))
      .map(LocalDate.parse)
      .toSet
  finally stream.close()
