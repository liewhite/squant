package strategy.utils.backtest

import hft.domain.{Exchange, Side, Symbol}
import hft.event.{AnyEvent, Topics}
import strategy.utils.viz.EquityChartHtml

import scala.collection.mutable.ArrayBuffer

/** 回测**统一观察 + 出图**框架 (策略无关)。
  *
  * 把"跑回测顺带产出净值曲线 / 仓位曲线 / 买卖点"的流程沉淀为可复用组件, 任何回测启动器只需:
  *   1. `val rec = BacktestRecorder(exchange, symbol, startMs, initBalance)`
  *   2. 把 `rec.observe` 放进 `BacktestEngine(observers = Seq(rec.observe))`
  *   3. 跑完 `BacktestReport.write(outDir, label, title, symbol, rec, result.finalEquity)`
  *
  * 产出三件套 (落 `outDir`)：
  *   - `<label>_equity.csv` : ts,equity,price,pos (小时采样)
  *   - `<label>_fills.csv`   : ts,side,price,qty (逐笔成交 = 买卖点记录)
  *   - `<label>.html`        : 策略净值 vs buy&hold 曲线 + 仓位方向带 + **价格线上的买卖点标记** + 摘要
  *
  * 旁路观察, 不参与撮合、不改策略 (成交回报本就在 事件总线广播)。所有测量取**正式区间** (`startMs`
  * 之后, 预热期不计入基准/统计)。基准净值 = 正式区间首个 equity, buy&hold = 价格相对正式区间首价。
  */
object BacktestReport:

  /** 出图/落盘结果 (含统计, 供启动器打印控制台摘要)。 */
  final case class Out(
      htmlPath: String,
      equityCsv: String,
      fillsCsv: String,
      retPct: Double,
      bhRetPct: Double,
      maxDdPct: Double,
      fills: Int,
      finalEquity: Double,
      baseEquity: Double,
  ):
    /** 控制台一行摘要。 */
    def summaryLine: String =
      f"策略收益=$retPct%+.2f%%   buy&hold=$bhRetPct%+.2f%%   超额=${retPct - bhRetPct}%+.2f%%   最大回撤=$maxDdPct%.2f%%   成交=$fills"

  /** 跑完回测后产出三件套。`extraStats` 追加进网页摘要面板 (如杠杆/费率等策略特定项)。 */
  def write(
      outDir: String,
      label: String,
      title: String,
      symbol: Symbol,
      rec: BacktestRecorder,
      finalEquity: Double,
      extraStats: Seq[(String, String)] = Nil,
  ): Out =
    java.nio.file.Files.createDirectories(java.nio.file.Path.of(outDir))
    val samples = rec.equitySamples
    val fillRows = rec.fills
    val base = rec.baseEquityOr(rec.initialBalance)
    val retPct = (finalEquity - base) / base * 100.0
    val bhRetPct = rec.buyHoldRetPct
    val maxDdPct = rec.maxDrawdown * 100.0

    // ---- equity CSV ----
    val equityCsv = s"$outDir/${label}_equity.csv"
    writeLines(equityCsv, "ts,equity,price,pos", samples.map { case (ts, eq, px, pos) => s"$ts,$eq,$px,$pos" })

    // ---- fills CSV (买卖点记录) ----
    val fillsCsv = s"$outDir/${label}_fills.csv"
    writeLines(fillsCsv, "ts,side,price,qty", fillRows.map { case (ts, side, px, qty) => s"$ts,$side,$px,$qty" })

    // ---- 净值曲线网页 (策略 vs buy&hold, 归一化到起点=1) + 买卖点 ----
    val ts = samples.map(_._1).toVector
    val eqNorm = samples.map(_._2 / base).toVector
    // buy&hold 归一化基准 = 正式区间首个成交价 (与 buyHoldRetPct / 买卖点同一基准, SSOT);
    // 无成交时退回首采样价。
    val px0 = Some(rec.firstPriceInPeriod).filter(_ > 0.0)
      .orElse(samples.headOption.map(_._3).filter(_ > 0.0))
      .getOrElse(1.0)
    val bhNorm = samples.map(_._3 / px0).toVector
    val posDir = samples.map(s => math.signum(s._4).toInt).toVector
    // 买卖点标在 buy&hold 价格线上: 纵值 = 成交价/起点价 (与 bhNorm 同基准)
    val markers = fillRows.map { case (t, side, px, _) => EquityChartHtml.Marker(t, buy = side == Side.Long, value = px / px0) }.toVector
    val htmlPath = s"$outDir/$label.html"
    EquityChartHtml.write(
      htmlPath,
      title = title,
      ts = ts,
      lines = Seq(
        EquityChartHtml.Line("策略净值", "#2563eb", eqNorm),
        EquityChartHtml.Line("Buy&Hold", "#9ca3af", bhNorm),
      ),
      posDir = posDir,
      stats = Seq(
        "标的" -> symbol.toString,
        "策略收益" -> f"$retPct%+.2f%%",
        "Buy&Hold" -> f"$bhRetPct%+.2f%%",
        "超额" -> f"${retPct - bhRetPct}%+.2f%%",
        "最大回撤" -> f"$maxDdPct%.2f%%",
        "成交数" -> rec.fillCount.toString,
        "末净值" -> f"$finalEquity%.0f",
      ) ++ extraStats,
      markers = markers,
    )
    Out(htmlPath, equityCsv, fillsCsv, retPct, bhRetPct, maxDdPct, rec.fillCount, finalEquity, base)

  private def writeLines(path: String, header: String, rows: Seq[String]): Unit =
    val pw = java.io.PrintWriter(path)
    try { pw.println(header); rows.foreach(pw.println) }
    finally pw.close()

/** 回测旁路观察者：收集小时净值采样、逐笔成交、最大回撤、buy&hold 基准。仅看**正式区间** (`startMs` 之后)。
  *
  * 单线程消费 (回测引擎 observers 串行回调), 无并发设施。每个事件 O(1)。
  */
final class BacktestRecorder(
    exchange: Exchange,
    symbol: Symbol,
    /** 正式区间起点 (毫秒)；此前为预热, 不计入基准/统计/成交。 */
    startMs: Long,
    /** 账户权益兜底 (正式区间首个 equity 出现前)。 */
    val initialBalance: Double,
    /** 净值采样间隔 (默认小时)。 */
    sampleIntervalMs: Long = 3_600_000L,
):
  private val samples = ArrayBuffer.empty[(Long, Double, Double, Double)] // ts, equity, price, pos
  private val fillBuf = ArrayBuffer.empty[(Long, Side, Double, Double)]   // ts, side, price, qty
  private var lastPx = 0.0
  private var runPos = 0.0
  private var firstPxInPeriod = 0.0
  private var baseEquity = 0.0
  private var baseSet = false
  private var peakEquity = 0.0
  private var maxDd = 0.0
  private var nFills = 0
  private var lastSampleTs = 0L
  private var sampled = false // 是否已采过首样 (保证首样必采, 不依赖 ts 量级)

  /** 放进 `BacktestEngine(observers = Seq(...))` 的观察函数。 */
  val observe: AnyEvent => Unit = ev =>
    ev.as(Topics.Trade).filter(t => t.exchange == exchange && t.symbol == symbol).foreach { t =>
      lastPx = t.price.value
      if t.timestamp >= startMs && firstPxInPeriod == 0.0 then firstPxInPeriod = t.price.value
    }
    ev.as(Topics.Fill).filter(f => f.exchange == exchange && f.symbol == symbol).foreach { f =>
      runPos += (if f.side == Side.Long then f.size.value else -f.size.value)
      if f.timestamp >= startMs then
        nFills += 1
        fillBuf += ((f.timestamp, f.side, f.price.value, f.size.value))
    }
    ev.as(Topics.AccountInfo).foreach { info =>
      if ev.exchangeTs >= startMs && info.equity > 0 then
        if !baseSet then { baseEquity = info.equity; peakEquity = info.equity; baseSet = true }
        if info.equity > peakEquity then peakEquity = info.equity
        val dd = (peakEquity - info.equity) / peakEquity
        if dd > maxDd then maxDd = dd
        if !sampled || ev.exchangeTs - lastSampleTs >= sampleIntervalMs then
          sampled = true
          lastSampleTs = ev.exchangeTs
          samples += ((ev.exchangeTs, info.equity, lastPx, runPos))
    }

  def equitySamples: Vector[(Long, Double, Double, Double)] = samples.toVector
  def fills: Vector[(Long, Side, Double, Double)] = fillBuf.toVector
  def fillCount: Int = nFills
  def maxDrawdown: Double = maxDd
  def baseEquityOr(default: Double): Double = if baseSet then baseEquity else default

  /** 正式区间首个成交价 (>0)；作 buy&hold 归一化与买卖点纵值的**单一基准**。0 = 区间内无成交。 */
  def firstPriceInPeriod: Double = firstPxInPeriod

  /** buy&hold 收益率 (%) = 末价相对正式区间首价。 */
  def buyHoldRetPct: Double =
    if firstPxInPeriod > 0.0 then (lastPx - firstPxInPeriod) / firstPxInPeriod * 100.0 else 0.0
