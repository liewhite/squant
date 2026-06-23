package strategy.utils.backtest

import hft.domain.Symbol

import java.nio.file.{Files, Path}
import java.time.LocalDate

/** 周度滚动回测的**共享报表/扫描工具** (买方 IV 网格回测与卖方 maker 回测共用, 故置于 utils.backtest)。
  * 全部基于共享记录 [[WeekRec]]，与具体策略无关。 */

// 以权利金**绝对值**为基准 (卖方 enteredPremium 为负=收到的权利金, 取绝对值得回报率)
def pct(x: Double, base: Double): Double = if base != 0.0 then x / math.abs(base) * 100.0 else 0.0

def writeWeekly(path: String, recs: Seq[WeekRec]): Unit =
  val pw = java.io.PrintWriter(path)
  try
    pw.println("idx,weekStart,weekEnd,rvThisWeek,ivUsed,ivPrev,mult,straddles,premium,optionPnl,hedgePnl,total,totalPctPrem,fills,startPx,endPx")
    recs.foreach { case WeekRec(p, r) =>
      pw.println(f"${p.idx},${p.start},${p.end},${p.rv}%.4f,${p.iv}%.4f,${p.ivPrev}%.4f,${p.mult}%.2f,${p.straddles}%.3f,${r.premium}%.2f,${r.optionPnl}%.2f,${r.hedgePnl}%.2f,${r.total}%.2f,${pct(r.total, r.premium)}%.2f,${r.fills},${r.startPx}%.2f,${r.endPx}%.2f")
    }
  finally pw.close()

/** 跨周拼接的累计净值曲线 (期权腿 / 对冲腿 / 总)，最旧->最新 */
def writeCurve(path: String, recs: Seq[WeekRec]): Unit =
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
def writeFills(path: String, recs: Seq[WeekRec]): Unit =
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
def summarize(recs: Seq[WeekRec]): Unit =
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
def completeWeeks(cacheDir: String, symbol: Symbol): Seq[(LocalDate, LocalDate)] =
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
