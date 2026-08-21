package strategy.utils.backtest

import hft.domain.Side

import java.time.LocalDate

/** 周度滚动回测的共享记录类型 (买方/卖方周度回测共用, 故置于 utils.backtest)。 */
final case class WeekPlan(idx: Int, start: LocalDate, end: LocalDate, rv: Double, iv: Double, ivPrev: Double, mult: Double, straddles: Double)
final case class WeekResult(
    optionPnl: Double, hedgePnl: Double, total: Double, premium: Double, fills: Int, startPx: Double, endPx: Double,
    curve: Vector[(Long, Double, Double)], fillRecs: Vector[(Long, Side, Double, Double)],
)
final case class WeekRec(plan: WeekPlan, r: WeekResult)

/** 周度 IV 网格的**纯逻辑**——独立成包级纯函数, 锁定两条关键口径不被后续改动破坏:
  *   ① IV/仓位倍数的因果 (只用过去周的已实现 RV, 绝不前视未来);
  *   ② 连续不重叠、每天有数据的 7 天窗口切分。
  * 编排层 [[WeeklyIvGridBacktest]] 调用这些函数, 便于直接单测。 */
object WeeklyIvGrid:

  /** 一周的计划: IV 与上周 IV 及仓位倍数。
    * @param iv     本周用的隐含波动 = 上周已实现 RV (首周=种子)
    * @param ivPrev 上周用的 IV (= 上上周 RV, 或种子)
    * @param mult   仓位倍数 (波动较上周下降→gridUp 多买, 上升→gridDown 少买, 持平/首周→1.0)
    */
  final case class Plan(iv: Double, ivPrev: Double, mult: Double)

  /** 仓位倍数策略 (纯函数 ctx -> 倍数, 0=不建仓)。新规则=新实现, 开放封闭。 */
  trait SizePolicy:
    /** @param iv 本周 IV(=上周RV) @param ivPrev 上周 IV @param idx 周序(0基) */
    def mult(iv: Double, ivPrev: Double, idx: Int): Double

  /** 两档网格 (现状)：波动较上周下降→up、上升→down、持平/首周→1.0。 */
  final class StepGrid(up: Double, down: Double) extends SizePolicy:
    def mult(iv: Double, ivPrev: Double, idx: Int): Double =
      if idx == 0 then 1.0
      else if iv < ivPrev then up
      else if iv > ivPrev then down
      else 1.0

  /** 只在波动下降时建仓、**越跌越买**：倍数 = clamp(scale × 相对跌幅, 0, cap)；波动持平/上升/首周 → 0 (不建仓)。
    * 相对跌幅 = (ivPrev − iv)/ivPrev。 */
  final class DropOnly(scale: Double, cap: Double) extends SizePolicy:
    def mult(iv: Double, ivPrev: Double, idx: Int): Double =
      if idx == 0 || ivPrev <= 0.0 then 0.0
      else
        val drop = (ivPrev - iv) / ivPrev // >0 表示波动下降
        if drop <= 0.0 then 0.0 else math.min(cap, scale * drop)

  /** 计算第 idx 周 (0 基) 的计划。**只读取 `rv(j)` 的 j ≤ idx-1** (严格过去周, 无前视)。
    *
    * iv(idx) = if idx==0 seed else rv(idx-1); ivPrev = iv(idx-1) = if idx<=1 seed else rv(idx-2);
    * 倍数由 [[SizePolicy]] 决定。
    *
    * @param rv 周已实现 RV 查询 (传入实现应保证只被以过去索引调用)
    */
  def planWeek(idx: Int, seedIv: Double, rv: Int => Double, policy: SizePolicy): Plan =
    val iv = if idx == 0 then seedIv else rv(idx - 1)
    val ivPrev = if idx <= 1 then seedIv else rv(idx - 2)
    Plan(iv, ivPrev, policy.mult(iv, ivPrev, idx))

  /** 计算第 idx 周 (0 基) 的计划，IV 为**进场时可观测的市场 IV** (如 Bybit IV 指数, 非滞后 RV)。
    *
    * 与 [[planWeek]] 的区别: 市场 IV 在开仓时即可见, 无需滞后一周 -> iv(idx)=ivAt(idx)、ivPrev=ivAt(idx-1)
    * (首周 ivPrev=iv 使 [[SizePolicy]] 退化为 1×)。`ivAt` 应在连续可用周上为全函数。 */
  def planObservedIv(idx: Int, ivAt: Int => Double, policy: SizePolicy): Plan =
    val iv = ivAt(idx)
    val ivPrev = if idx == 0 then iv else ivAt(idx - 1)
    Plan(iv, ivPrev, policy.mult(iv, ivPrev, idx))

  /** tranche 持有期 (第 idx 周起、跨 tenorWeeks 周) 的**已实现 RV** = 各周 RV 的均方根 (RMS)。
    *
    * 同时长不相交子区间的方差可加, 故区间年化波动 = sqrt(各周年化方差均值) = sqrt(mean(rv²))；tenorWeeks=1
    * 时退化为 `rv(idx)`。供"卖出 IV = 期权存续期实际 RV (iv=rv, 完美预知, **不可交易**)"的 edge 检验:
    * 以此作 `ivAt` 喂 [[planObservedIv]] 即得 iv=本期实现 RV、ivPrev=上期实现 RV。 */
  def windowRvRms(idx: Int, tenorWeeks: Int, rv: Int => Double): Double =
    val n = math.max(1, tenorWeeks)
    val sumSq = (0 until n).map(k => { val v = rv(idx + k); v * v }).sum
    math.sqrt(sumSq / n)

  /** 从已有日期集合切出**长 lenDays、步长 stepDays** 的窗口 (升序, 含起止日)：
    * 从最早日期起按 stepDays 推进; 含缺天的窗口整窗跳过 (不回退对齐)。stepDays<lenDays 时窗口重叠。 */
  def windows(dates: Set[LocalDate], lenDays: Int, stepDays: Int): Seq[(LocalDate, LocalDate)] =
    if dates.isEmpty || lenDays < 1 || stepDays < 1 then Seq.empty
    else
      val first = dates.min
      val last = dates.max
      Iterator
        .iterate(first)(_.plusDays(stepDays))
        .takeWhile(d => !d.plusDays(lenDays - 1).isAfter(last))
        .filter(d => (0 until lenDays).forall(k => dates.contains(d.plusDays(k))))
        .map(d => (d, d.plusDays(lenDays - 1)))
        .toSeq

  /** 连续不重叠的 7 天窗口 (= windows(dates, 7, 7)) */
  def weekWindows(dates: Set[LocalDate]): Seq[(LocalDate, LocalDate)] = windows(dates, 7, 7)
