package hft.demo.edge

import java.time.LocalDate

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

  /** 计算第 idx 周 (0 基) 的计划。**只读取 `rv(j)` 的 j ≤ idx-1** (严格过去周, 无前视)。
    *
    * iv(idx)   = if idx==0 seed else rv(idx-1)
    * ivPrev    = iv(idx-1) = if idx<=1 seed else rv(idx-2)
    * mult      = if idx==0 1.0 elif iv<ivPrev gridUp elif iv>ivPrev gridDown else 1.0
    *
    * @param rv 周已实现 RV 查询 (传入实现应保证只被以过去索引调用)
    */
  def planWeek(idx: Int, seedIv: Double, rv: Int => Double, gridUp: Double, gridDown: Double): Plan =
    val iv = if idx == 0 then seedIv else rv(idx - 1)
    val ivPrev = if idx <= 1 then seedIv else rv(idx - 2)
    val mult =
      if idx == 0 then 1.0
      else if iv < ivPrev then gridUp      // 波动下降 -> 多买
      else if iv > ivPrev then gridDown    // 波动上升 -> 少买
      else 1.0
    Plan(iv, ivPrev, mult)

  /** 从已有日期集合切出连续、不重叠、每天都有数据的 7 天窗口 (升序, 含起止日)。
    * 从最早日期起以 7 天步长推进; 含缺天的窗口被整窗跳过 (不回退对齐)。 */
  def weekWindows(dates: Set[LocalDate]): Seq[(LocalDate, LocalDate)] =
    if dates.isEmpty then Seq.empty
    else
      val first = dates.min
      val last = dates.max
      Iterator
        .iterate(first)(_.plusDays(7))
        .takeWhile(d => !d.plusDays(6).isAfter(last))
        .filter(d => (0 to 6).forall(k => dates.contains(d.plusDays(k))))
        .map(d => (d, d.plusDays(6)))
        .toSeq
