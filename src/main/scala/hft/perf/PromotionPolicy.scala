package hft.perf

import hft.domain.{Instrument, Timestamp}

/** 监督者对某个标的能看到的全部事实 */
final case class SymbolPerformance(
    instrument: Instrument,
    /** 影子账户的战绩 (常驻运行) */
    paper: Performance,
    /** 实盘战绩；None = 该标的当前没开实盘 */
    live: Option[Performance],
    /** 实盘开启的时刻；None = 未开 */
    liveSince: Option[Timestamp],
    now: Timestamp,
):
  def isLive: Boolean = live.isDefined
  def liveElapsedMs: Long = liveSince.map(now - _).getOrElse(0L)

/** 监督者对一个标的的处置 */
enum Decision:
  /** 拉起实盘实例 */
  case Promote
  /** 撤下实盘实例并平掉它的仓位 */
  case Demote
  /** 什么都不做 */
  case Hold

/** 晋升 / 降级的判据 —— **扩展点，框架不预设任何阈值**。
  *
  * 框架负责：维护每个 (账户, 标的) 的战绩、按节拍询问判据、执行决定 (拉起/撤下实盘实例、
  * 降级时平掉敞口)。什么时候值得上实盘，是使用者的业务判断。
  *
  * ## 写判据前务必知道的两件事
  *
  * **1. 多重比较。** 同时有 N 个标的在跑影子盘，即使策略毫无 edge，任意时刻约一半是盈利的，
  * 而**最好的那几个几乎必然看起来很赚**（N 个样本的最大值大约落在数个 σ 上）。以"盈利 > 0"
  * 晋升，等于系统性地在幸运跑完之后进场，紧接着均值回复；再叠加"实盘亏损就降级"，
  * 就构成了高位晋升、低位降级的负 alpha 循环。判据需要统计功效：最少往返笔数
  * （[[Performance.roundTrips]] 而非成交笔数）、显著性，必要时加样本外确认。
  *
  * **2. 影子盘的成交偏乐观。** 虚拟柜台不建模队列位置，只要行情越过挂单价就算成交 ——
  * 成交**价格**是对的，成交**机会**偏多，真实盘口里排在后面的单可能根本轮不到。因此影子盘
  * 的盈亏系统性偏高，门槛要留余量。晋升之后同一标的上实盘与影子并行，**两边成交率之差
  * 正是校准这个偏差的数据**，可以拿它反过来修正门槛。
  *
  * 还有一条：[[Performance.realizedPnl]] 里的手续费是按名义费率估算的，不是账单实数
  * （见 [[PerformanceTracker]]）。比较两个账户的相对表现时这个偏差抵消，但别拿它当对账依据。
  */
trait PromotionPolicy:
  def decide(view: SymbolPerformance): Decision

/** 默认判据：永不晋升。
  *
  * 框架不替使用者决定什么时候上真钱 —— 一个"看起来合理"的内置阈值比没有阈值更危险，
  * 因为它会被当成经过验证的默认值直接用上实盘。
  */
object NeverPromote extends PromotionPolicy:
  def decide(view: SymbolPerformance): Decision = Decision.Hold
