package hft.perf

import hft.domain.*
import hft.event.Topic

/** 一个 (账户, 标的) 的表现快照。
  *
  * 实盘与影子盘用**同一份**算法从各自的成交流重建 —— 这是两边数字可比的前提，
  * 也是"按影子盘表现决定实盘去留"能成立的前提。不用交易所推送的账户净值，
  * 因为那是整个账户的 (含其他策略与手工仓位)，不是本策略的战绩。
  *
  * @param realizedPnl 已实现盈亏，**已扣按名义费率估算的手续费** (见 [[PerformanceTracker]])
  * @param roundTrips  完成的往返次数 (仓位从非零回到零算一次)：判据的样本量看它，
  *                    而不是成交笔数 —— 一次往返才是一个完整的下注结果
  */
final case class Performance(
    account: AccountId,
    instrument: Instrument,
    realizedPnl: Double,
    fees: Double,
    fills: Int,
    roundTrips: Int,
    position: Quantity,
    since: Timestamp,
    updatedAt: Timestamp,
)

/** 表现快照的事件族。按 (账户, 标的) 路由 —— 监督者据此比较同一标的上实盘与影子的战绩 */
object Performances extends Topic[AccountInstrument, Performance]("performance"):
  def keyOf(p: Performance): AccountInstrument = AccountInstrument(p.account, p.instrument)
