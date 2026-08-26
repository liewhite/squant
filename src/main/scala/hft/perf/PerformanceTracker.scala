package hft.perf

import hft.actor.{Actor, ActorContext}
import hft.domain.*
import hft.event.{AnyEvent, Event, Interest, Topics}


import scala.collection.mutable

/** 从成交流重建每个 (账户, 标的) 的战绩，周期发布 [[Performance]]。
  *
  * ## 为什么不用交易所的账户净值
  *
  * 那是整个账户的数字，含其他策略与手工仓位；而判据要比较的是**这一个策略实例**的战绩。
  * 更接近它的事实是该 (账户, 标的) 上的成交流，所以从 Fill 重建。实盘与影子走同一段代码
  * ([[Ledger]]，也是回测用的那个)，两边数字因此可比。
  *
  * 一个如实的前提：这里统计的是**该 (账户, 标的) 的全部成交**，"等于某个策略实例的战绩"
  * 靠的是 [[hft.engine.InstrumentClaims]] 的独占登记 (一个 (账户, 标的) 最多一个实例)。
  * 同一标的上的手工交易仍会被算进这份战绩 —— 而拒绝账户净值的理由恰恰就有手工仓位。
  * 影子账户没有这个问题 (没人手工操作它)，实盘侧则要求人不去插手已托管的标的。
  *
  * ## 手续费是估算的
  *
  * 交易所私有流目前没解析手续费，柜台那边倒是精确知道。**两边都用名义费率估算**，
  * 而不是"影子精确、实盘记 0" —— 后者会让两个账户的数字失去可比性，恰好毁掉这个类
  * 存在的理由。判据比较的是两边的**相对**表现，共同的估算偏差在比较中抵消。
  *
  * 代价要说清楚：这里的 `realizedPnl` 不是账单实数，不能拿去做对账。
  *
  * @param feeRate 名义费率 (成交名义额 × 该费率)。不区分 maker/taker —— [[Fill]] 里没有
  *                这个信息，取一个折中值即可，两边同一个数就够用了。
  */
final class PerformanceTracker(feeRate: Double, publishIntervalMs: Long = 1000) extends Actor:
  private val ledgers = mutable.Map.empty[AccountInstrument, Ledger]
  private val stats = mutable.Map.empty[AccountInstrument, Stats]
  private var ctx: ActorContext = scala.compiletime.uninitialized
  private var lastPublish: Timestamp = 0L
  /** 自上次发布以来有新成交的键 —— 只发它们，避免每节拍全量重发历史键把总线撑满 */
  private val dirty = mutable.Set.empty[AccountInstrument]

  private final case class Stats(fills: Int, roundTrips: Int, fees: Double, since: Timestamp)

  override def name: String = "performance-tracker"

  /** 全量收成交：跟踪谁由发布者决定，不必预先登记账户 */
  override def interests: Set[Interest] = Set(Interest.All(Topics.Fill), Interest.All(Topics.Clock))

  override def onStart(context: ActorContext): Unit = ctx = context

  override def onEvent(event: AnyEvent, now: Timestamp): Vector[AnyEvent] =
    event.as(Topics.Fill).foreach(fill => applyFill(fill, now))
    event.as(Topics.Clock).foreach(_ => publishAll(now))
    Vector.empty

  private def applyFill(fill: Fill, now: Timestamp): Unit =
    val key = AccountInstrument(fill.account, Instrument(fill.exchange, fill.symbol))
    val ledger = ledgers.getOrElse(key, Ledger.empty(fill.account, 0.0))
    val before = ledger.positions.get(fill.symbol).map(_.size).getOrElse(Coin.Zero)
    val fee = fill.size.notional(fill.price) * feeRate
    val next = ledger.applyFill(fill.exchange, fill.symbol, fill.side, fill.price, fill.size, fee)
    val after = next.positions.get(fill.symbol).map(_.size).getOrElse(Coin.Zero)
    ledgers(key) = next
    dirty += key

    val prev = stats.getOrElse(key, Stats(0, 0, 0.0, fill.timestamp))
    // 一次往返 = 一次下注有了结果：仓位归零算，**反手也算** ——
    // 反手那一笔已经把原仓位平掉、实现了盈亏。只认"归零"的话，
    // 一个始终反手换向、从不落平的策略 roundTrips 恒为 0，判据的样本量永远不够、永远不晋升。
    val flat = after.isZero
    val reversed = before.signum * after.signum < 0
    val closed = before.nonZero && (flat || reversed)
    stats(key) = prev.copy(
      fills = prev.fills + 1,
      roundTrips = prev.roundTrips + (if closed then 1 else 0),
      fees = prev.fees + fee.value,
    )

  private def publishAll(now: Timestamp): Unit =
    if now - lastPublish < publishIntervalMs then return
    lastPublish = now
    val toPublish = dirty.toVector
    dirty.clear()
    toPublish.flatMap(k => ledgers.get(k).map(k -> _)).foreach { (key, ledger) =>
      val st = stats.getOrElse(key, Stats(0, 0, 0.0, now))
      ctx.publish(Event.local(
        Performances,
        Performance(
          account = key.account,
          instrument = key.instrument,
          // Ledger 起始现金取 0，故 cash 即已实现盈亏 (已扣估算费用)
          realizedPnl = ledger.cash,
          fees = st.fees,
          fills = st.fills,
          roundTrips = st.roundTrips,
          position = ledger.positions.get(key.instrument.symbol).map(_.size).getOrElse(Coin.Zero),
          since = st.since,
          updatedAt = now,
        ),
      ))
    }

  /** 当前快照 (供测试与观测) */
  def snapshot(key: AccountInstrument): Option[Performance] =
    ledgers.get(key).map { ledger =>
      val st = stats.getOrElse(key, Stats(0, 0, 0.0, 0L))
      Performance(key.account, key.instrument, ledger.cash, st.fees, st.fills, st.roundTrips,
        ledger.positions.get(key.instrument.symbol).map(_.size).getOrElse(Coin.Zero), st.since, 0L)
    }
