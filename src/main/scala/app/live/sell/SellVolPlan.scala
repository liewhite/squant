package app.live.sell
import app.live.option.*

import hft.indicator.RealizedVol
import hft.option.BlackScholes

import java.time.temporal.TemporalAdjusters
import java.time.{DayOfWeek, Instant, LocalTime, ZoneId, ZonedDateTime}

/** 卖方决策的**纯逻辑** (无 IO, 可单测): RV 计算 / 仓位倍数 / 选 21天 ATM 跨式 / 决策时点。 */
object SellVolPlan:
  /** 5 分钟 bar 的年化基准: 365×24×12 */
  val BarsPerYear5m: Double = BlackScholes.HoursPerYear * 12.0

  /** 由 5 分钟收盘价序列 (最旧->最新) 算年化实现波动 (复用 hft RealizedVol 公式, SSOT) */
  def annualizedRv(closes: Seq[Double]): Double = RealizedVol.annualizedFromPrices(closes, BarsPerYear5m)

  /** 仓位倍数: 把最近 2 周 5min 收盘价对半分 (前半=上周, 后半=本周), 本周 RV **较上周上升**→[[gridHigh]] (卖更多),
    * 下降/持平→[[gridLow]]。返回 (倍数, 上周RV, 本周RV)。 */
  def decideMultiplier(closes2w: Seq[Double], gridHigh: Double, gridLow: Double): (Double, Double, Double) =
    if closes2w.sizeIs < 4 then (gridLow, 0.0, 0.0)
    else
      val mid = closes2w.size / 2
      val rvPrev = annualizedRv(closes2w.take(mid))   // 上周
      val rvThis = annualizedRv(closes2w.drop(mid))   // 本周
      val mult = if rvThis > rvPrev then gridHigh else gridLow
      (mult, rvPrev, rvThis)

  /** 一天的毫秒数 (到期/锚点计算) */
  val DayMs: Long = 86_400_000L

  /** 决策锚点: 常规调度 = **本周五 17:00** ([[currentDecisionTime]]); **runNow = 上周五 17:00**
    * ([[lastDecisionTime]])。targetExpiry 与 orderLinkId 共用此锚点, 保证 runNow 以上周五为基准且自洽幂等。 */
  def decisionAnchor(nowMs: Long, runNow: Boolean, zone: ZoneId = ZoneId.of("Asia/Shanghai"), decisionHour: Int = 17): Long =
    if runNow then lastDecisionTime(nowMs, zone, decisionHour) else currentDecisionTime(nowMs, zone, decisionHour)

  /** 目标到期 = **决策锚点 + targetDays** (默认 21 天后那个周五的周度期权)。锚点在周五且 21=3×7,
    * 故 +targetDays 仍落周五; 周度期权恒周五交割, [[selectStraddle]] 据此选最近到期。 */
  def targetExpiryMs(anchorMs: Long, targetDays: Int): Long =
    anchorMs + targetDays.toLong * DayMs

  /** 从期权链选 **离 targetExpiryMs 最近的到期、ATM** 的跨式 (call+put 同行权同到期)。
    * 先选交割时间最接近 targetExpiryMs 的到期, 再在该到期内取行权价最接近 spot 的 call/put。 */
  def selectStraddle(
      chain: Seq[OptionInstrument],
      nowMs: Long,
      spot: Double,
      targetExpiryMs: Long,
  ): Option[(OptionInstrument, OptionInstrument)] =
    val futures = chain.filter(_.expiryMs > nowMs)
    if futures.isEmpty then None
    else
      val expiry = futures.minBy(i => math.abs(i.expiryMs - targetExpiryMs)).expiryMs
      val atExpiry = futures.filter(_.expiryMs == expiry)
      val strikes = atExpiry.map(_.strike).distinct
      if strikes.isEmpty then None
      else
        val atm = strikes.minBy(k => math.abs(k - spot))
        for
          call <- atExpiry.find(i => i.strike == atm && i.right == OptionRight.Call)
          put <- atExpiry.find(i => i.strike == atm && i.right == OptionRight.Put)
        yield (call, put)

  /** 下一个**决策时点** = from 之后最近的"周五 decisionHour:00" (在 zone 时区), 返回 ms epoch。
    * 默认 zone=Asia/Shanghai (北京时间)、decisionHour=17 (晚于期权 16:00 北京交割, 当周已结算)。 */
  def nextDecisionTime(fromMs: Long, zone: ZoneId = ZoneId.of("Asia/Shanghai"), decisionHour: Int = 17): Long =
    val from = ZonedDateTime.ofInstant(Instant.ofEpochMilli(fromMs), zone)
    val thisFri = from.`with`(TemporalAdjusters.nextOrSame(DayOfWeek.FRIDAY)).`with`(LocalTime.of(decisionHour, 0))
    val next = if thisFri.isAfter(from) then thisFri else thisFri.plusWeeks(1)
    next.toInstant.toEpochMilli

  /** 当前**决策周期锚点** = from 当下或之前最近的"周五 decisionHour:00" (ms epoch)。
    * 用于派生**幂等** orderLinkId (同一周决策无论重启/重试都得同一 link, 交易所据此拒重复单)。 */
  def currentDecisionTime(fromMs: Long, zone: ZoneId = ZoneId.of("Asia/Shanghai"), decisionHour: Int = 17): Long =
    val from = ZonedDateTime.ofInstant(Instant.ofEpochMilli(fromMs), zone)
    val cand = from.`with`(TemporalAdjusters.previousOrSame(DayOfWeek.FRIDAY)).`with`(LocalTime.of(decisionHour, 0))
    val anchor = if cand.isAfter(from) then cand.minusWeeks(1) else cand // prevOrSame 落在周五但时刻可能晚于 from
    anchor.toInstant.toEpochMilli

  /** **上周五**决策锚点 = from 之前**严格最近**的"周五 decisionHour:00" (若 from 当天即周五也回退到上一周五)。
    * runNow 用它: 以上周五为基准往后 targetDays 天选到期 (而非用本周五/当前时刻)。 */
  def lastDecisionTime(fromMs: Long, zone: ZoneId = ZoneId.of("Asia/Shanghai"), decisionHour: Int = 17): Long =
    val from = ZonedDateTime.ofInstant(Instant.ofEpochMilli(fromMs), zone)
    from.`with`(TemporalAdjusters.previous(DayOfWeek.FRIDAY)).`with`(LocalTime.of(decisionHour, 0)).toInstant.toEpochMilli

  /** 按交易所 qtyStep 向下取整并校验 minQty: 返回合规下单量, 低于最小量返回 None。step<=0 时只校验 minQty。 */
  def quantizeQty(qty: Double, qtyStep: Double, minQty: Double): Option[Double] =
    val q = if qtyStep > 0 then math.floor(qty / qtyStep + 1e-9) * qtyStep else qty
    val rounded = if qtyStep > 0 then math.round(q / qtyStep) * qtyStep else q // 消除浮点尾差
    if rounded >= minQty && rounded > 0 then Some(rounded) else None
