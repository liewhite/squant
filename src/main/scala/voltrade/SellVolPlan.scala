package voltrade

import hft.indicator.RealizedVol

import java.time.temporal.TemporalAdjusters
import java.time.{DayOfWeek, Instant, LocalTime, ZoneId, ZonedDateTime}

/** 卖方决策的**纯逻辑** (无 IO, 可单测): RV 计算 / 仓位倍数 / 选 21天 ATM 跨式 / 决策时点。 */
object SellVolPlan:
  /** 5 分钟 bar 的年化基准: 365×24×12 */
  val BarsPerYear5m: Double = 365.0 * 24.0 * 12.0

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

  /** 从期权链选 **~targetDays 到期、ATM** 的跨式 (call+put 同行权同到期)。
    * 先选交割时间最接近 now+targetDays 的到期, 再在该到期内取行权价最接近 spot 的 call/put。 */
  def selectStraddle(
      chain: Seq[OptionInstrument],
      nowMs: Long,
      spot: Double,
      targetDays: Int,
  ): Option[(OptionInstrument, OptionInstrument)] =
    val targetMs = nowMs + targetDays.toLong * 86_400_000L
    val futures = chain.filter(_.expiryMs > nowMs)
    if futures.isEmpty then None
    else
      val expiry = futures.minBy(i => math.abs(i.expiryMs - targetMs)).expiryMs
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
    * 默认 zone=Asia/Shanghai (北京时间)、decisionHour=15。 */
  def nextDecisionTime(fromMs: Long, zone: ZoneId = ZoneId.of("Asia/Shanghai"), decisionHour: Int = 15): Long =
    val from = ZonedDateTime.ofInstant(Instant.ofEpochMilli(fromMs), zone)
    val thisFri = from.`with`(TemporalAdjusters.nextOrSame(DayOfWeek.FRIDAY)).`with`(LocalTime.of(decisionHour, 0))
    val next = if thisFri.isAfter(from) then thisFri else thisFri.plusWeeks(1)
    next.toInstant.toEpochMilli

  /** 当前**决策周期锚点** = from 当下或之前最近的"周五 decisionHour:00" (ms epoch)。
    * 用于派生**幂等** orderLinkId (同一周决策无论重启/重试都得同一 link, 交易所据此拒重复单)。 */
  def currentDecisionTime(fromMs: Long, zone: ZoneId = ZoneId.of("Asia/Shanghai"), decisionHour: Int = 15): Long =
    val from = ZonedDateTime.ofInstant(Instant.ofEpochMilli(fromMs), zone)
    val cand = from.`with`(TemporalAdjusters.previousOrSame(DayOfWeek.FRIDAY)).`with`(LocalTime.of(decisionHour, 0))
    val anchor = if cand.isAfter(from) then cand.minusWeeks(1) else cand // prevOrSame 落在周五但时刻可能晚于 from
    anchor.toInstant.toEpochMilli

  /** 按交易所 qtyStep 向下取整并校验 minQty: 返回合规下单量, 低于最小量返回 None。step<=0 时只校验 minQty。 */
  def quantizeQty(qty: Double, qtyStep: Double, minQty: Double): Option[Double] =
    val q = if qtyStep > 0 then math.floor(qty / qtyStep + 1e-9) * qtyStep else qty
    val rounded = if qtyStep > 0 then math.round(q / qtyStep) * qtyStep else q // 消除浮点尾差
    if rounded >= minQty && rounded > 0 then Some(rounded) else None
