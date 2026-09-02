package strategy.strategies.volsell.logic
import strategy.utils.option.*

import hft.indicator.RealizedVol
import hft.option.BlackScholes

import java.time.temporal.TemporalAdjusters
import java.time.{DayOfWeek, Instant, LocalTime, ZoneId, ZonedDateTime}

/** 卖方决策的**纯逻辑** (无 IO, 可单测): RV 计算 / 仓位倍数 / 选 21天 ATM 跨式 / 决策时点。 */
object SellVolPlan:
  /** 5 分钟 bar 的年化基准: 365×24×12 */
  val BarsPerYear5m: Double = BlackScholes.HoursPerYear * 12.0

  /** 由 5 分钟收盘价序列 (最旧->最新) 算年化实现波动 (复用 hft RealizedVol 公式, SSOT)。
    * 样本不足估不出 -> None (见 [[RealizedVol.annualizedFromPrices]])。 */
  def annualizedRv(closes: Seq[Double]): Option[Double] = RealizedVol.annualizedFromPrices(closes, BarsPerYear5m)

  /** 仓位定量结果：倍数与它依据的两周 RV。 */
  final case class SizeDecision(mult: Double, rvPrev: Double, rvThis: Double)

  /** 仓位倍数: 把最近 2 周 5min 收盘价对半分 (前半=上周, 后半=本周), 本周 RV **较上周上升**→[[gridHigh]] (卖更多),
    * 下降/持平→[[gridLow]]。
    *
    * 任一半估不出 RV 就返回 `None`：样本不足时曾经直接返回 `gridLow`，那是把"不知道波动是升是降"
    * 说成了"波动在降"——一个凭空得出的方向判断，而它决定卖出多少份。 */
  def decideMultiplier(closes2w: Seq[Double], gridHigh: Double, gridLow: Double): Option[SizeDecision] =
    val mid = closes2w.size / 2
    for
      rvPrev <- annualizedRv(closes2w.take(mid))  // 上周
      rvThis <- annualizedRv(closes2w.drop(mid))  // 本周
    yield SizeDecision(if rvThis > rvPrev then gridHigh else gridLow, rvPrev, rvThis)

  /** 一天的毫秒数 (到期/锚点计算) */
  val DayMs: Long = 86_400_000L

  /** 决策锚点: 常规调度 = **本周五 17:00** ([[currentDecisionTime]]); **runNow = 上周五 17:00**
    * ([[lastDecisionTime]])。targetExpiry 与 orderLinkId 共用此锚点, 保证 runNow 以上周五为基准且自洽幂等。 */
  def decisionAnchor(nowMs: Long, runNow: Boolean, zone: ZoneId = ZoneId.of("Asia/Shanghai"), decisionHour: Int = 17): Long =
    if runNow then lastDecisionTime(nowMs, zone, decisionHour) else currentDecisionTime(nowMs, zone, decisionHour)

  /** 目标到期 = **决策锚点 + targetDays** (默认 21 天后那个周五的周度期权)。锚点在周五且 21=3×7,
    * 故 +targetDays 仍落周五; 周度期权恒周五交割, [[selectStrangle]] 据此选最近到期。 */
  def targetExpiryMs(anchorMs: Long, targetDays: Int): Long =
    anchorMs + targetDays.toLong * DayMs

  /** 卖价决策的两个参数，**单位是中价的比例**，不是绝对价格。
    *
    * ## 为什么不能是绝对值
    *
    * 从前它们是 `0.3` / `0.2` 个报价货币单位，而报价货币是**交易所的事实**：
    * Bybit 的 USDT 期权按美元报价 (周度 OTM 权利金常在几十美元)，而 OKX 的币本位期权
    * `px` 以标的币计 (ETH 期权约 0.005–0.1 ETH)。同一份常量跨过 `OptionsExchange` 抽象后：
    * 在 OKX 上价差恒 `<= 0.3` -> **永远走 taker**；万一走到 maker 分支，`mid - 0.2` 还是负数。
    * `OkxVolSellLauncher` 宣称"策略逻辑零改动"，恰恰把这处单位泄漏盖住了。
    *
    * 比例形式在两家都成立，而且对便宜的期权也成立 —— 绝对值 0.3 对一张 1 美元的期权是 30%。
    *
    * ## 默认值的来历
    *
    * `SpreadTakerCutoffRatio` 由原来的绝对值在 **Bybit ETH 周度期权的典型权利金量级 (~40 USDT)**
    * 上换算而来：`0.3/40 ≈ 0.75%`。**换交易所或换标的请重新校准** —— 它是微观结构参数，
    * 不是普适常数。
    *
    * `PassiveOffsetRatio` **不是独立调参, 而是由 cutoff 派生**：让价超过半个 cutoff 价差,
    * maker 单就会报到买一以下而被拒 (推导见 [[sellQuote]])。原来的一对绝对值
    * `0.3` / `0.2` 恰好破了这条关系, 于是相对价差落在 0.75%~1.0% 时永远挂不上单。
    * 派生之后这个失效形态在类型层面就不可能出现。 */
  val SpreadTakerCutoffRatio: Double = 0.0075
  val PassiveOffsetRatio: Double = SpreadTakerCutoffRatio / 2

  /** 由盘口 [[Quote]] 定卖价与下单方式: 相对价差 ≤ cutoffRatio -> (对手价=买一, taker);
    * 否则 (中价 × (1 − offsetRatio) 并**按 tick 向上对齐**, maker)。
    *
    * ## 两个参数之间的约束
    *
    * maker 卖单要挂得住, 价格必须**严格高于买一**: `mid×(1−offset) > bid ⟺ spread > 2×mid×offset`。
    * 而进 maker 分支只保证 `spread > mid×cutoff`。于是 `cutoff < 2×offset` 时存在一段价差
    * (原来的 0.75%~1.0%) 落进 maker 分支却报出低于买一的价 —— postOnly 单**必被交易所拒**,
    * 表现是"这一档价差下永远挂不上单"。所以 `cutoffRatio >= 2 × offsetRatio` 是前置条件, 不是调参建议。
    *
    * ## 为什么向上对齐
    *
    * `tickSize` 两家客户端都解析了却从来没人用, 而落在 tick 网格之外会被交易所按精度拒。
    * 对齐方向要选**远离对手盘**的那侧: 卖方向上 = 多收一点权利金, 且不会因对齐把价格推到买一
    * 以下而触发 postOnly 拒单。向下对齐两头都亏 —— 白让权利金, 还增加穿价概率
    * (OKX 币本位 ETH 期权 `tickSz=0.0001`, 权利金 0.005 ETH 时一个 tick 就是 2%, 远大于 0.5% 的 offset)。
    *
    * 返回 (卖价, postOnly)。 */
  def sellQuote(
      q: Quote,
      tickSize: Double,
      cutoffRatio: Double = SpreadTakerCutoffRatio,
      offsetRatio: Double = PassiveOffsetRatio,
  ): (Double, Boolean) =
    require(tickSize > 0, s"报价最小变动单位必须为正, 实际 $tickSize")
    require(
      cutoffRatio >= 2 * offsetRatio,
      s"cutoffRatio ($cutoffRatio) 须 >= 2×offsetRatio ($offsetRatio) —— 否则 maker 分支会报出低于买一的价, postOnly 必被拒",
    )
    if q.spread <= q.mid * cutoffRatio then (q.bid, false) // 价差窄: 对手价(买一)直接成交 (taker)
    else
      // 价差宽: 公允(中)价让出 offsetRatio 挂单 (maker), 并向上对齐到 tick 网格 (远离对手盘)
      val raw = q.mid * (1.0 - offsetRatio)
      val ticks = BigDecimal(math.ceil(raw / tickSize - 1e-9))
      ((ticks * BigDecimal(tickSize)).toDouble, true)

  /** 从期权链选 **离 targetExpiryMs 最近的到期、当前价位最近的宽跨 (strangle)**:
    * 同一到期内, call 取**严格高于 spot 的最小行权** (OTM), put 取**严格低于 spot 的最大行权** (OTM)。
    * 任一侧无价外行权 (spot 超出行权范围) 则无宽跨。 */
  def selectStrangle(
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
      for
        callK <- atExpiry.filter(i => i.right == OptionRight.Call && i.strike > spot).map(_.strike).minOption
        putK <- atExpiry.filter(i => i.right == OptionRight.Put && i.strike < spot).map(_.strike).maxOption
        call <- atExpiry.find(i => i.strike == callK && i.right == OptionRight.Call)
        put <- atExpiry.find(i => i.strike == putK && i.right == OptionRight.Put)
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

  /** 按交易所 qtyStep 向下取整并校验 minQty: 返回合规下单量, 低于最小量返回 None。
    * `qtyStep`/`minQty` 必须为正 (由 [[OptionInstrument]] 的不变量保证)。
    * 判据在 [[OptionQty.alignDown]] —— 与 IV 定量卖方策略共用一份, 两处各写会在同一交易所上给出
    * 不同答案而没有任何编译错误。 */
  def quantizeQty(qty: Double, qtyStep: Double, minQty: Double): Option[Double] =
    OptionQty.alignDown(qty, qtyStep, minQty)
