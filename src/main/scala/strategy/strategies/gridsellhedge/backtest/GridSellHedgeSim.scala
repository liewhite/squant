package strategy.strategies.gridsellhedge.backtest

import hft.domain.Side
import hft.indicator.RealizedVol
import hft.option.{BlackScholes, OptionRight}
import strategy.strategies.gridsellhedge.logic.DynamicHedgeBand
import strategy.strategies.gridsellhedge.logic.OptionGrid

import scala.collection.mutable

/** 网格卖方 + 动态阈值备兑对冲 回测参数 (常量集中, 单一数据源)。 */
final case class GridConfig(
    spacing: Double = 100.0,           // 网格间距 (整数位)
    tenorDays: Double = 30.0,          // 卖 30 天后到期的期权
    nodeContracts: Double = 1.0,       // 每档卖出张数 (每张 = 1 单位标的)
    rvWindowHours: Int = 72,           // 已实现波动 (作卖出 IV) 的回看小时数
    warmupHours: Int = 72,             // 预热: 攒够 RV 样本后才开始建网格
    pollIntervalMs: Long = 3000,       // 每 3 秒检查一次 (轮询驱动, 替代逐笔驱动)
    makerOffsetPct: Double = 0.0001,   // 对冲挂单在 BBO 外 0.01% (被动 maker)
    optFeeRate: Double = 0.0003,       // 期权成交费率 (按标的名义, 卖出时收)
    perpFeeRate: Double = 0.0002,      // 对冲腿 (永续 maker) 成交费率 (开+平各收)
    slippagePct: Double = 0.0,         // 对冲腿额外滑点 (作为显式成本; maker 成交于限价, 默认 0)
    riskFreeRate: Double = 0.0,
    initialBalance: Double = 1_000_000.0,
    minIv: Double = 1e-6,              // 卖出定价的 IV 下限 (窗口内每根收益恰为 0 的静止市场)
    band: DynamicHedgeBand.Params = DynamicHedgeBand.Params(),
):
  require(spacing > 0 && tenorDays > 0 && nodeContracts > 0 && pollIntervalMs > 0)
  // n 个小时采样只给出 n-1 个对数收益, 年化波动至少要 2 个收益 -> 至少 3 个采样。
  // 这条从前没写, 于是 warmupHours=2 的配置在建第一档网格时 RV 只有 1 个收益, 被静默当成 0。
  require(rvWindowHours >= 3, s"rvWindowHours 至少为 3 (n 个采样只有 n-1 个收益), 实际 $rvWindowHours")
  require(warmupHours >= 3, s"warmupHours 至少为 3 (预热要攒够 2 个对数收益), 实际 $warmupHours")
  require(warmupHours <= rvWindowHours, s"warmupHours($warmupHours) 不能超过 rvWindowHours($rvWindowHours), 否则预热永不结束")

object GridSellHedgeSim:
  /** 备兑腿: 建仓价与带符号数量 (>0 多标的=对冲短 call, <0 空标的=对冲短 put)。 */
  final case class Hedge(entryPrice: Double, qty: Double)

  /** 挂在永续上的一张对冲限价单。`side` 买/卖, `limit` 限价, `open` 是开仓单(true)还是平仓单(false)。 */
  final case class Resting(side: Side, limit: Double, open: Boolean)

  /** 回测统计。 */
  final case class Stats(
      sold: Int,
      expired: Int,
      hedgeOpens: Int,
      hedgeCloses: Int,
      ordersPlaced: Int,
      optRealized: Double,
      hedgeRealized: Double,
      feesPaid: Double,
      totalRealized: Double,
      openPositions: Int,
  )

/** 一个空头期权持仓 (含其独立的阈值状态、备兑腿与挂单)。IV 卖出时固定, 全程用它定价。 */
private final class ShortOpt(
    val right: OptionRight,
    val strike: Double,
    val expiry: Long,
    val contracts: Double,
    val iv: Double,
    val entryPremium: Double,
    var band: DynamicHedgeBand.State,
    var hedge: Option[GridSellHedgeSim.Hedge],
    var order: Option[GridSellHedgeSim.Resting],
)

/** 网格卖方 + 动态阈值备兑对冲 的**轮询驱动**模拟器 (每 [[GridConfig.pollIntervalMs]] 检查一次)。
  *
  *   - **规则 1 (每次轮询)**: 维护当前价上下最近两档各有空头 ([[OptionGrid.bracket]]), 缺则补卖。
  *   - **规则 2 (每次轮询)**: 每档独立。达到开仓阈值且无挂单 → 在 BBO 外 0.01% 挂 maker 单; 已有挂单 → 撤单重挂 (追价)。
  *     成交才真正开/平对冲 (maker 有不成交风险)。开仓阈值触发即随成交放大 ×1.2, 回到 0.24% 内挂平仓单, 未触发按小时衰减。
  *
  * maker 成交模型: 逐笔跟踪两次轮询之间的 tape 高/低; 上一轮挂的买单若窗口最低 ≤ 限价则成交于限价, 卖单若窗口最高 ≥ 限价则成交。
  *
  * 记账 (无重复计): `equity = 初始 + 已实现 + Σ未实现(期权 BS 市值) + Σ未实现(备兑腿, 中间价)`。 */
final class GridSellHedgeSim(config: GridConfig):
  import GridSellHedgeSim.{Hedge, Resting}

  private val HourMs = 3_600_000L
  private val MsPerYear = BlackScholes.MillisPerYear
  private val tenorMs = math.round(config.tenorDays * 24.0 * HourMs)

  private val positions = mutable.ArrayBuffer.empty[ShortOpt]
  private val occ = mutable.HashSet.empty[(OptionRight, Double)]
  private val hourBuf = mutable.ArrayDeque.empty[Double]
  private val equityBuf = mutable.ArrayBuffer.empty[(Long, Double, Double, Double)]
  private val fillBuf = mutable.ArrayBuffer.empty[(Long, Side, Double, Double)]

  private var lastPx = Double.NaN
  private var lastNow = 0L
  private var lastHour = Long.MinValue
  private var firstPx = 0.0
  private var lastPollTs = Long.MinValue
  private var winHigh = Double.NaN // 本轮询窗口 tape 最高
  private var winLow = Double.NaN  // 本轮询窗口 tape 最低

  private var realized = 0.0
  private var optRealized = 0.0
  private var hedgeRealized = 0.0
  private var feesPaid = 0.0
  private var sold = 0
  private var expired = 0
  private var hedgeOpens = 0
  private var hedgeCloses = 0
  private var ordersPlaced = 0

  // ---- 定价 ----
  private def tYears(now: Long, expiry: Long): Double = math.max(0.0, (expiry - now).toDouble / MsPerYear)
  private def bsPrice(right: OptionRight, s: Double, k: Double, ty: Double, iv: Double): Double =
    BlackScholes.greeks(right, s, k, ty, iv, config.riskFreeRate).price
  private def intrinsic(right: OptionRight, s: Double, k: Double): Double = right match
    case OptionRight.Call => math.max(s - k, 0.0)
    case OptionRight.Put  => math.max(k - s, 0.0)
  /** 窗口 RV。估不出即抛 —— 调用点都在 `warmupHours` 预热闸门之后, 拿不到值说明窗口配置有误,
    * 而以 0 顶替会让 [[BlackScholes]] 把期权按内在价值定价、希腊值全归零。 */
  private def currentRv: Double =
    RealizedVol
      .annualizedFromPrices(hourBuf, BlackScholes.HoursPerYear)
      .getOrElse(
        sys.error(
          s"RV 窗口只有 ${hourBuf.size} 个小时采样, 估不出年化波动 " +
            s"(warmupHours=${config.warmupHours}, rvWindowHours=${config.rvWindowHours})"
        )
      )
  private def netPos: Double = positions.iterator.flatMap(_.hedge).map(_.qty).sum

  /** 逐笔驱动: 更新价/RV/窗口高低, 每 pollIntervalMs 触发一次轮询决策。 */
  def onPrice(price: Double, now: Long): Unit =
    if firstPx == 0.0 then firstPx = price
    val h = now / HourMs
    val hourTick = h != lastHour
    if hourTick then
      if lastHour != Long.MinValue then
        hourBuf.append(price)
        while hourBuf.sizeIs > config.rvWindowHours do hourBuf.removeHead()
      lastHour = h
    lastPx = price; lastNow = now

    if hourBuf.sizeIs < config.warmupHours then return // 预热: 只记价/RV

    // 窗口 tape 高低 (供 maker 成交判定)
    if winHigh.isNaN then { winHigh = price; winLow = price } else { winHigh = math.max(winHigh, price); winLow = math.min(winLow, price) }
    if lastPollTs == Long.MinValue then lastPollTs = now
    if now - lastPollTs >= config.pollIntervalMs then
      poll(price, now)
      winHigh = price; winLow = price; lastPollTs = now
    if hourTick then equityBuf.append((now, equityAt(price, now), price, netPos))

  /** 一次轮询: 先撮合上一轮挂单, 再到期结算, 再补网格, 最后管理各档对冲挂单。 */
  private def poll(price: Double, now: Long): Unit =
    positions.foreach(p => matchResting(p, now))                                          // 1. maker 成交
    expire(price, now)                                                                    // 2. 到期
    OptionGrid.bracket(price, config.spacing).filterNot(s => occ.contains((s.right, s.strike)))
      .foreach(s => sell(s.right, s.strike, price, now))                                  // 3. 补网格
    positions.foreach(p => manageHedge(p, price, now))                                    // 4. 挂/撤对冲单

  /** 卖出一档空头期权 (规则 1)。 */
  private def sell(right: OptionRight, strike: Double, price: Double, now: Long): Unit =
    val iv = math.max(currentRv, config.minIv)
    val expiry = now + tenorMs
    val premium = config.nodeContracts * bsPrice(right, price, strike, tYears(now, expiry), iv)
    val fee = config.optFeeRate * config.nodeContracts * price
    realized -= fee; feesPaid += fee
    positions += ShortOpt(right, strike, expiry, config.nodeContracts, iv, premium, DynamicHedgeBand.initial(now, config.band), None, None)
    occ += ((right, strike))
    sold += 1

  /** 到期结算: 撤挂单、平掉对冲腿、按内在价值结清、释放网格档。
    * 近似: 结算价用检测到到期这一轮的 price (轮询粒度 ≤3s + 每小时必到), 对 3 天期期权可接受。 */
  private def expire(price: Double, now: Long): Unit =
    var i = 0
    while i < positions.size do
      val p = positions(i)
      if now >= p.expiry then
        p.order = None
        p.hedge.foreach(h => closeHedgeAt(p, h, price, now)) // 强制平仓于中间价
        val pnl = p.entryPremium - p.contracts * intrinsic(p.right, price, p.strike)
        realized += pnl; optRealized += pnl
        occ -= ((p.right, p.strike))
        positions.remove(i)
        expired += 1
      else i += 1

  /** 撮合该档上一轮挂的 maker 单: 买单窗口最低 ≤ 限价 / 卖单窗口最高 ≥ 限价 → 成交于限价。
    *
    * 乐观假设 (回测简化): 不建模队列位置/成交量/部分成交 —— 只要窗口内价格触及限价即视为**全额**成交于限价。
    * 对 3 天期、单张标的的低频轮询回测可接受; 实盘 maker 可能因排队而少成交, 故本模型倾向高估对冲成交率。 */
  private def matchResting(pos: ShortOpt, now: Long): Unit =
    pos.order.foreach { o =>
      val filled = o.side match
        case Side.Long  => !winLow.isNaN && winLow <= o.limit
        case Side.Short => !winHigh.isNaN && winHigh >= o.limit
      if filled then
        pos.order = None
        if o.open then fillOpen(pos, o.limit, now) else fillClose(pos, o.limit, now)
    }

  /** 管理该档对冲挂单 (规则 2 执行): 按当前状态计算目标挂单, 与现挂单不同 (价变/开平变) 才撤单重挂 (追价)。
    * 仅在挂单真正变化时计数, 避免价未动时把"维持原挂单"重复计入 ordersPlaced。
    *
    * 开仓: 价越过行权价 (d ≥ 0) 立即对冲; 平仓: 已持对冲且价回落超过离场阈值 (d ≤ −threshold)。 */
  private def manageHedge(pos: ShortOpt, price: Double, now: Long): Unit =
    val d = DynamicHedgeBand.riskyDistance(pos.right, pos.strike, price)
    if pos.hedge.isEmpty then pos.band = DynamicHedgeBand.decayed(pos.band, now, config.band) // 未持对冲才衰减
    val desired: Option[Resting] =
      if pos.hedge.isEmpty then
        if d >= 0.0 then Some(makeOrder(pos, open = true, price)) else None
      else if d <= -pos.band.threshold then Some(makeOrder(pos, open = false, price))
      else None
    if pos.order != desired then // 撤单重挂 (仅在目标变化时)
      pos.order = desired
      if desired.isDefined then ordersPlaced += 1

  /** 目标 maker 单: BBO 外 0.01%。开短 call→买/短 put→卖; 平仓为反向。买单挂低于价、卖单挂高于价 (被动)。 */
  private def makeOrder(pos: ShortOpt, open: Boolean, price: Double): Resting =
    val off = config.makerOffsetPct
    val buy = // 需要买入标的?
      if open then pos.right == OptionRight.Call // 开: 短call买
      else pos.hedge.exists(_.qty < 0)           // 平: 平掉空标的=买
    if buy then Resting(Side.Long, price * (1 - off), open) else Resting(Side.Short, price * (1 + off), open)

  private def perpCost(qty: Double, price: Double): Double = (config.perpFeeRate + config.slippagePct) * math.abs(qty) * price

  /** 开仓单成交: 建备兑腿, 放大阈值。 */
  private def fillOpen(pos: ShortOpt, fillPrice: Double, now: Long): Unit =
    val qty = pos.right match
      case OptionRight.Call => pos.contracts
      case OptionRight.Put  => -pos.contracts
    val cost = perpCost(qty, fillPrice)
    realized -= cost; feesPaid += cost
    pos.hedge = Some(Hedge(fillPrice, qty))
    pos.band = DynamicHedgeBand.expandOnOpen(pos.band, config.band)
    fillBuf.append((now, if qty > 0 then Side.Long else Side.Short, fillPrice, math.abs(qty)))
    hedgeOpens += 1

  /** 平仓单成交: 了结备兑腿, 重置衰减时钟。 */
  private def fillClose(pos: ShortOpt, fillPrice: Double, now: Long): Unit =
    pos.hedge.foreach { h =>
      val pnl = h.qty * (fillPrice - h.entryPrice)
      val cost = perpCost(h.qty, fillPrice)
      realized += pnl - cost; hedgeRealized += pnl; feesPaid += cost
      pos.hedge = None
      pos.band = DynamicHedgeBand.resetClock(pos.band, now)
      fillBuf.append((now, if h.qty > 0 then Side.Short else Side.Long, fillPrice, math.abs(h.qty)))
      hedgeCloses += 1
    }

  /** 强制平仓于中间价 (到期路径, 非 maker)。 */
  private def closeHedgeAt(pos: ShortOpt, h: Hedge, price: Double, now: Long): Unit = fillClose(pos, price, now)

  /** 某价/时点账户权益 = 初始 + 已实现 + Σ期权未实现(BS市值) + Σ备兑腿未实现(中间价)。 */
  def equityAt(price: Double, now: Long): Double =
    var eq = config.initialBalance + realized
    positions.foreach { p =>
      val value = p.contracts * bsPrice(p.right, price, p.strike, tYears(now, p.expiry), p.iv)
      eq += p.entryPremium - value
      p.hedge.foreach(h => eq += h.qty * (price - h.entryPrice))
    }
    eq

  // ---- 结果访问 ----
  def finalEquity: Double = equityAt(if lastPx.isNaN then firstPx else lastPx, lastNow)
  def equitySamples: Vector[(Long, Double, Double, Double)] = equityBuf.toVector
  def fills: Vector[(Long, Side, Double, Double)] = fillBuf.toVector
  def firstPrice: Double = firstPx
  def lastPrice: Double = if lastPx.isNaN then firstPx else lastPx
  def openPositions: Int = positions.size
  def netUnderlying: Double = netPos
  /** 某档当前的离场阈值 (供观测/测试: 验证阈值随成交放大)。 */
  def thresholdOf(right: OptionRight, strike: Double): Option[Double] =
    positions.find(p => p.right == right && p.strike == strike).map(_.band.threshold)
  def stats: GridSellHedgeSim.Stats =
    GridSellHedgeSim.Stats(sold, expired, hedgeOpens, hedgeCloses, ordersPlaced, optRealized, hedgeRealized, feesPaid, realized, openPositions)
