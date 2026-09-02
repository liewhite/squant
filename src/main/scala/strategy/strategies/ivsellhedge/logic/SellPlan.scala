package strategy.strategies.ivsellhedge.logic

import strategy.utils.option.{OptionHolding, OptionInstrument, OptionQty, OptionRight, Quote}

import hft.domain.Timestamp
import hft.option.BlackScholes

/** IV 定量的**纯决策**：目标张数 / 选到期 / 选行权 / 三道闸门 / 杠杆率 —— 无 IO，可直接断言。
  *
  * 范式是**声明式对账**：每轮算出"这两条腿各该有多少空头张数"，比当前持仓少就补卖。
  * 因此天然自愈 —— 下单未成交、进程重启、手工平仓，下一轮继续补足；也无需任何持久化状态。
  * 反过来，IV 回落只让目标变小，**不主动平仓**：已有仓位只是不再加卖。
  */
object SellPlan:

  /** IV 定量的四个参数。
    *
    * {{{
    * 目标张数 = 0                                                      iv <  ivStart
    * 目标张数 = floor( min( qtyStart + (iv − ivStart)/0.01 × qtySlope,
    *                        qtyMax ) )                                 iv >= ivStart
    * }}}
    *
    * @param ivStart   起卖点 (年化 IV, 0.2 = 20%)：低于它一张不卖 (硬门槛)
    * @param qtyStart  起卖量：刚到起卖点就卖的张数
    * @param qtySlope  斜率：**每 1 个波动率点 (1% IV)** 增加的张数
    * @param qtyMax    目标张数上限：兜住极端行情与异常 markVol，防单轮天量卖单
    */
  final case class IvQty(ivStart: Double, qtyStart: Double, qtySlope: Double, qtyMax: Double):
    /** 装配期校验 —— 越界即抛。`qtyStart > qtyMax` 会让起点被上限静默改写，
      * 那种"配了却没生效"的参数比报错危险得多。 */
    def validated: IvQty =
      // 要求 > 0 而非 >= 0: ivStart=0 意味着从零波动就开始线性放大, IV 稍高就顶到上限,
      // 那不是任何人想要的"起卖点", 更可能是漏配
      require(ivStart > 0.0, s"ivStart 须 > 0 (起卖点), 实为 $ivStart")
      require(qtySlope > 0.0, s"qtySlope 须 > 0 (否则 IV 缩放没有意义), 实为 $qtySlope")
      require(qtyMax >= 1.0, s"qtyMax 须 >= 1, 实为 $qtyMax")
      require(qtyStart >= 0.0 && qtyStart <= qtyMax, s"须 0 <= qtyStart($qtyStart) <= qtyMax($qtyMax)")
      this

  /** 一个波动率点 = 1% IV (斜率的单位基准, 不是魔法值) */
  val IvPoint: Double = 0.01

  /** 目标张数：与 IV **正相关**，低于起卖点为 0。中间计算全程保留小数，最后一步才向下对齐到整张。
    *
    * 向下取整前加极小量补偿浮点尾差 —— 否则恰好落在整点上的值 (如算出 1.0 张) 会被 floor 吞成 0。
    */
  def targetContracts(iv: Double, cfg: IvQty): Double =
    if iv.isNaN || iv < cfg.ivStart then 0.0
    else
      val raw = cfg.qtyStart + (iv - cfg.ivStart) / IvPoint * cfg.qtySlope
      math.floor(math.min(raw, cfg.qtyMax) + 1e-9)

  /** 选到期：剩余期限 < [[minTtlMs]] 的一律排除，其余取离 `nowMs + targetDays` **最近**的一档；
    * 距离相等时取**更晚**的一档 (使结果只由日期决定，不受候选集合迭代序影响)。
    *
    * 排除近到期不是保守 —— 当日到期的合约权利金≈0 而 gamma 极大，风险画像与策略意图完全不同。
    * 判据用"剩余期限"而不是"是不是今天到期"：后者要挑一个时区，而时区选错会在跨日附近静默改变
    * 选中的合约；剩余期限本来就是真正相关的那个量，且与时区无关。
    */
  def selectExpiry(chain: Seq[OptionInstrument], nowMs: Timestamp, targetDays: Int, minTtlMs: Long): Option[Long] =
    val target = nowMs + targetDays.toLong * DayMs
    chain.iterator
      .map(_.expiryMs)
      .filter(_ - nowMs >= minTtlMs)
      .toVector
      .distinct
      .minByOption(e => (math.abs(e - target), -e))

  /** 一天的毫秒数 */
  val DayMs: Long = 86_400_000L

  /** 选宽跨两腿：同一到期内，
    *   - call 取 `>= spot·(1+minDistance)` 的**最小**行权
    *   - put  取 `<= spot·(1−minDistance)` 的**最大**行权
    *
    * 即"满足距离条件里最近的一档"，最近一档不够远就自动往外挪一档。两腿因此恒离现价至少
    * `minDistance`，减少现价长期贴着行权价导致的频繁对冲。
    *
    * 某侧无满足条件的行权价 -> 该侧 None (**单腿裸卖**，调用方须告警)。
    */
  def strangleLegs(
      chain: Seq[OptionInstrument],
      expiryMs: Long,
      spot: Double,
      minDistance: Double,
  ): (Option[OptionInstrument], Option[OptionInstrument]) =
    val atExpiry = chain.filter(_.expiryMs == expiryMs)
    val callFloor = spot * (1.0 + minDistance)
    val putCeil = spot * (1.0 - minDistance)
    val call = atExpiry.filter(i => i.right == OptionRight.Call && i.strike >= callFloor).minByOption(_.strike)
    val put = atExpiry.filter(i => i.right == OptionRight.Put && i.strike <= putCeil).maxByOption(_.strike)
    (call, put)

  /** **单腿**被跳过的原因 —— 带上原因才能对"连续多轮想卖却卖不掉"告警。
    * 杠杆闸门不在这里：它是账户级的、在任何单腿决策之前就短路整轮，放进来会让调用方以为
    * 它是逐腿判定的。 */
  enum Skip:
    /** 目标张数已满足 (含目标为 0) */
    case AtTarget(target: Double, held: Double)
    /** 权利金太薄：卖出所得盖不住手续费与 gamma 风险 */
    case PremiumTooLow(bid: Double, min: Double)
    /** 盘口太宽：流动性差时贱卖 */
    case SpreadTooWide(ratio: Double, max: Double)
    /** 补差量不足最小下单量 */
    case BelowMinQty(qty: Double, minQty: Double)

  /** 一条待下单的卖出腿。价格取 **bid** 并走 IOC —— 主动吃买盘，保证成交、不赚点差；
    * 未成交部分立即撤销，不留挂单，下一轮用新盘口重试。bid 才是 IOC 卖出的实收价，
    * 所以权利金闸门也按它判。 */
  final case class SellLeg(symbol: String, contracts: Double, price: Double, target: Double, held: Double, iv: Double)

  /** 单腿决策：目标 (由 IV 定) 与当前空头张数比，差多少补多少，再过两道盘口闸门。
    *
    * 只看**空头**张数：这条策略只卖，多头张数 (若有，来自别处) 不该被当成"已经卖过了"而抵扣目标。
    */
  def planLeg(
      inst: OptionInstrument,
      iv: Double,
      held: Seq[OptionHolding],
      cfg: IvQty,
      quote: Quote,
      minPremium: Double,
      maxSpreadRatio: Double,
  ): Either[Skip, SellLeg] =
    val shortHeld = held.iterator.filter(_.symbol == inst.symbol).map(h => math.max(0.0, -h.contracts)).sum
    val target = targetContracts(iv, cfg)
    val gap = target - shortHeld
    for
      _ <- Either.cond(gap > 0.0, (), Skip.AtTarget(target, shortHeld))
      // 权利金闸门排在点差之前: 权利金太薄与盘口质量无关, 是这笔交易本身不值得做
      _ <- Either.cond(quote.bid >= minPremium, (), Skip.PremiumTooLow(quote.bid, minPremium))
      ratio = quote.ask / quote.bid // bid > 0 由 Quote 的不变量保证
      _ <- Either.cond(ratio <= maxSpreadRatio, (), Skip.SpreadTooWide(ratio, maxSpreadRatio))
      qty <- OptionQty.alignDown(gap, inst.qtyStep, inst.minQty).toRight(Skip.BelowMinQty(gap, inst.minQty))
    yield SellLeg(inst.symbol, qty, quote.bid, target, shortHeld, iv)

  /** 期权杠杆率 = 所有期权持仓的名义价值之和 / 账户净值。
    *
    * 名义价值按**绝对张数**计 (多空都贡献总名义)，比净敞口口径更严 —— 一个 delta 中性但两腿
    * 都很大的组合，风险不是 0。净值 <= 0 时返回 +∞ (禁止卖出)，而不是让除法给出一个荒谬的负数。
    */
  def optionLeverage(holdings: Seq[OptionHolding], chain: Seq[OptionInstrument], spot: Double, equity: Double): Double =
    if equity <= 0.0 then Double.PositiveInfinity
    else
      val bySymbol = chain.iterator.map(i => i.symbol -> i).toMap
      val notional = holdings.iterator
        .flatMap(h => bySymbol.get(h.symbol).map(i => math.abs(i.toCoin(h.contracts)) * spot))
        .sum
      notional / equity

  /** 剩余期限的年化值 (供诊断日志：卖的到底是几天的期权) */
  def ttlYears(expiryMs: Long, nowMs: Timestamp): Double =
    (expiryMs - nowMs).toDouble / BlackScholes.MillisPerYear
