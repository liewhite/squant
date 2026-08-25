package strategy.strategies.ivsellhedge.live

import strategy.strategies.ivsellhedge.logic.{OptionExposure, OptionExposureTopic, PortfolioDelta, SellPlan}
import strategy.utils.option.*

import hft.actor.{Actor, ActorContext}
import hft.domain.{Coin, Exchange, Price, Timestamp, nowMs}
import hft.event.Event
import org.slf4j.LoggerFactory

/** **期权卖方 actor** —— 一个组件干两件事：按 IV 卖出宽跨、按秒发布账户实时敞口。
  *
  * ## 为什么是 actor 而不是策略
  *
  * 期权下单**完全旁路框架的下单通道**，这是被架构钉死的：框架 OKX 适配层的 symbol 是基础币
  * (`ETH` -> `ETH-USDT-SWAP`)，表达不了同时持有的几十个期权 instId；`AccountOutcome` 的构造器
  * 又是 `private[hft]`，策略包里根本拼不出一条下单意图。所以期权腿只能自己走 REST。
  *
  * 代价如实记下：这条腿**没有** pending 登记、没有 Fill 回流、不进 `StateManager`、不进
  * `PerformanceTracker`。可以接受，因为卖出走 IOC (不留挂单)，而"该有多少空头张数"每轮都从
  * 交易所实际持仓重新推导 —— 没有需要跟踪的中间状态，也就没有可发散的状态。
  *
  * ## 两个 fork，各自自洽
  *
  *   - **敞口 fork** ([[publishExposureMs]], 默认 1s)：每拍取一次标的最新价重算 delta 并发布；
  *     IV / 持仓 / 现金这些慢变量每 [[refreshMarksMs]] (默认 5s) 刷一次。delta 主要随标的价变，
  *     所以新鲜度该由价格的新鲜度决定，而不是被最慢的那个读数拖着。
  *   - **卖出 fork** ([[sellIntervalMs]], 默认 5s)：声明式对账。
  *
  * 两个 fork **不共享任何可变状态** —— 各自轮询自己要的数据。多花几个 REST 调用 (远在限频之内)
  * 换掉一整类跨线程读写问题：actor 的事件循环与 fork 本就在不同线程上，共享缓存就得上锁，
  * 而"忘了加锁"这种错误在这里表现为 delta 偶尔读到半个快照。
  *
  * ## 重复卖出的防线
  *
  * 卖出走 IOC，所以没有挂单可跟；下一轮的判据是**已结算持仓**。持仓端点滞后超过一个轮询间隔
  * 时，同一个缺口会被连卖两轮，而卖出侧**从不主动平仓** —— 超卖会永久留在账上。所以提交过
  * 卖单之后强制静默 `settleRounds` 轮（见 [[Config.settleRounds]]），把这条不变量从注释变成
  * 代码。轮询间隔本身仍应显著大于结算延迟。
  */
final class OptionSellerActor(
    ex: OptionsExchange & OptionAccountData,
    exchange: Exchange,
    cfg: OptionSellerActor.Config,
) extends Actor:
  import OptionSellerActor.*

  private val logger = LoggerFactory.getLogger(classOf[OptionSellerActor])

  override def name: String = s"option-seller-${cfg.baseCoin}"

  override def onStart(ctx: ActorContext): Unit =
    logger.warn(
      s"期权卖方启动: ${cfg.baseCoin} 目标到期≈${cfg.targetDays}天 行权距离≥${cfg.minStrikeDistance * 100}% " +
        s"IV定量(起卖${cfg.ivQty.ivStart} 起量${cfg.ivQty.qtyStart} 斜率${cfg.ivQty.qtySlope}/点 上限${cfg.ivQty.qtyMax}) " +
        s"杠杆上限=${cfg.maxOptionLeverage} 敞口发布=${cfg.publishExposureMs}ms 卖出轮询=${cfg.sellIntervalMs}ms " +
        (if cfg.enableOpen then "*** 实盘卖出 ***" else "(enableOpen=false, 只打印意图不下单)")
    )
    ctx.fork(exposureLoop(ctx))
    ctx.fork(sellLoop(ctx))

  // ==================== 敞口 fork：每秒重算并发布 ====================

  private def exposureLoop(ctx: ActorContext): Unit =
    var slow: Option[SlowSnapshot] = None
    var fails = 0
    /** 连续多少轮没能发出一份可信的敞口 —— 只在真正发布成功时归零 */
    def failed(what: String, err: String): Unit =
      fails += 1
      if fails >= FailEscalateAfter then
        logger.error(s"!!! $what 已连续 $fails 轮失败, 敞口读数停更 -> 对冲将因读数陈旧而暂停, 期权处于裸敞口, 请人工介入: $err")
      else logger.error(s"$what 失败 (本轮不发布, 将重试): $err")

    while !ctx.sleepUnlessStopped(cfg.publishExposureMs) do
      if slow.forall(s => nowMs - s.at >= cfg.refreshMarksMs) then
        fetchSlow() match
          case Right(s) => slow = Some(s)
          case Left(e)  => failed("IV/持仓/现金 刷新", e)
      slow match
        case None => () // 首轮还没拿到慢变量, 失败已在上面记过
        case Some(s) if nowMs - s.at > maxSlowAgeMs =>
          // 慢变量过期就**停止发布**, 而不是拿它配一个新价格。IV 慢变、不差这几秒, 但**持仓**
          // 会因为卖出而变 —— 用过期持仓配新价算出的 delta 会漏掉刚卖出的腿, 而它算得出数、
          // 数是错的, 症状只是"对冲总差一点"。
          failed(s"慢变量已过期 ${nowMs - s.at}ms (> ${maxSlowAgeMs}ms)", "等待下一次刷新成功")
        case Some(s) =>
          val resolved = PortfolioDelta.resolve(s.holdings, s.chain, s.marks)
          if !resolved.complete then
            // 有持仓却配不上合约定义或标记 IV -> 这一轮的 delta 不完整。半个敞口比没有敞口更危险:
            // 对冲会照着一个偏小的数去做, 而症状只是"对冲总差一点"。
            failed(s"${resolved.unpriceable.size} 个持仓缺合约定义或标记 IV", resolved.unpriceable.mkString(","))
          else
            ex.underlyingLast(cfg.symbol) match
              case Left(e) => failed("标的最新价", e)
              case Right(spot) =>
                // 时间戳盖在**读数到手之后**: 盖在发起请求之前会把整段 REST 往返算进事件年龄,
                // 把"对端慢"误诊成"读数陈旧"。它同时是 BS 的估值时刻, 两处必须是同一个时刻。
                val at = nowMs
                fails = 0
                val (delta, gamma) = PortfolioDelta.greeks(resolved.legs, spot, at, cfg.riskFreeRate)
                // Event.local: 这条读数是 REST 派生的, 没有交易所时间戳可言 (框架对这类事件的约定)
                ctx.publish(
                  Event.local(
                    OptionExposureTopic,
                    OptionExposure(exchange, cfg.ccy, delta, gamma, Coin(s.cash.coinBalance), Price(spot), resolved.legs.size, at),
                  )
                )

  /** 慢变量的可用年龄上限 = 4× 刷新间隔 (与对冲侧的陈旧闸门同一形状: 连续几次刷新失败即停更) */
  private val maxSlowAgeMs: Long = cfg.refreshMarksMs * 4

  /** 慢变量的一次性取数 —— 四份数据必须来自同一轮，缺任一份就整轮作废。
    * 分别缓存会让 delta 用上一轮的持仓配这一轮的 IV，而那种不一致算得出数、数是错的。 */
  private def fetchSlow(): Either[String, SlowSnapshot] =
    for
      chain <- ex.optionChain(cfg.baseCoin)
      marks <- ex.optionMarks(cfg.baseCoin)
      holdings <- ex.optionPositions(cfg.baseCoin)
      cash <- ex.accountCash(cfg.ccy)
    yield SlowSnapshot(chain, marks, holdings, cash, at = nowMs)

  // ==================== 卖出 fork：声明式对账 ====================

  private def sellLoop(ctx: ActorContext): Unit =
    var lastSubmitAt = 0L // 只被本 fork 读写
    while !ctx.sleepUnlessStopped(cfg.sellIntervalMs) do
      reconcileOnce(lastSubmitAt) match
        case Left(e)             => logger.error(s"本轮卖出跳过: $e")
        case Right(Some(at))     => lastSubmitAt = at
        case Right(None)         => ()

  /** **结算静默期** = settleRounds × 卖出轮询间隔。
    *
    * 提交过卖单之后要等这么久才做下一轮对账。这条防线不能只写在文档里：本策略防重复卖出的
    * 唯一依据是"已结算持仓"，而持仓端点滞后超过一个轮询间隔时，同一个缺口会被连卖两轮 ——
    * 卖出侧**从不主动平仓**，超卖会永久留在账上。等一轮把结算预算翻倍，并且把这个不变量
    * 从注释变成代码。
    */
  private val settleGraceMs: Long = cfg.settleRounds * cfg.sellIntervalMs

  /** 一轮对账。
    *
    * @param lastSubmitAt 上次提交卖单的时刻 (0 = 从未)
    * @return Left = 这一轮的输入不足以决策 (取数失败 / 无合适到期)；
    *         Right(Some(t)) = 在 t 提交过卖单；Right(None) = 本轮未提交
    */
  private[live] def reconcileOnce(lastSubmitAt: Timestamp): Either[String, Option[Timestamp]] =
    val sinceSubmit = nowMs - lastSubmitAt
    if lastSubmitAt > 0 && sinceSubmit < settleGraceMs then
      logger.info(s"距上次提交仅 ${sinceSubmit}ms (< ${settleGraceMs}ms 结算静默期) -> 本轮不对账, 等持仓落地")
      Right(None)
    else reconcile()

  private def reconcile(): Either[String, Option[Timestamp]] =
    for
      chain <- ex.optionChain(cfg.baseCoin)
      marks <- ex.optionMarks(cfg.baseCoin)
      holdings <- ex.optionPositions(cfg.baseCoin)
      cash <- ex.accountCash(cfg.ccy)
      spot <- ex.underlyingLast(cfg.symbol)
      now = nowMs
      expiry <- SellPlan
        .selectExpiry(chain, now, cfg.targetDays, cfg.minTtlMs)
        .toRight(s"无剩余期限 >= ${cfg.minTtlMs / SellPlan.DayMs} 天的挂牌到期, 不卖")
    yield
      val resolved = PortfolioDelta.resolve(holdings, chain, marks)
      val leverage = SellPlan.optionLeverage(holdings, chain, spot, cash.equity)
      val (call, put) = SellPlan.strangleLegs(chain, expiry, spot, cfg.minStrikeDistance)
      if call.isEmpty || put.isEmpty then
        logger.warn(
          s"!!! 宽跨不完整 (call=${call.map(_.symbol).getOrElse("无")} put=${put.map(_.symbol).getOrElse("无")}): " +
            s"现价 $spot 距离要求 ${cfg.minStrikeDistance} 之外无挂牌行权价 -> 该侧不卖, 存在单腿裸方向敞口"
        )
      val driftDays = math.abs(expiry - now - cfg.targetDays.toLong * SellPlan.DayMs).toDouble / SellPlan.DayMs
      if driftDays > ExpiryDriftWarnDays then
        logger.warn(f"选中到期偏离目标 $driftDays%.1f 天 (>$ExpiryDriftWarnDays) —— 挂牌稀疏或 targetDays 配错的早期信号")
      logger.info(
        f"对账: 现价=$spot%.2f 到期=${java.time.Instant.ofEpochMilli(expiry)} 净值=${cash.equity}%.2f " +
          f"期权杠杆=$leverage%.3f/${cfg.maxOptionLeverage}%.3f 现货余额=${cash.coinBalance}%.4f"
      )
      if !resolved.complete then
        // 有持仓配不上期权链 -> 杠杆率被低估 (那些腿的名义算不进去), 闸门就形同放宽。
        // 敞口侧已经因此停发, 卖出侧没有理由继续放行。
        logger.error(
          s"!!! ${resolved.unpriceable.size} 个持仓缺合约定义或标记 IV -> 杠杆率被低估, 本轮不卖: " +
            resolved.unpriceable.mkString(",")
        )
        None
      else if leverage >= cfg.maxOptionLeverage then
        logger.warn(f"期权杠杆 $leverage%.3f 已达上限 ${cfg.maxOptionLeverage}%.3f -> 停止卖出新期权 (对冲照常运行)")
        None
      else
        val submitted = Seq(call, put).flatten.map(inst => sellLeg(inst, marks, holdings)).count(identity)
        if submitted > 0 then Some(nowMs) else None

  /** @return 是否真的向交易所提交了订单 (决定要不要进入结算静默期) */
  private def sellLeg(inst: OptionInstrument, marks: Seq[OptionMark], holdings: Seq[OptionHolding]): Boolean =
    val ivOpt = marks.iterator.find(_.symbol == inst.symbol).map(_.markVol)
    val quoteOpt = ex.optionQuote(inst.symbol)
    (ivOpt, quoteOpt) match
      case (None, _) =>
        // IV 缺失不回退到"基础张数": 目标张数与 IV 正相关是这条策略的全部定量依据, 少了它就没有目标
        logger.warn(s"${inst.symbol} 无标记 IV -> 本轮不卖 (IV 是定量的唯一依据, 不猜)")
        false
      case (_, Left(e)) => logger.error(s"${inst.symbol} 取盘口失败 -> 本轮不卖: $e"); false
      case (_, Right(None)) => logger.warn(s"${inst.symbol} 无两边盘口报价 -> 本轮不卖 (无法定价)"); false
      case (Some(iv), Right(Some(quote))) =>
        SellPlan.planLeg(inst, iv, holdings, cfg.ivQty, quote, cfg.minPremium, cfg.maxSpreadRatio) match
          case Left(skip) => logSkip(inst, iv, skip); false
          case Right(leg) =>
            logger.warn(
              f"卖出意图: ${leg.symbol} 补 ${leg.contracts}%.4f 张 (目标${leg.target}%.0f 已持${leg.held}%.0f) " +
                f"@bid=${leg.price} IV=${iv * 100}%.1f%% IOC"
            )
            if !cfg.enableOpen then
              logger.warn("enableOpen=false -> 不下单")
              false
            else
              // postOnly=false -> IOC: 以买一价主动吃单, 保证成交、不留挂单; 未成交部分立即撤销
              ex.sellOption(leg.symbol, leg.contracts, leg.price, postOnly = false, orderLinkId = clOrdIdOf(leg.symbol)) match
                case Right(id) =>
                  logger.warn(s"已提交 ${leg.symbol} ${leg.contracts}张 @${leg.price} -> ordId=$id (IOC, 实际成交看下轮持仓)")
                  true
                case Left(e) =>
                  // 被拒的单没有成立, 不进入结算静默期 (下一轮该重试)
                  logger.error(s"!!! ${leg.symbol} 卖出被拒: $e")
                  false

  private def logSkip(inst: OptionInstrument, iv: Double, skip: SellPlan.Skip): Unit = skip match
    case SellPlan.Skip.AtTarget(t, held) =>
      logger.info(f"${inst.symbol} 已达目标 (目标$t%.0f 已持$held%.0f, IV=${iv * 100}%.1f%%) -> 不卖")
    case SellPlan.Skip.PremiumTooLow(bid, min) =>
      logger.warn(f"${inst.symbol} 权利金 $bid 低于门槛 $min -> 不卖 (卖出所得盖不住手续费与 gamma 风险)")
    case SellPlan.Skip.SpreadTooWide(r, max) =>
      logger.warn(f"${inst.symbol} 盘口比 $r%.3f 超上限 $max%.3f -> 不卖 (流动性差, 避免贱卖)")
    case SellPlan.Skip.BelowMinQty(q, minQty) =>
      logger.info(f"${inst.symbol} 补差 $q%.4f 不足最小下单量 $minQty -> 不卖 (下轮继续攒)")

  /** clOrdId 只取 instId 的字母数字部分 —— 这里**不追求幂等**：卖出走 IOC，重复提交的防线是
    * 声明式对账 (下一轮按已结算持仓重算缺口)，不是订单 id。硬塞一个"幂等" id 反而会在真正需要
    * 补卖时被交易所以重复单拒掉。 */
  private def clOrdIdOf(instId: String): String = instId.filter(_.isLetterOrDigit).takeRight(28) + "s"

object OptionSellerActor:
  /** 连续失败多少次升级为"请人工介入"级告警 */
  val FailEscalateAfter: Int = 3

  /** 选中到期与目标相差超过这么多天就告警 (挂牌稀疏或参数配错的早期信号) */
  val ExpiryDriftWarnDays: Double = 3.0

  /** 慢变量的一份快照 (同一轮取数)。`at` = 取数完成时刻, 用来判它还能不能用。 */
  private final case class SlowSnapshot(
      chain: Vector[OptionInstrument],
      marks: Vector[OptionMark],
      holdings: Vector[OptionHolding],
      cash: OptionAccountCash,
      at: Timestamp,
  )

  /** 卖方 actor 的全部调参。
    *
    * @param symbol            标的 symbol (OKX: 基础币 ETH, 内部拼 ETH-quote-SWAP 取永续价)
    * @param baseCoin          期权基础币 (取期权链 instFamily=`<baseCoin>-USD`)
    * @param ccy               现货余额与敞口读数的币种 (币本位期权即基础币)
    * @param targetDays        目标到期天数 (相对**今天**的滑动量, 不是周五这类绝对锚点)
    * @param minTtlMs          剩余期限低于它的到期一律排除 (当日到期权利金≈0 而 gamma 极大)
    * @param minStrikeDistance 两腿行权价距现价的最小比例 (0.02 = 2%)
    * @param ivQty             IV 定量参数 (见 [[SellPlan.IvQty]])
    * @param minPremium        权利金门槛 (按 bid 判, 那才是 IOC 卖出的实收价)
    * @param maxSpreadRatio    ask/bid 上限, 超过即流动性太差不卖
    * @param maxOptionLeverage 期权杠杆率上限 (Σ|张数|×ctVal×现价 / 净值), 达到即停止卖出
    * @param settleRounds      提交过卖单后强制静默的轮数 (给持仓落地留时间, 防同一缺口被连卖两轮)
    * @param enableOpen        false = 只打印卖出意图不下单
    * @param riskFreeRate      BS 定价的无风险利率 (默认 0, 见 [[PortfolioDelta.DefaultRate]])
    */
  final case class Config(
      symbol: String,
      baseCoin: String,
      ccy: String,
      targetDays: Int = 3,
      minTtlMs: Long = SellPlan.DayMs,
      minStrikeDistance: Double = 0.02,
      ivQty: SellPlan.IvQty,
      minPremium: Double = 0.0,
      maxSpreadRatio: Double = 1.1,
      maxOptionLeverage: Double = 1.0,
      enableOpen: Boolean = false,
      publishExposureMs: Long = 1000,
      refreshMarksMs: Long = 5000,
      sellIntervalMs: Long = 5000,
      settleRounds: Int = 1,
      riskFreeRate: Double = PortfolioDelta.DefaultRate,
  ):
    def validated: Config =
      require(targetDays >= 1, s"targetDays 须 >= 1, 实为 $targetDays")
      require(minStrikeDistance >= 0.0, s"minStrikeDistance 须 >= 0, 实为 $minStrikeDistance")
      require(maxSpreadRatio >= 1.0, s"maxSpreadRatio 须 >= 1, 实为 $maxSpreadRatio")
      require(maxOptionLeverage > 0.0, s"maxOptionLeverage 须 > 0, 实为 $maxOptionLeverage")
      require(publishExposureMs > 0 && refreshMarksMs > 0 && sellIntervalMs > 0, "轮询间隔须 > 0")
      require(settleRounds >= 1, s"settleRounds 须 >= 1 (提交后至少静默一轮), 实为 $settleRounds")
      ivQty.validated
      this
