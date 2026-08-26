package strategy.strategies.ivsellhedge.logic

import strategy.utils.hedge.{DeltaBand, DeltaCtx, QuoteLeg, QuotePolicy, QuoteStyle}

import hft.domain.*
import hft.event.{AnyEvent, Topics}
import hft.indicator.{Kama, KlineSeries, Macd}
import hft.strategy.{Strategy, StrategyContext, StrategyHandlers}

/** **敞口轴的 delta 中性对冲** —— 判据是**真实净敞口**，MACD 与效率比只调阈值。
  *
  * ## 一条根本规则
  *
  * {{{
  *   真实净敞口 = 期权 delta + 该币现金余额 + 永续净持仓
  *   |真实净敞口| 超过该侧阈值  ->  按真实净敞口全平到 0
  * }}}
  *
  * 就这一条。两个信号 (MACD 方向、效率比 ER) 都只是阈值的乘数，**不替换被测量的量**
  * —— 见 [[DeltaBand]] 里对这条分工的说明：让平滑值替换真实敞口去过阈值，会让这条规则被架空
  * (边抖边涨的行情里敞口可以累积一两个小时而不触发)，然后就得再加一道绝对值硬闸门把上界找回来
  * ——那是给自己制造的问题打补丁。
  *
  * 现在阈值最宽也只有 `基准 × chopWiden`，所以**真实敞口有确定上界**，只有一个判据。
  *
  * ## 两个信号
  *
  *   - **效率比 ER** (1 分钟价格 K 线上的 [[Kama]] 给出)：震荡 (ER→0) 放宽阈值 -> 少对冲；
  *     趋势 (ER→1) 收紧阈值 -> 尽快跟上。这正是"KAMA 仅用来在震荡时减少对冲次数、趋势时
  *     尽快跟上"的落地方式。
  *   - **MACD 方向** (1 小时价格 K 线)：逆势侧阈值减半。
  *
  * 两条 K 线都由永续中间价逐笔聚合，且都能用**历史 K 线预热** ([[prewarmKama]] / [[prewarmMacd]])
  * —— 开机即就绪。ER 还有盘中形态 (见 [[Kama]])，逐笔就有值，不存在"一根 bar 内指标冻结"的盲区。
  *
  * ER 预热不足时体制系数取 1.0 (用基准阈值)。这里可以安全地"不猜"，正因为判据是真实敞口 ——
  * 猜错只影响对冲的疏密，不会让敞口失去上界。
  *
  * ## 执行
  *
  * 被动挂单 ([[MakerQuoteLeg]] 的后继 `QuoteLeg`)，报价方式按 ER 在"被动慢挂 / 跨价追单"之间
  * 切换 (见 [[QuotePolicy]])。onEvent 由框架单线程串行调用，内部可变状态无需同步。
  *
  * @param symbol             永续标的 (OKX 统一 symbol = 基础币, 如 ETH)
  * @param ccy                期权基础币 —— [[OptionExposure]] 按交易所路由, 币种在载荷里, 故自行判别
  * @param band               敞口死区 (阈值如何随信号缩放, 见 [[DeltaBand.adaptive]])
  * @param quotes             报价方式的选择 —— 敞口平缓时被动慢挂、走单边时跨价追单,
  *                           见 [[QuotePolicy.byEfficiency]]
  * @param kamaBarMs          ER 的 K 线粒度 (默认 1 分钟)
  * @param macdBarMs          MACD 的 K 线粒度 (默认 1 小时)
  * @param maxExposureStaleMs 敞口读数陈旧阈值 (ms): 超过它暂停对冲。>0 才生效
  */
final class DeltaHedgeStrategy(
    exchange: Exchange,
    symbol: Symbol,
    ccy: String,
    band: DeltaBand,
    kamaBarMs: Long = 60_000L,
    kamaErBars: Int = 10,
    kamaFastBars: Int = 2,
    kamaSlowBars: Int = 30,
    macdBarMs: Long = 3_600_000L,
    macdFastPeriod: Int = 12,
    macdSlowPeriod: Int = 26,
    macdSignal: Int = 9,
    quotes: QuotePolicy,
    cancelConfirmMs: Long = 3000,
    minHedgeQty: Coin = Coin(0.001),
    maxHedgeQty: Coin = Coin(Double.MaxValue),
    maxExposureStaleMs: Long = 0L,
) extends Strategy:
  private val logger = org.slf4j.LoggerFactory.getLogger(classOf[DeltaHedgeStrategy])
  /** **按类别**分别节流：共用一个计数器的话，一条高频告警会把另一条低频但更重要的
    * (如"敞口陈旧") 淹没到 1/200 采样，而那条恰恰是需要立刻看到的。 */
  private val warnCounts = scala.collection.mutable.Map.empty[String, Long]
  private def warnThrottled(kind: String, msg: String): Unit =
    val n = warnCounts.getOrElse(kind, 0L)
    if n % DeltaHedgeStrategy.WarnEvery == 0 then logger.warn(s"[DeltaKamaHedge $symbol] $msg")
    warnCounts(kind) = n + 1

  /** MACD 的 K 线 (粗粒度趋势过滤), 由永续中间价逐笔聚合 */
  private val macdKlines =
    new KlineSeries(macdBarMs, math.max(macdSlowPeriod + macdSignal + 8, 64)) with Macd:
      override protected def macdFast: Int = macdFastPeriod
      override protected def macdSlow: Int = macdSlowPeriod
      override protected def macdSignalPeriod: Int = macdSignal

  /** 效率比 ER 的 K 线 (细粒度), 同样由中间价逐笔聚合。只用它的 [[Kama.efficiencyRatio]] ——
    * KAMA 的**值**不参与判据 (那正是之前用错的地方)。 */
  private val kamaKlines =
    new KlineSeries(kamaBarMs, math.max(kamaErBars * 4, 64)) with Kama:
      override protected def kamaErPeriod: Int = kamaErBars
      override protected def kamaFast: Int = kamaFastBars
      override protected def kamaSlow: Int = kamaSlowBars

  private var exposure: Option[OptionExposure] = None
  private val leg = QuoteLeg(cancelConfirmMs)

  /** MACD 的启动预热 (历史 `macdBarMs` 粒度 K 线, 最旧->最新)。不预热的话开机后数十根 bar 内
    * 方向恒为 0, 死区退化为对称 —— 那条规则在最需要它的启动期缺席。 */
  def prewarmMacd(bars: Seq[(Double, Double, Double)]): Unit = feed(macdKlines, bars, macdBarMs)

  /** ER 的启动预热 (历史 `kamaBarMs` 粒度 K 线, 最旧->最新)。
    *
    * 这是把它建在**标的价**上换来的：价格历史交易所有，敞口历史没有。开机即就绪。 */
  def prewarmKama(bars: Seq[(Double, Double, Double)]): Unit = feed(kamaKlines, bars, kamaBarMs)

  private def feed(series: KlineSeries, bars: Seq[(Double, Double, Double)], barMs: Long): Unit =
    bars.zipWithIndex.foreach { case ((h, l, c), i) =>
      val t = i.toLong * barMs
      series.update(t, h); series.update(t, l); series.update(t, c)
    }

  // 订单超时需 > requote, 否则框架会先把正常挂单当超时清理
  override def orderTimeoutMs: Long = quotes.maxTtlMs * 3

  override def handlers: StrategyHandlers = StrategyHandlers.empty
    .own(Topics.OrderUpdate) { (u, _, _) =>
      leg.onOrderUpdate(u) // 成交价对敞口轴判据没有意义 (判据是敞口本身), 故不用它重置任何东西
      Vector.empty
    }
    .market(Topics.Bbo, Instrument(exchange, symbol)) { (b, ctx, now) =>
      macdKlines.update(b.timestamp, b.midPrice.value)
      kamaKlines.update(b.timestamp, b.midPrice.value) // ER 逐笔即时更新, 无"一根 bar 内冻结"的盲区
      manage(b, now, ctx)
    }
    // 敞口读数按交易所路由, 币种在载荷里 -> 自行判别
    .custom(OptionExposureTopic, Set(exchange)) { (e, ctx, now) =>
      if e.ccy != ccy then Vector.empty
      else
        exposure = Some(e)
        ctx.state.symbolState(symbol).flatMap(_.bbo(exchange)).map(manage(_, now, ctx)).getOrElse {
          warnThrottled("盘口未就绪", "盘口未就绪 -> 本次敞口更新不对冲 (检查永续 BBO 订阅)")
          Vector.empty
        }
    }

  /** 两个时钟域，各有各的理由：
    *   - `bbo.timestamp` 是**交易所时钟**，用于挂单年龄 (requote) —— 挂单确认的时刻也来自
    *     交易所回报，两边必须同一个时钟，否则撤单时机随投递延迟与时钟偏斜漂移。
    *   - `localNow` 是**本地处理时刻**，用于敞口读数的陈旧判定 —— 那条读数是 REST 派生的，
    *     时间戳盖的就是本地墙钟，拿交易所时钟去减就是在测量两地的时钟偏斜。
    */
  private def manage(bbo: BBO, localNow: Timestamp, ctx: StrategyContext): Vector[AnyEvent] =
    val style = quotes.styleFor(kamaKlines.efficiencyRatio)
    leg.step(bbo.timestamp, style) match
      case QuoteLeg.Step.Blocked => Vector.empty
      case QuoteLeg.Step.Requote(ref, why) =>
        logRequote(why)
        Vector(ctx.cancel(exchange, symbol, ref))
      case QuoteLeg.Step.Ready => tryHedge(bbo, localNow, style, ctx)

  private def logRequote(why: QuoteLeg.Requote): Unit = why match
    case QuoteLeg.Requote.Expired(_) => () // 正常节奏, 不刷日志
    case QuoteLeg.Requote.Preempted(from, to) =>
      // 体制切换值得看见: 它说明敞口刚从震荡转单边 (或反过来), 执行方式随之改变
      warnThrottled("报价抢占", s"体制切换 ${from.label} -> ${to.label}, 抢在超时前换单")
    case QuoteLeg.Requote.Unconfirmed(waited) =>
      // 期间这条腿不挂新单, 对冲实际停着, 必须看得见
      warnThrottled("撤单重发", s"撤单确认 ${waited}ms 未到, 重发撤单 (期间不挂新单, 对冲暂停)")

  private def tryHedge(bbo: BBO, localNow: Timestamp, style: QuoteStyle, ctx: StrategyContext): Vector[AnyEvent] =
    exposure match
      case None =>
        warnThrottled("敞口未到达", "期权敞口读数未到达 -> 未对冲 (检查 OptionSellerActor 是否在发布)")
        Vector.empty
      case Some(e) if maxExposureStaleMs > 0 && localNow - e.timestamp > maxExposureStaleMs =>
        warnThrottled("敞口陈旧", s"敞口读数陈旧 ${localNow - e.timestamp}ms > ${maxExposureStaleMs}ms -> 暂停对冲 (宁可不动也不按过期 delta 乱挂)")
        Vector.empty
      case Some(e) =>
        (for ss <- ctx.state.symbolState(symbol)
        yield
          val perp = ss.positionSize(exchange)          // 自己的对冲仓位
          val net = e.delta + perp                       // 真实净敞口 —— 判据与下单量的唯一依据
          val macdDir = macdKlines.macdDirection         // 预热不足 = 0 -> 死区对称, 不猜方向
          val er = kamaKlines.efficiencyRatio            // 预热不足 = None -> 体制系数 1.0, 不猜体制
          val (upTh, downTh) = band.bands(DeltaCtx(net, macdDir, er))
          val breached = net > upTh || net < -downTh
          if !breached then Vector.empty
          else
            val qty = net.abs
            if qty < minHedgeQty then Vector.empty
            else if qty > maxHedgeQty then
              warnThrottled("超硬上限", f"对冲量 ${qty.value}%.4f 超硬上限 ${maxHedgeQty.value}%.4f -> 不下单 (疑似 delta 计算 bug, 请查)")
              Vector.empty
            else
              val side = if net > Coin.Zero then Side.Short else Side.Long // 净多->卖, 净空->买
              val (limitPx, tif) = leg.place(style, side, Some(bbo), bbo.midPrice)
              Vector(
                ctx.place(
                  Order("", exchange, symbol, side, OrderType.Limit(limitPx, tif), qty,
                    reduceOnly = false, clientOrderId = ""),
                  f"delta_kama_hedge | $side qty=${qty.value}%.4f ${style.label} limit=${limitPx.value}%.2f " +
                    f"净敞口=${net.value}%.4f 带=(+${upTh.value}%.4f,-${downTh.value}%.4f) " +
                    f"macd=$macdDir er=${er.map(v => f"$v%.2f").getOrElse("预热中")} " +
                    f"期权=${e.optionDelta.value}%.4f 现货=${e.coinBalance.value}%.4f 永续=${perp.value}%.4f",
                )
              )
        ).getOrElse(Vector.empty)

object DeltaHedgeStrategy:
  /** 每类告警每这么多次打一条 (逐 tick 的告警不节流会把日志刷爆) */
  val WarnEvery: Long = 200L
