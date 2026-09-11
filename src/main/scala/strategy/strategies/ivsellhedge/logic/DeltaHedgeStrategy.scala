package strategy.strategies.ivsellhedge.logic

import strategy.utils.hedge.{DeltaBand, DeltaCtx, QuoteLeg, QuotePolicy, QuoteStyle}

import hft.domain.*
import hft.event.{AnyEvent, Topics}
import hft.indicator.{EfficiencyRatio, KlineSeries, Macd, RealizedVol}
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
  * ## 三个输入，各管一段
  *
  * {{{
  *   预测什么        管什么                            用什么
  *   ─────────────────────────────────────────────────────────────────
  *   波动率          阈值的**尺度** (对冲频率 -> 成本)   分钟级实现波动 σ (或期权 IV)
  *   方向            两侧阈值的**不对称** (能不能撑住)   1H MACD 柱的符号
  *   ——              敞口的**硬上界** (风险)             maxTheta, 与预测无关
  * }}}
  *
  * 阈值以"敞口在一个对冲时间尺度内的典型漂移幅度"为单位 (`|gamma| × spot × σ × √τ`)，
  * 漂移推向的那一侧收紧 (尽快跟上)、反侧放到噪声之外 (别被正常波动来回止损)。
  * 完整推导见 [[DeltaBand.volScaled]]。
  *
  * 一个容易搞反的对应关系：空头跨式在价格上涨时 delta 更负、账户偏空、需要**买入**对冲，
  * 所以"预测上涨"时被推向的是**空头敞口侧**。
  *
  * ## 效率比 ER 只管报价方式
  *
  * ER 曾被用来缩放死区阈值，现在退出了：实测它滞后约等于窗口长度 (默认 10 根 1m = 9 分钟)、
  * 在无漂移随机游走里有 17% 的时间被误判成趋势、且对幅度完全盲 (0.1 与 20 美元/分的斜率给
  * 同一个 ER)。作为阈值的**尺度**它无能为力 (无量纲)，作为**方向**判据它不如 MACD。
  * 留在报价方式那一路是因为那里判错的代价小：只是这一单挂法不同，不动敞口上界。
  *
  * ## 执行
  *
  * 被动挂单 ([[QuoteLeg]])，报价方式按 ER 在"被动慢挂 / 跨价追单"之间
  * 切换 (见 [[QuotePolicy]])。onEvent 由框架单线程串行调用，内部可变状态无需同步。
  *
  * @param symbol             永续标的 (OKX 统一 symbol = 基础币, 如 ETH)
  * @param ccy                期权基础币 —— [[OptionExposure]] 按交易所路由, 币种在载荷里, 故自行判别
  * @param band               敞口死区 (阈值如何随信号缩放, 见 [[DeltaBand.adaptive]])
  * @param quotes             报价方式的选择 —— 敞口平缓时被动慢挂、走单边时跨价追单,
  *                           见 [[QuotePolicy.byEfficiency]]
  * @param fastBarMs          细粒度序列的 K 线粒度 (默认 1 分钟), σ 与 ER 都建在它上面
  * @param erPeriodBars       ER 的回看根数 (默认 10)
  * @param rvBars             实现波动的回看根数 (默认 30)
  * @param sigmaSource        σ 取实现波动还是期权 IV (见 [[SigmaSource]])
  * @param macdBarMs          MACD 的 K 线粒度 (默认 1 小时)
  * @param maxExposureStaleMs 敞口读数陈旧阈值 (ms): 超过它暂停对冲。>0 才生效
  * @param history            取历史 K 线的通道 —— `(粒度毫秒, 根数) => 最旧->最新的 (high, low, close)`。
  *                           [[prepare]] 在开跑前用它把两条序列喂热。默认不预热 (回测与单测
  *                           自己喂数据, 见 [[prewarmFast]])
  */
final class DeltaHedgeStrategy(
    exchange: Exchange,
    symbol: Symbol,
    ccy: String,
    band: DeltaBand,
    fastBarMs: Long = 60_000L,
    erPeriodBars: Int = 10,
    rvBars: Int = 30,
    sigmaSource: SigmaSource = SigmaSource.Realized,
    macdBarMs: Long = 3_600_000L,
    macdFastPeriod: Int = 12,
    macdSlowPeriod: Int = 26,
    macdSignal: Int = 9,
    quotes: QuotePolicy,
    cancelConfirmMs: Long = 3000,
    minHedgeQty: Coin = Coin(0.001),
    maxHedgeQty: Coin = Coin(Double.MaxValue),
    maxExposureStaleMs: Long = 0L,
    history: (Long, Int) => Either[String, Seq[(Double, Double, Double)]] =
      (_, _) => Left("没有接预热数据源"),
) extends Strategy:
  /** 本策略交易的标的 —— 状态查询与行情声明的同一个键 */
  private val instrument = Instrument.perp(exchange, symbol)
  private val logger = org.slf4j.LoggerFactory.getLogger(classOf[DeltaHedgeStrategy])
  /** **按类别**分别节流：共用一个计数器的话，一条高频告警会把另一条低频但更重要的
    * (如"敞口陈旧") 淹没到 1/200 采样，而那条恰恰是需要立刻看到的。 */
  private val warnCounts = scala.collection.mutable.Map.empty[String, Long]
  private def warnThrottled(kind: String, msg: String): Unit =
    val n = warnCounts.getOrElse(kind, 0L)
    if n % DeltaHedgeStrategy.WarnEvery == 0 then logger.warn(s"[DeltaHedge $symbol] $msg")
    warnCounts(kind) = n + 1

  /** MACD 的 K 线 (粗粒度趋势过滤), 由永续中间价逐笔聚合 */
  private val macdKlines =
    new KlineSeries(macdBarMs, math.max(macdSlowPeriod + macdSignal + 8, 64)) with Macd:
      override protected def macdFast: Int = macdFastPeriod
      override protected def macdSlow: Int = macdSlowPeriod
      override protected def macdSignalPeriod: Int = macdSignal

  /** 细粒度 (分钟级) 价格序列，由中间价逐笔聚合，供两个读数：
    *   - [[RealizedVol.rvShort]]：年化实现波动 σ —— 死区阈值的**尺度**
    *   - [[EfficiencyRatio.efficiencyRatio]]：ER —— 只用于选**报价方式** (被动慢挂 / 跨价追单)
    *
    * 混 [[EfficiencyRatio]] 而不是 `Kama`：这里只要 ER 这个读数，不要 KAMA 那条均线。
    * 混 Kama 会带进 fast/slow 两个**配了也不生效**的参数 —— 那种"看着能调、其实没用"的旋钮是陷阱。 */
  private val fastKlines =
    new KlineSeries(fastBarMs, math.max(math.max(erPeriodBars, rvBars) * 4, 64))
      with EfficiencyRatio with RealizedVol:
      override protected def erPeriod: Int = erPeriodBars
      override protected def rvShortBars: Int = rvBars
      // 死区只用近端 RV, 不需要基线窗口 (volRatio 用不上) -> 与近端同长, 免得白占内存
      override protected def rvLongBars: Int = rvBars

  private var exposure: Option[OptionExposure] = None
  private val leg = QuoteLeg(cancelConfirmMs)

  /** MACD 的启动预热 (历史 `macdBarMs` 粒度 K 线, 最旧->最新)。不预热的话开机后数十根 bar 内
    * 方向恒为 0, 死区退化为对称 —— 那条规则在最需要它的启动期缺席。 */
  def prewarmMacd(bars: Seq[(Double, Double, Double)]): Unit = feed(macdKlines, bars, macdBarMs)

  /** 细粒度序列的启动预热 (历史 `fastBarMs` 粒度 K 线, 最旧->最新)，一次把 σ 与 ER 都喂热。
    *
    * 这是把两个读数都建在**标的价**上换来的：价格历史交易所有，敞口历史没有。开机即就绪。 */
  def prewarmFast(bars: Seq[(Double, Double, Double)]): Unit = feed(fastKlines, bars, fastBarMs)

  /** 就绪：把两条序列用历史 K 线喂热，**开机即就绪**。
    *
    * 从前这段在启动器里 (拉 K 线 -> 调 prewarmXxx -> 处理失败)，忘了调没有任何症状。
    * 收进来之后它是策略自己的事，见 [[Strategy.prepare]]。
    *
    * 粒度与根数直接问序列自己要 (`periodMs` / `maxBars`)，不再在这里重算一遍 ——
    * 那是同一个事实，写两处迟早对不上：要多少根取决于指标窗口，而窗口就是建序列时给的。
    *
    * 取不到只降级不终止：慢热期的行为是**保守侧**的 (死区对称、σ 取下限 -> 对冲偏频而非偏松)，
    * 为它拒绝启动不划算。但每一条都说清降级后果，否则日志里只剩一句无从判断的失败。
    */
  override def prepare(): Unit =
    // 喂热走 prewarm* —— 与回测/单测同一个入口, "怎么喂"这件事只有一处实现
    warm(macdKlines, prewarmMacd, "MACD", "启动期方向恒为 0, 死区退化为对称")
    warm(fastKlines, prewarmFast, "σ 与 ER", "启动期 σ 取下限 (对冲偏频但安全), ER 按单边处理 (报价更贵)")

  private def warm(series: KlineSeries, feedIn: Seq[(Double, Double, Double)] => Unit, what: String, degraded: String): Unit =
    history(series.periodMs, series.maxBars) match
      case Right(bars) if bars.nonEmpty =>
        feedIn(bars)
        logger.warn(s"[$symbol] 预热 ${bars.size} 根 ${series.periodMs}ms K 线 -> $what 就绪")
      case Right(_) =>
        // 接了数据源但拿回空: 多半是新上市、没有那么长的历史。与"没接"分开说, 否则排障会
        // 被引去查装配而不是查数据。
        logger.warn(s"[$symbol] $what 预热取到空 (这个标的没有那么长的历史?): $degraded")
      case Left(why) =>
        logger.error(s"[$symbol] $what 未预热, 将靠实时 BBO 慢热 —— $why: $degraded")

  private def feed(series: KlineSeries, bars: Seq[(Double, Double, Double)], barMs: Long): Unit =
    bars.zipWithIndex.foreach { case ((h, l, c), i) =>
      val t = i.toLong * barMs
      series.update(t, h); series.update(t, l); series.update(t, c)
    }

  override def handlers: StrategyHandlers = StrategyHandlers.empty
    .own(Topics.OrderUpdate) { (u, _, now) =>
      leg.onOrderUpdate(u, now) // 成交价对敞口轴判据没有意义 (判据是敞口本身), 故不用它重置任何东西
      Vector.empty
    }
    .market(Topics.Bbo, instrument) { (b, ctx, now) =>
      macdKlines.update(b.timestamp, b.midPrice.value)
      fastKlines.update(b.timestamp, b.midPrice.value) // ER 逐笔即时更新, 无"一根 bar 内冻结"的盲区
      manage(b, now, ctx)
    }
    // 敞口读数按交易所路由, 币种在载荷里 -> 自行判别
    .custom(OptionExposureTopic, Set(exchange)) { (e, ctx, now) =>
      if e.ccy != ccy then Vector.empty
      else
        exposure = Some(e)
        ctx.state.instrumentState(instrument).flatMap(_.bbo).map(manage(_, now, ctx)).getOrElse {
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
    val style = quotes.styleFor(fastKlines.efficiencyRatio)
    leg.step(localNow, style) match
      case QuoteLeg.Step.Blocked => Vector.empty
      case QuoteLeg.Step.Requote(ref, why) =>
        logRequote(why)
        Vector(ctx.cancel(instrument, ref))
      case QuoteLeg.Step.Ready => tryHedge(bbo, localNow, style, ctx)

  /** 年化波动率的来源。两个都说得通, 做成可切换以便回测对比：
    *   - 实现波动是**回看**的, 时间尺度与对冲的分钟级节奏吻合;
    *   - 期权 IV 是**前瞻**的 (市场对未来的报价), 但它对应的是期权剩余整个生命的波动。
    */
  private def sigmaOf(e: OptionExposure): Option[Double] = sigmaSource match
    case SigmaSource.Realized     => fastKlines.rvShort
    case SigmaSource.ImpliedVol   => e.markVol
    case SigmaSource.MaxOfBoth    => (fastKlines.rvShort, e.markVol) match
        case (Some(a), Some(b)) => Some(math.max(a, b)) // 两者取大 = 保守 (阈值更宽, 对冲更疏)
        case (a, b)             => a.orElse(b)

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
        (for ss <- ctx.state.instrumentState(instrument)
        yield
          val perp = ss.positionSize          // 自己的对冲仓位
          val net = e.delta + perp                       // 真实净敞口 —— 判据与下单量的唯一依据
          // 方向只取**符号**: 方向判断比强度判断可靠, 用强度还要再拍一组映射, 没有依据
          val driftDir = macdKlines.macdDirection        // 预热不足 = 0 -> 死区对称, 不猜方向
          val sigma = sigmaOf(e)
          val (upTh, downTh) = band.bands(DeltaCtx(net, driftDir, e.optionGamma, e.spot, sigma))
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
              val (limitPx, tif) = leg.place(style, side, bbo)
              ctx.place(
                Order.on(instrument, side, OrderType.Limit(limitPx, tif), qty, reduceOnly = false),
                f"delta_hedge | $side qty=${qty.value}%.4f ${style.label} limit=${limitPx.value}%.2f " +
                  f"净敞口=${net.value}%.4f 带=(+${upTh.value}%.4f,-${downTh.value}%.4f) " +
                  f"方向=$driftDir σ=${sigma.map(v => f"$v%.3f").getOrElse("预热中")} " +
                  f"gamma=${e.optionGamma.value}%.5f er=${fastKlines.efficiencyRatio.map(v => f"$v%.2f").getOrElse("预热中")} " +
                  f"期权=${e.optionDelta.value}%.4f 现货=${e.coinBalance.value}%.4f 永续=${perp.value}%.4f",
              )
        ).getOrElse(Vector.empty)

object DeltaHedgeStrategy:
  /** 每类告警每这么多次打一条 (逐 tick 的告警不节流会把日志刷爆) */
  val WarnEvery: Long = 200L

/** 年化波动率的来源 */
enum SigmaSource:
  /** 分钟级**实现波动** (回看): 时间尺度与对冲的分钟级节奏吻合 */
  case Realized
  /** 期权**标记 IV** (前瞻, 市场对未来的报价): 但它对应期权剩余整个生命的波动 */
  case ImpliedVol
  /** 两者取大 —— 保守 (阈值更宽、对冲更疏), 也免得任一路预热不足时没有值 */
  case MaxOfBoth
