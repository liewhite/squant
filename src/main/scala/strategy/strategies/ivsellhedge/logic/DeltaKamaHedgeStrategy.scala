package strategy.strategies.ivsellhedge.logic

import strategy.utils.hedge.{DeltaBand, DeltaCtx, QuoteLeg, QuotePolicy, QuoteStyle}

import hft.domain.*
import hft.event.{AnyEvent, Topics}
import hft.indicator.{Kama, KlineSeries, Macd}
import hft.strategy.{Strategy, StrategyContext, StrategyHandlers}

/** **敞口轴的 delta 中性对冲** —— 死区判在 KAMA 平滑后的净敞口上，方向与数量按**真实**敞口。
  *
  * ## 三个量，各管一件事
  *
  *   - **外生敞口 O** = 期权 delta + 现货余额，由 [[OptionExposure]] 按秒送来。这是被行情推着
  *     走的、带噪声的量。
  *   - **对冲仓位 P** = 本策略的永续净持仓 (框架私有流的事实)。这是我们自己的动作。
  *   - **判据信号** = 把敞口重算在 `KAMA(标的价)` 上，再加 `P`。
  *
  * ## 平滑的是价，不是敞口
  *
  * 敞口是标的价的函数，所以"平滑敞口"与"把敞口算在平滑价上"到一阶是同一件事。
  * 后者好在三点：
  *
  *   1. **价格历史交易所有，敞口历史没有** —— 于是可以用历史 K 线预热 ([[prewarmKama]])，
  *      开机即就绪。平滑敞口序列则必须现场攒够窗口，重启后头几十分钟指标不可用。
  *   2. **卖出新腿的 delta 跳变不被平滑** —— 那不是噪声，而是一份真实的新敞口，该立刻进判据。
  *      平滑敞口序列会把它和"价格动了"混在一起一起滤掉，于是刚卖出的腿要等好几个窗口才被看见。
  *   3. **`KlineSeries` 现成** —— 收盘固定 + 盘中动态 (见 [[Kama]])，逐笔就有值，
  *      不存在"一根 bar 内指标冻结"的盲区。
  *
  * 具体走 gamma 一阶修正：`delta(S) ≈ delta(S₀) + gamma·(S − S₀)`，S 取 KAMA 价、
  * S₀ 取敞口读数当时的现价。二阶误差是 gamma 的曲率 × 平滑残差，对"要不要对冲"这个判断无关紧要。
  *
  * `P` 与现货余额都以**原值**入信号，不参与平滑：前者是我们自己刚做的决定、不是噪声
  * (于是对冲一执行信号就立刻回落，死区**自动复位**)；后者只在收权利金/交割时变，本来就不抖。
  *
  * 两种行情下的行为正是想要的：
  *   - **震荡**：O 来回折返，ER→0，KAMA 几乎不动、贴在均值上；真实 O 反复越界而信号不越界
  *     -> **少对冲** (每次对冲都是负 gamma 组合的实亏成本)。
  *   - **趋势**：O 单边走，ER→1，KAMA 贴紧真实值 -> **该跟就跟**，滞后小。
  *
  * ## 触发只看 KAMA，数量只看真实敞口
  *
  * 越界判定完全由平滑信号给出，**不**再拿真实敞口做二次确认。触发后按 `O + P` 全平到 0 ——
  * 包括真实敞口此刻已经缩回带内、甚至符号与信号相反的情形：目标始终是"把当下真实的敞口打回
  * 0"，而当下真实敞口就是 `O + P`。量太小的那些由 [[minHedgeQty]] 吸收 (交易所也会拒 0 量单)。
  *
  * ## 冷启动
  *
  * KAMA 需要 `erPeriod+1` 个已结束的桶才有值 (默认 5m×11 ≈ 55 分钟)。这期间**回退到真实敞口**
  * 判越界 —— 对冲得更频繁，但绝不能因为"指标没预热"就放着裸敞口不管。delta 的历史无法从
  * 交易所回补，所以没有预热的办法，只能如实降级。
  *
  * ## 执行
  *
  * 被动挂单 ([[MakerQuoteLeg]])，与价格轴的 `MakerHedgeStrategy` 共用同一份挂单/撤单/防重机制。
  * onEvent 由框架单线程串行调用，内部可变状态无需同步。
  *
  * @param symbol             永续标的 (OKX 统一 symbol = 基础币, 如 ETH)
  * @param ccy                期权基础币 —— [[OptionExposure]] 按交易所路由, 币种在载荷里, 故自行判别
  * @param band               敞口死区 (MACD 顺势侧收紧见 [[DeltaBand.macdTightened]])
  * @param quotes             报价方式的选择 —— 敞口平缓时被动慢挂、走单边时跨价追单,
  *                           见 [[QuotePolicy.byEfficiency]]
  * @param kamaBarMs          KAMA 的 K 线粒度 (默认 1 分钟)
  * @param macdBarMs          MACD 的 K 线粒度 (默认 1 小时)
  * @param maxExposureStaleMs 敞口读数陈旧阈值 (ms): 超过它暂停对冲。>0 才生效
  */
final class DeltaKamaHedgeStrategy(
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
  private val logger = org.slf4j.LoggerFactory.getLogger(classOf[DeltaKamaHedgeStrategy])
  /** **按类别**分别节流：共用一个计数器的话，一条高频告警会把另一条低频但更重要的
    * (如"敞口陈旧") 淹没到 1/200 采样，而那条恰恰是需要立刻看到的。 */
  private val warnCounts = scala.collection.mutable.Map.empty[String, Long]
  private def warnThrottled(kind: String, msg: String): Unit =
    val n = warnCounts.getOrElse(kind, 0L)
    if n % DeltaKamaHedgeStrategy.WarnEvery == 0 then logger.warn(s"[DeltaKamaHedge $symbol] $msg")
    warnCounts(kind) = n + 1

  /** MACD 的 K 线 (粗粒度趋势过滤), 由永续中间价逐笔聚合 */
  private val macdKlines =
    new KlineSeries(macdBarMs, math.max(macdSlowPeriod + macdSignal + 8, 64)) with Macd:
      override protected def macdFast: Int = macdFastPeriod
      override protected def macdSlow: Int = macdSlowPeriod
      override protected def macdSignalPeriod: Int = macdSignal

  /** KAMA 的 K 线 (细粒度, 平滑**标的价**), 同样由中间价逐笔聚合 */
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

  /** KAMA 的启动预热 (历史 `kamaBarMs` 粒度 K 线, 最旧->最新)。
    *
    * 这是把 KAMA 建在**标的价**上换来的：价格历史交易所有，敞口历史没有。开机即就绪，
    * 不再有"头几十分钟指标不可用"的窗口。 */
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
      kamaKlines.update(b.timestamp, b.midPrice.value) // KAMA 平滑的是**价**, 逐笔即时更新
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
          val perp = ss.positionSize(exchange)          // P: 自己的对冲仓位
          val raw = e.delta + perp                       // 真实净敞口 (下单量的唯一依据)
          // 判据信号: 把敞口重算在**平滑后的标的价**上, 再加原值仓位。
          // gamma 一阶修正: delta(S) ≈ delta(S₀) + gamma·(S − S₀), 这里 S=KAMA 价、S₀=读数当时的现价。
          // 平滑作用在**价**而不是敞口上, 于是卖出新腿带来的 delta 跳变**不被平滑** ——
          // 那不是噪声, 而是一份真实的新敞口, 该立刻进判据。
          val smoothedOption = kamaKlines.kama.fold(e.optionDelta) { px =>
            e.optionDelta + e.optionGamma.scaled(px - e.spot.value)
          }
          val signal = smoothedOption + e.coinBalance + perp
          val macdDir = macdKlines.macdDirection         // 预热不足 = 0 -> 死区对称, 不猜方向
          val (upTh, downTh) = band.bands(DeltaCtx(signal, macdDir))
          val breached = signal > upTh || signal < -downTh
          if !breached then Vector.empty
          else
            val qty = raw.abs
            if qty < minHedgeQty then Vector.empty
            else if qty > maxHedgeQty then
              warnThrottled("超硬上限", f"对冲量 ${qty.value}%.4f 超硬上限 ${maxHedgeQty.value}%.4f -> 不下单 (疑似 delta 计算 bug, 请查)")
              Vector.empty
            else
              val side = if raw > Coin.Zero then Side.Short else Side.Long // 净多->卖, 净空->买
              val (limitPx, tif) = leg.place(style, side, Some(bbo), bbo.midPrice)
              Vector(
                ctx.place(
                  Order("", exchange, symbol, side, OrderType.Limit(limitPx, tif), qty,
                    reduceOnly = false, clientOrderId = ""),
                  f"delta_kama_hedge | $side qty=${qty.value}%.4f ${style.label} limit=${limitPx.value}%.2f " +
                    f"raw=${raw.value}%.4f signal=${signal.value}%.4f 带=(+${upTh.value}%.4f,-${downTh.value}%.4f) " +
                    f"macd=$macdDir kamaPx=${kamaKlines.kama.map(v => f"$v%.2f").getOrElse("预热中")} " +
                    f"er=${kamaKlines.efficiencyRatio.map(v => f"$v%.2f").getOrElse("预热中")} " +
                    f"期权=${e.optionDelta.value}%.4f 现货=${e.coinBalance.value}%.4f 永续=${perp.value}%.4f",
                )
              )
        ).getOrElse(Vector.empty)

object DeltaKamaHedgeStrategy:
  /** 每类告警每这么多次打一条 (逐 tick 的告警不节流会把日志刷爆) */
  val WarnEvery: Long = 200L
