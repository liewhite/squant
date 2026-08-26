package strategy.strategies.ivsellhedge.logic

import strategy.utils.hedge.{DeltaBand, DeltaCtx, QuoteLeg, QuotePolicy, QuoteStyle}

import hft.domain.*
import hft.event.{AnyEvent, Topics}
import hft.indicator.{BucketedKama, KlineSeries, Macd}
import hft.strategy.{Strategy, StrategyContext, StrategyHandlers}

/** **敞口轴的 delta 中性对冲** —— 死区判在 KAMA 平滑后的净敞口上，方向与数量按**真实**敞口。
  *
  * ## 三个量，各管一件事
  *
  *   - **外生敞口 O** = 期权 delta + 现货余额，由 [[OptionExposure]] 按秒送来。这是被行情推着
  *     走的、带噪声的量。
  *   - **对冲仓位 P** = 本策略的永续净持仓 (框架私有流的事实)。这是我们自己的动作。
  *   - **判据信号** = `KAMA(O) + P`。
  *
  * ## 为什么只平滑 O，不平滑 O+P
  *
  * 需要被平滑的是**噪声**，而 P 是我们自己刚做的决定，不是噪声。若把 O+P 整体喂进 KAMA，
  * 每次对冲造成的仓位跳变也会被平滑掉 —— 于是对冲完成后信号仍停在越界值上好几个桶，
  * 死区形同失效 (只剩 [[minHedgeQty]] 在挡)。把 P 以原值加进去，对冲一执行信号就立刻回落，
  * 死区**自动复位**：残留的偏离恰好是 `KAMA(O) − O` 这个滞后量，而死区宽度本来就是为吸收
  * 这个量级的抖动设的。
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
  * @param kamaBucketMs       KAMA 的一步 = 多长时间 (默认 5 分钟)
  * @param macdBarMs          MACD 的 K 线粒度 (默认 1 小时)
  * @param maxExposureStaleMs 敞口读数陈旧阈值 (ms): 超过它暂停对冲。>0 才生效
  */
final class DeltaKamaHedgeStrategy(
    exchange: Exchange,
    symbol: Symbol,
    ccy: String,
    band: DeltaBand,
    kamaBucketMs: Long = 300_000L,
    kamaErPeriod: Int = 10,
    kamaFast: Int = 2,
    kamaSlow: Int = 30,
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

  /** MACD 的 K 线由永续中间价逐笔聚合 (与既有对冲策略同一手法) */
  private val klines =
    new KlineSeries(macdBarMs, math.max(macdSlowPeriod + macdSignal + 8, 64)) with Macd:
      override protected def macdFast: Int = macdFastPeriod
      override protected def macdSlow: Int = macdSlowPeriod
      override protected def macdSignalPeriod: Int = macdSignal

  /** 外生敞口 O 的时间桶 KAMA */
  private val kama = BucketedKama(kamaBucketMs, kamaErPeriod, kamaFast, kamaSlow)

  private var exposure: Option[OptionExposure] = None
  private val leg = QuoteLeg(cancelConfirmMs)

  /** 启动预热: 用历史 (high, low, close) 喂 K 线, 使 MACD 开机即就绪 (否则要等数十根 bar
    * 才有方向, 那期间死区退化为对称)。最旧->最新。 */
  def prewarm(bars: Seq[(Double, Double, Double)]): Unit =
    bars.zipWithIndex.foreach { case ((h, l, c), i) =>
      val t = i.toLong * macdBarMs
      klines.update(t, h); klines.update(t, l); klines.update(t, c)
    }

  // 订单超时需 > requote, 否则框架会先把正常挂单当超时清理
  override def orderTimeoutMs: Long = quotes.maxTtlMs * 3

  override def handlers: StrategyHandlers = StrategyHandlers.empty
    .own(Topics.OrderUpdate) { (u, _, _) =>
      leg.onOrderUpdate(u) // 成交价对敞口轴判据没有意义 (判据是敞口本身), 故不用它重置任何东西
      Vector.empty
    }
    .market(Topics.Bbo, Instrument(exchange, symbol)) { (b, ctx, now) =>
      klines.update(b.timestamp, b.midPrice.value)
      manage(b, now, ctx)
    }
    // 敞口读数按交易所路由, 币种在载荷里 -> 自行判别
    .custom(OptionExposureTopic, Set(exchange)) { (e, ctx, now) =>
      if e.ccy != ccy then Vector.empty
      else
        exposure = Some(e)
        kama.update(e.timestamp, e.delta.value) // 只把**外生**敞口喂进平滑器
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
    val style = quotes.styleFor(kama.efficiencyRatio)
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
          // 判据信号: 平滑外生敞口 + 原值仓位。KAMA 预热不足 -> 回退真实值 (降级但不裸奔)
          val signal = Coin(kama.value.getOrElse(e.delta.value)) + perp
          val macdDir = klines.macdDirection             // 预热不足 = 0 -> 死区对称, 不猜方向
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
                    f"macd=$macdDir kama=${kama.value.map(v => f"$v%.4f").getOrElse("预热中")} " +
                    f"er=${kama.efficiencyRatio.map(v => f"$v%.2f").getOrElse("预热中")} " +
                    f"期权=${e.optionDelta.value}%.4f 现货=${e.coinBalance.value}%.4f 永续=${perp.value}%.4f",
                )
              )
        ).getOrElse(Vector.empty)

object DeltaKamaHedgeStrategy:
  /** 每类告警每这么多次打一条 (逐 tick 的告警不节流会把日志刷爆) */
  val WarnEvery: Long = 200L
