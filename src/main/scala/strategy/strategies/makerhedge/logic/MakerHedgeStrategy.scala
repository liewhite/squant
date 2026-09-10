package strategy.strategies.makerhedge.logic
import strategy.utils.hedge.{HedgeBand, HedgeCtx, QuoteLeg, QuoteStyle}

import hft.domain.*
import hft.indicator.{Atr, KlineSeries, Macd, RealizedVol, Sma}
import hft.event.{AnyEvent, Topics}
import hft.strategy.{Strategy, StrategyContext, StrategyHandlers}

/** **Maker (被动挂单) 对冲** —— 用 [[HedgeBand]] 决定**何时**对冲 (市价 take 的被动版), 执行改为
  * 在 **BBO 外** [[offsetPct]] (默认 0.01%) 挂 PostOnly 限价单 (省 taker 费 + 赚价差改善), [[requoteMs]] (默认 5s)
  * 未成交则撤单, 下一 tick 按新价重挂。
  *
  * 机制 (同一时刻最多一张挂单)：
  *   - 越带且净 delta≥minQty 且无挂单 -> 挂被动单 (卖挂 bestAsk·(1+offset)、买挂 bestBid·(1−offset),
  *     即对手盘外 offset, 保证 PostOnly 不吃单; 净多→卖, 净空→买)。
  *   - 已有挂单且年龄 > requoteMs -> 撤单 (下 tick 重挂); 成交 -> 中心移到成交价、清挂单。
  *   - 自管 orderId (来自撮合回流 OrderUpdated): Pending→记 id; Filled→recenter+清; Cancelled/Rejected→清。
  *   - awaitingAck 防"下单到确认之间"重复下单。
  *
  * 负 gamma (卖方) 对冲是"追价", 被动单常错过成交 (价格跑开)，requote 追挂——回测会体现这部分裸 delta 成本,
  * 这正是 maker vs taker 的取舍 (省费 vs 漏对冲)。onEvent 由框架单线程串行调用。 */
final class MakerHedgeStrategy(
    exchange: Exchange,
    symbol: Symbol,
    ccy: String,
    band: HedgeBand,
    /** 被动挂单相对 BBO 的改善幅度 (0.0001=0.01%): 卖挂 bestAsk·(1+offset)、买挂 bestBid·(1−offset) */
    offsetPct: Double = 0.0001,
    /** 未成交重挂间隔 (ms) */
    requoteMs: Long = 5000,
    atrPeriodBars: Int = 14,
    macdTrendBars: Int = 2,
    rvShortWindowBars: Int = 24,
    rvLongWindowBars: Int = 168,
    maSmaPeriod: Int = 20,
    /** 是否用 gamma 一阶修正 delta (两次 greeks 更新间用现价刷新, tick 级新鲜)。回测中 greeks 每秒已重算,
      * 默认关 (行为不变); 实盘 greeks 轮询较慢, 开启可消除轮询间的 delta 滞后。 */
    gammaAdjust: Boolean = false,
    /** greeks 陈旧阈值 (ms): >0 时, greeks 距今超过它则暂停对冲 (防按过期 delta 乱挂); 0=不限 (回测默认) */
    maxGreeksStaleMs: Long = 0L,
    barIntervalMs: Long = 3_600_000L,
    minHedgeQty: Coin = Coin(0.001),
    /** 单笔对冲数量硬上限 (币本位) (sanity): 超出则不下单 + 告警 (防 delta/gamma 计算 bug 误下巨单)。默认不限 (回测) */
    maxHedgeQty: Coin = Coin(Double.MaxValue),
    /** 取历史 K 线的通道 —— `(粒度毫秒, 根数) => 最旧->最新的 (high, low, close)`。
      * [[prepare]] 在开跑前用它把 ATR/均线喂热。默认不预热 (回测与单测自己喂, 见 [[prewarm]]) */
    history: (Long, Int) => Either[String, Seq[(Double, Double, Double)]] =
      (_, _) => Left("没有接预热数据源"),
) extends Strategy:
  /** 本策略交易的标的 —— 状态查询与行情声明的同一个键 */
  private val instrument = Instrument(exchange, symbol)
  private val logger = org.slf4j.LoggerFactory.getLogger(classOf[MakerHedgeStrategy])
  private var warnCnt = 0L
  private def warnThrottled(msg: String): Unit =
    if warnCnt % 200 == 0 then logger.warn(s"[MakerHedge $symbol] $msg")
    warnCnt += 1

  private val klines =
    new KlineSeries(barIntervalMs, math.max(math.max(atrPeriodBars * 4, rvLongWindowBars + 8), 64))
      with Atr with Macd with RealizedVol with Sma:
      override protected def atrPeriod: Int = atrPeriodBars
      override protected def rvShortBars: Int = rvShortWindowBars
      override protected def rvLongBars: Int = rvLongWindowBars
      override protected def smaPeriod: Int = maSmaPeriod

  private var center: Double = Double.NaN
  /** 本策略恒用被动挂单 (价格轴对冲的既有行为)。挂/撤/防重的机制与敞口轴对冲策略
    * 共用一份, 见 [[QuoteLeg]]。 */
  private val quoteStyle = QuoteStyle.passive(offsetPct, requoteMs)
  private val leg = QuoteLeg()
  private var greeksRefMid: Double = Double.NaN // 上次 greeks 更新时的中间价 (gamma 修正基准)

  /** 就绪：用历史 K 线把 ATR/均线喂热，开机即就绪。
    *
    * 粒度与根数直接问序列自己要 —— 从前启动器写死拉 64 根, 而序列容量按
    * `max(atrPeriodBars*4, rvLongWindowBars+8, 64)` 算 (默认 176), 于是长窗口的 RV
    * 其实没被喂满。同一个事实写在两处, 对不上也没人会发现。
    *
    * ## 预热失败的后果是**完全不对冲**，不是"保守一些"
    *
    * 对冲带的宽度是 `f(ATR)`，而 ATR 未就绪时整个 `manage` 分支被 `atr <- klines.atr` 短路
    * (见下面那个 for)：**一次也不对冲**。默认 `atrPeriodBars=14`、1h bar，也就是靠实时 BBO
    * 慢热要 **14 小时**。对一个已经卖出期权的负 gamma 账户，那不是保守，是裸奔。
    *
    * 这里仍只降级不终止 (`Strategy.prepare` 允许策略自己选)，但错误信息必须把真实后果说清 ——
    * 从前写的是"对冲会保守一些"，照着它读日志的人不会知道自己有十几个小时没有对冲。
    */
  override def prepare(): Unit =
    val blindMs = klines.periodMs * atrPeriodBars
    def blindWarning(reason: String): String =
      f"!!! [$symbol] $reason —— ATR 未就绪期间**完全不对冲** (不是保守对冲): " +
        f"需靠实时行情慢热约 ${blindMs / 3_600_000.0}%.1f 小时" +
        "; 期权已卖出的账户在此期间是裸敞口, 请人工确认是否继续"
    history(klines.periodMs, klines.maxBars) match
      case Right(bars) if bars.nonEmpty =>
        prewarm(bars)
        logger.warn(s"[$symbol] 预热 ${bars.size} 根 ${klines.periodMs}ms K 线 -> ATR/均线就绪")
      case Right(_) =>
        logger.error(blindWarning("预热取到空 (这个标的没有那么长的历史?)"))
      case Left(why) =>
        logger.error(blindWarning(s"预热失败: $why"))

  /** 喂历史 (high, low, close) 进 K 线 (h/l/c 当三笔 tick), 使 ATR/均线在开机即就绪,
    * 避免实盘冷启动需等数十根 BBO 累积才敢对冲。最旧->最新。 */
  def prewarm(bars: Seq[(Double, Double, Double)]): Unit =
    bars.zipWithIndex.foreach { case ((h, l, c), i) =>
      val t = i.toLong * barIntervalMs; klines.update(t, h); klines.update(t, l); klines.update(t, c)
    }

  override def handlers: StrategyHandlers = StrategyHandlers.empty
    .own(Topics.OrderUpdate) { (u, _, now) =>
      leg.onOrderUpdate(u, now).foreach(px => center = px.value) // 对冲成交 -> 中心重置到成交价
      Vector.empty
    }
    .market(Topics.Bbo, instrument) { (b, ctx, now) =>
      val px = b.midPrice.value
      klines.update(b.timestamp, px)
      if center.isNaN then center = px
      // 用**本地处理时刻**：挂单年龄与 greeks 陈旧度都算在本地钟上 (见 QuoteLeg 的"时钟域")。
      // K 线的时间轴仍用交易所钟 (上一行) —— 那是"这根 bar 属于哪一刻", 不是"多久以前"。
      manage(b, now, ctx)
    }
    // greeks 的路由键只到交易所，币种在载荷里，故 ccy 仍需自行判断
    .account(Topics.Greeks) { (g, ctx, now) =>
      if g.ccy != ccy then Vector.empty
      else
        ctx.state.instrumentState(instrument).flatMap(_.bbo).map { b =>
          greeksRefMid = b.midPrice.value // 记录本次 greeks 对应的现价, 供 gamma 修正
          manage(b, now, ctx) // 同上: 本地钟
        }.getOrElse(Vector.empty)
    }

  /** 两个入口 (BBO tick / greeks tick) 都是在**拿到盘口之后**才进来的，所以这里收 [[BBO]]
    * 本体而不是"中间价 + 再自己查一次盘口"。
    *
    * 从前它只收 `px`，然后在下单前又 `ss.bbo(exchange)` 查一遍并写"盘口缺失则回退中间价
    * (降级, 告警)" —— 而那个"中间价"本身就是从这条查不到的盘口算出来的。同一 tick 同一份
    * 状态，缺失是不可能的：那条降级分支是给一个不存在的状态预留的防御。 */
  private def manage(bbo: BBO, now: Timestamp, ctx: StrategyContext): Vector[AnyEvent] =
    val px = bbo.midPrice.value
    leg.step(now, quoteStyle) match
      case QuoteLeg.Step.Blocked => Vector.empty
      case QuoteLeg.Step.Requote(ref, why) =>
        // 恒用同一种方式 -> 不会被抢占; 只有"撤单确认没回来"值得告警 (期间对冲实际停着)
        why match
          case QuoteLeg.Requote.Unconfirmed(waited) =>
            warnThrottled(s"撤单确认 ${waited}ms 未到, 重发撤单 $ref (期间不挂新单, 对冲暂停)")
          case _ => ()
        Vector(ctx.cancel(exchange, symbol, ref))
      case QuoteLeg.Step.Ready =>
          ctx.state.greeks(exchange, ccy) match
            case None =>
              warnThrottled("greeks/ccy 余额未就绪 -> 未对冲 (检查期权 greeks 流是否在推、启动对齐的钱包快照是否到过)")
              Vector.empty
            // 陈旧度问状态层要 (本地接收时刻), 不拿载荷里的交易所时间戳去减本地的 now ——
            // 那是跨时钟域相减, 偏斜超过阈值就会永久暂停对冲, 且没有任何症状。
            case Some(_) if maxGreeksStaleMs > 0 &&
                ctx.state.greeksAgeMs(exchange, ccy, now).exists(_ > maxGreeksStaleMs) =>
              val age = ctx.state.greeksAgeMs(exchange, ccy, now).getOrElse(0L)
              warnThrottled(s"greeks 陈旧 ${age}ms > ${maxGreeksStaleMs}ms -> 暂停对冲 (宁可不动也不按过期 delta 乱挂)")
              Vector.empty
            case Some(greeks) =>
              // ATR / 中枢未就绪 = **完全不对冲**, 而这段时间长达 periodMs × atrPeriodBars
              // (默认 1h × 14 ≈ 14 小时)。启动时报过一次不够: 之后十几个小时一条日志都没有,
              // 与"在对冲但没触发"从外面看完全一样。所以这里持续报出当前状态与原因。
              if klines.atr.forall(_ <= 0.0) || center.isNaN then
                warnThrottled(
                  s"ATR/中枢未就绪 -> **完全不对冲** (atr=${klines.atr.map(_.toString).getOrElse("预热中")} " +
                    s"center=${if center.isNaN then "预热中" else center.toString}); 预热需 ${atrPeriodBars} 根 ${barIntervalMs}ms K 线"
                )
              (for
                ss <- ctx.state.instrumentState(instrument)
                atr <- klines.atr
                if atr > 0.0 && !center.isNaN
              yield
                val maBias = klines.sma.fold(0)(m => math.signum(px - m).toInt)
                // 1h MACD 柱斜率: 较前一根升=+1、降=-1、持平/预热不足=0 (驱动 AsymHedgeBand.byMacdBar)
                val macdHistDir =
                  if !klines.macdReady then 0
                  else if klines.macdHistSeries.rising(1) then 1
                  else if klines.macdHistSeries.falling(1) then -1
                  else 0
                val hc = HedgeCtx(px, center, atr, klines.volRatio.getOrElse(1.0), klines.histBias(macdTrendBars), maBias, macdHistDir)
                val (up, down) = band.bands(hc)
                val crossed = (px - center > up) || (center - px > down)
                if !crossed then Vector.empty
                else
                  // gamma 一阶修正: 两次 greeks 间用现价相对基准价刷新 delta (tick 级)
                  val gammaAdj = if gammaAdjust && !greeksRefMid.isNaN then greeks.gamma * (px - greeksRefMid) else 0.0
                  val netDelta = greeks.delta + gammaAdj + ss.positionSize.value
                  val qty = math.abs(netDelta)
                  if qty < minHedgeQty.value then Vector.empty
                  else if qty > maxHedgeQty.value then
                    warnThrottled(s"对冲量 $qty 超硬上限 $maxHedgeQty -> 不下单 (疑似 delta/gamma bug, 请查)")
                    Vector.empty
                  else
                    val side = if netDelta > 0 then Side.Short else Side.Long // 净多→卖, 净空→买
                    // BBO 外 offset 挂被动单: 卖挂 bestAsk·(1+off)、买挂 bestBid·(1−off)
                    val (limitPx, tif) = leg.place(quoteStyle, side, bbo)
                    ctx.place(
                      Order("", exchange, symbol, side, OrderType.Limit(limitPx, tif), Coin(qty), reduceOnly = false, clientOrderId = ""),
                      f"maker_hedge | $side qty=$qty%.4f limit=$limitPx%.2f px=$px%.2f netDelta=$netDelta%.4f maBias=$maBias band=($up%.2f,$down%.2f)",
                    )
              ).getOrElse(Vector.empty)
