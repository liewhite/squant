package strategy.live

import hft.domain.*
import hft.exchange.SubscriptionKind
import hft.indicator.{Atr, KlineSeries, Macd, RealizedVol, Sma}
import hft.messaging.{EventData, IncomeEvent, StateManager}
import hft.strategy.{OutcomeEvent, Strategy}

/** **Maker (被动挂单) 对冲** —— 与 [[BandHedgeStrategy]] 同样用 [[HedgeBand]] 决定**何时**对冲, 但执行改为
  * 在现价外 [[offsetPct]] (默认 0.02%) 挂 PostOnly 限价单 (省 taker 费 + 赚价差改善), [[requoteMs]] (默认 5s)
  * 未成交则撤单, 下一 tick 按新价重挂。
  *
  * 机制 (同一时刻最多一张挂单)：
  *   - 越带且净 delta≥minQty 且无挂单 -> 挂被动单 (卖挂高 +offset, 买挂低 −offset; 净多→卖, 净空→买)。
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
    /** 被动挂单相对现价的改善幅度 (0.0002=0.02%): 卖挂 px·(1+offset)、买挂 px·(1−offset) */
    offsetPct: Double = 0.0002,
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
    minHedgeQty: Quantity = 0.001,
    /** 单笔对冲张数硬上限 (sanity): 超出则不下单 + 告警 (防 delta/gamma 计算 bug 误下巨单)。默认不限 (回测) */
    maxHedgeQty: Quantity = Double.MaxValue,
) extends Strategy:
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
  private var restingId: Option[OrderId] = None // 当前挂单的撮合 orderId (来自回流)
  private var restingAt: Timestamp = 0L
  private var awaitingAck: Boolean = false // 已下单、等待 Pending 回流确认
  private var greeksRefMid: Double = Double.NaN // 上次 greeks 更新时的中间价 (gamma 修正基准)

  /** 启动预热: 用历史 (high, low, close) 喂 K 线 (h/l/c 当三笔 tick), 使 ATR/均线在开机即就绪,
    * 避免实盘冷启动需等数十根 BBO 累积才敢对冲。最旧->最新。 */
  def prewarm(bars: Seq[(Double, Double, Double)]): Unit =
    bars.zipWithIndex.foreach { case ((h, l, c), i) =>
      val t = i.toLong * barIntervalMs; klines.update(t, h); klines.update(t, l); klines.update(t, c)
    }

  override def publicStreams: Map[Exchange, Set[SubscriptionKind]] =
    Map(exchange -> Set(SubscriptionKind.BBO(symbol)))

  // 订单超时需 > requote, 否则框架会先把我们的正常挂单当超时清理
  override def orderTimeoutMs: Long = requoteMs * 3

  override def onEvent(event: IncomeEvent, state: StateManager): Vector[OutcomeEvent] =
    event.data match
      case EventData.OrderUpdated(u) if u.exchange == exchange && u.symbol == symbol =>
        u.status match
          case OrderStatus.Pending | OrderStatus.PartiallyFilled(_) =>
            restingId = Some(u.orderId); restingAt = u.timestamp; awaitingAck = false
          case OrderStatus.Filled =>
            center = u.price; restingId = None; awaitingAck = false // 对冲成交 -> 中心重置
          case OrderStatus.Cancelled | OrderStatus.Rejected(_) | OrderStatus.Error(_) =>
            restingId = None; awaitingAck = false
          case OrderStatus.Created => () // 本地态, 等确认
        Vector.empty
      case EventData.BboUpdate(b) if b.exchange == exchange && b.symbol == symbol =>
        val px = b.midPrice
        klines.update(b.timestamp, px)
        if center.isNaN then center = px
        manage(px, event.exchangeTs, state)
      case EventData.GreeksUpdate(g) if g.exchange == exchange && g.ccy == ccy =>
        state.symbolState(symbol).flatMap(_.bbo(exchange)).map { b =>
          greeksRefMid = b.midPrice // 记录本次 greeks 对应的现价, 供 gamma 修正
          manage(b.midPrice, event.exchangeTs, state)
        }.getOrElse(Vector.empty)
      case _ => Vector.empty

  private def manage(px: Price, now: Timestamp, state: StateManager): Vector[OutcomeEvent] =
    if awaitingAck then Vector.empty
    else
      restingId match
        case Some(id) =>
          if now - restingAt > requoteMs then
            restingId = None // 撤后下一 tick 重挂 (按新价)
            Vector(OutcomeEvent.CancelOrder(exchange, symbol, id))
          else Vector.empty
        case None =>
          state.greeks(exchange, ccy) match
            case None =>
              warnThrottled("greeks/ccy 余额未就绪 -> 未对冲 (检查期权 greeks 流是否在推、ccy 余额是否注入)")
              Vector.empty
            case Some(g) if maxGreeksStaleMs > 0 && now - g.timestamp > maxGreeksStaleMs =>
              warnThrottled(s"greeks 陈旧 ${now - g.timestamp}ms > ${maxGreeksStaleMs}ms -> 暂停对冲 (宁可不动也不按过期 delta 乱挂)")
              Vector.empty
            case Some(greeks) =>
              (for
                ss <- state.symbolState(symbol)
                atr <- klines.atr
                if atr > 0.0 && !center.isNaN
              yield
                val maBias = klines.sma.fold(0)(m => math.signum(px - m).toInt)
                val ctx = HedgeCtx(px, center, atr, klines.volRatio.getOrElse(1.0), klines.histBias(macdTrendBars), maBias)
                val (up, down) = band.bands(ctx)
                val crossed = (px - center > up) || (center - px > down)
                if !crossed then Vector.empty
                else
                  // gamma 一阶修正: 两次 greeks 间用现价相对基准价刷新 delta (tick 级)
                  val gammaAdj = if gammaAdjust && !greeksRefMid.isNaN then greeks.gamma * (px - greeksRefMid) else 0.0
                  val netDelta = greeks.delta + gammaAdj + ss.positionSize(exchange)
                  val qty = math.abs(netDelta)
                  if qty < minHedgeQty then Vector.empty
                  else if qty > maxHedgeQty then
                    warnThrottled(s"对冲量 $qty 超硬上限 $maxHedgeQty -> 不下单 (疑似 delta/gamma bug, 请查)")
                    Vector.empty
                  else
                    val side = if netDelta > 0 then Side.Short else Side.Long // 净多→卖, 净空→买
                    val limitPx = if side == Side.Short then px * (1.0 + offsetPct) else px * (1.0 - offsetPct)
                    awaitingAck = true
                    Vector(
                      OutcomeEvent.PlaceOrders(
                        Vector(Order("", exchange, symbol, side, OrderType.Limit(limitPx, TimeInForce.PostOnly), qty, reduceOnly = false, clientOrderId = "")),
                        f"maker_hedge | $side qty=$qty%.4f limit=$limitPx%.2f px=$px%.2f netDelta=$netDelta%.4f maBias=$maBias band=($up%.2f,$down%.2f)",
                      )
                    )
              ).getOrElse(Vector.empty)
