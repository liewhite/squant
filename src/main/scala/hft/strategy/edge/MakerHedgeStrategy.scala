package hft.strategy.edge

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
    barIntervalMs: Long = 3_600_000L,
    minHedgeQty: Quantity = 0.001,
) extends Strategy:

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
        state.symbolState(symbol).flatMap(_.bbo(exchange)).map(b => manage(b.midPrice, event.exchangeTs, state)).getOrElse(Vector.empty)
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
          (for
            ss <- state.symbolState(symbol)
            greeks <- state.greeks(exchange, ccy)
            atr <- klines.atr
            if atr > 0.0 && !center.isNaN
          yield
            val maBias = klines.sma.fold(0)(m => math.signum(px - m).toInt)
            val ctx = HedgeCtx(px, center, atr, klines.volRatio.getOrElse(1.0), klines.histBias(macdTrendBars), maBias)
            val (up, down) = band.bands(ctx)
            val crossed = (px - center > up) || (center - px > down)
            if !crossed then Vector.empty
            else
              val netDelta = greeks.delta + ss.positionSize(exchange)
              val qty = math.abs(netDelta)
              if qty < minHedgeQty then Vector.empty
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
