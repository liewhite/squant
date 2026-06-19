package hft.strategy

import hft.domain.*
import hft.exchange.SubscriptionKind
import hft.indicator.{KlineSeries, Macd, Sma}
import hft.messaging.{EventData, IncomeEvent, PendingOrder, StateManager}

import scala.collection.mutable

/** 均线 + MACD 方向性网格策略 (trade-native)。
  *
  * 用小时 K 线上的 [[smaPeriod]] 均线判趋势方向、MACD 柱 (histogram) 连续性判动量，分四个"档位 (slot)"
  * 各维护至多一张挂单，构成方向性网格：
  *
  *   - 价在均线**上**：
  *       · MACD 柱连续上升 (≥ risingBars 根) → **加仓买 (小档距) + 平仓卖 (大档距)**
  *       · 否则 (动量未续) → **只挂多头平仓卖单 (大档距)**，不再加仓
  *   - 价在均线**下** (对称)：
  *       · MACD 柱连续下降 → **加仓卖 (小档距) + 平仓买 (大档距)**
  *       · 否则 → **只挂空头平仓买单 (大档距)**
  *
  * 设计要点 (与撮合/框架对齐)：
  *   - 仅用 GTC 限价单：trade-only 行情无 L1 盘口，限价单到达即 resting，由真实成交价穿过而 maker 成交；
  *     市价单在无盘口时会被拒 (见 SimState.onOrderArrived)，故不用。
  *   - 平仓语义由策略自持 (撮合不识别 reduceOnly)：平仓单数量截断到当前持仓、且仅在有对应持仓时挂出，
  *     故平仓单绝不会反向开仓；只有"加仓单"允许净持仓穿越/反向 (净持仓模型下自然完成趋势反转时的换手)。
  *   - 网格行为：每个 slot 的挂单**只在缺失时按当前价重挂** (成交后下一笔行情即在新价位补挂)，
  *     resting 期间不追价 —— 等价于"成交即重锚"的网格，价格须回到挂单价才成交。
  *   - 持仓上限 maxPositionCoin 限制单向累积，超限即停止该方向加仓 (平仓不受限)。
  *
  * 系数 (档距/每档量/持仓上限) 由回测确定，见 hft.demo.MaMacdGridBacktest。
  */
final class MaMacdGridStrategy(
    exchange: Exchange,
    symbol: Symbol,
    /** 均线周期 (根)，配合 barIntervalMs：60 + 1h = 60 小时均线 */
    maPeriodBars: Int = 60,
    /** K 线周期，默认 1 小时 */
    barIntervalMs: Long = 3_600_000L,
    /** MACD 柱"连续上升/下降"的根数门槛 (连续 2 根或以上) */
    risingBars: Int = 2,
    /** 顺势加仓档距 (小)，如 0.004 = 0.4% (回测优选，见 MaMacdGridBacktest) */
    smallSpacing: Double = 0.004,
    /** 平仓/止盈档距 (大)，如 0.05 = 5% (回测优选：宽止盈让顺势仓位跑) */
    largeSpacing: Double = 0.05,
    /** 每档下单量 (币本位) */
    baseQty: Quantity = 0.5,
    /** 单向持仓上限 (币本位)，超限停止该方向加仓 */
    maxPositionCoin: Double = 10.0,
    macdFastP: Int = 12,
    macdSlowP: Int = 26,
    macdSignalP: Int = 9,
) extends Strategy:

  /** 四个挂单档位 */
  private enum Slot:
    case BuyAccum, SellClose, SellAccum, BuyClose

  private val klines =
    new KlineSeries(barIntervalMs, math.max(maPeriodBars, macdSlowP + macdSignalP) + risingBars + 2)
      with Macd with Sma:
      override protected def smaPeriod: Int = maPeriodBars
      override protected def macdFast: Int = macdFastP
      override protected def macdSlow: Int = macdSlowP
      override protected def macdSignalPeriod: Int = macdSignalP

  /** 已发出撤单、等待 Cancelled 确认的订单 (避免重复撤单) */
  private val cancelling = mutable.Set.empty[OrderId]

  override def publicStreams: Map[Exchange, Set[SubscriptionKind]] =
    Map(exchange -> Set(SubscriptionKind.Trade(symbol)))

  override def orderTimeoutMs: Long = 5000

  override def onEvent(event: IncomeEvent, state: StateManager): Vector[OutcomeEvent] =
    event.data match
      case EventData.MarketTradeUpdate(t) if t.exchange == exchange && t.symbol == symbol =>
        klines.update(t.timestamp, t.price, t.qty)
        reconcile(t.price, state)
      case _ => Vector.empty

  private def reconcile(price: Price, state: StateManager): Vector[OutcomeEvent] =
    state.symbolState(symbol) match
      case None => Vector.empty
      case Some(ss) =>
        // 清理已确认消失 (成交/撤成) 的撤单追踪
        cancelling.filterInPlace(id => ss.pendingOrders.exists(_.order.id == id))
        klines.sma match
          case None => Vector.empty // 均线未预热 -> 不动作
          case Some(ma) =>
            val pos = ss.positionSize(exchange)
            val above = price > ma
            val below = price < ma
            val rising = klines.macdHistSeries.rising(risingBars)
            val falling = klines.macdHistSeries.falling(risingBars)

            // 每个 slot 的期望挂单 (Side, 价格, 数量, reduceOnly)；None = 不期望
            def spec(slot: Slot): Option[(Side, Price, Quantity, Boolean)] = slot match
              case Slot.BuyAccum =>
                Option.when(above && rising && pos < maxPositionCoin)(
                  (Side.Long, price * (1 - smallSpacing), baseQty, false)
                )
              case Slot.SellClose =>
                Option.when(above && pos > 0.0)(
                  (Side.Short, price * (1 + largeSpacing), math.min(baseQty, pos), true)
                )
              case Slot.SellAccum =>
                Option.when(below && falling && pos > -maxPositionCoin)(
                  (Side.Short, price * (1 + smallSpacing), baseQty, false)
                )
              case Slot.BuyClose =>
                Option.when(below && pos < 0.0)(
                  (Side.Long, price * (1 - largeSpacing), math.min(baseQty, -pos), true)
                )

            // 现有 pending 按 slot 归类 (每 slot 至多一张：只在缺失时补挂)
            val present: Map[Slot, PendingOrder] =
              ss.pendingOrders.groupBy(p => slotOf(p.order)).view.mapValues(_.head).toMap

            val actions = mutable.ArrayBuffer.empty[OutcomeEvent]
            Slot.values.foreach { slot =>
              (spec(slot), present.get(slot)) match
                // 期望且缺失 -> 按当前价补挂 (网格重锚)
                case (Some((side, px, qty, ro)), None) if qty > 0.0 =>
                  actions += OutcomeEvent.PlaceOrders(
                    Vector(mkOrder(side, px, qty, ro)),
                    f"grid:$slot ${if ro then "close" else "accum"} $side px=$px%.2f qty=$qty%.4f pos=$pos%.4f ma=$ma%.2f rise=$rising fall=$falling",
                  )
                // 不期望但仍挂着 -> 撤单 (仅撤已确认的)
                case (None, Some(p)) =>
                  cancelConfirmed(p).foreach(actions += _)
                // 期望且已挂 -> 保持 resting (不追价)；不期望且无挂单 -> 无事
                case _ => ()
            }
            actions.toVector

  private def slotOf(o: Order): Slot = (o.side, o.reduceOnly) match
    case (Side.Long, false)  => Slot.BuyAccum
    case (Side.Short, true)  => Slot.SellClose
    case (Side.Short, false) => Slot.SellAccum
    case (Side.Long, true)   => Slot.BuyClose

  private def mkOrder(side: Side, price: Price, qty: Quantity, reduceOnly: Boolean): Order =
    Order(
      id = "",
      exchange = exchange,
      symbol = symbol,
      side = side,
      orderType = OrderType.Limit(price, TimeInForce.GTC),
      quantity = qty,
      reduceOnly = reduceOnly,
      clientOrderId = "",
    )

  private def cancelConfirmed(p: PendingOrder): Option[OutcomeEvent] =
    if p.status.isConfirmed && !cancelling.contains(p.order.id) then
      cancelling += p.order.id
      Some(OutcomeEvent.CancelOrder(exchange, symbol, p.order.id))
    else None
