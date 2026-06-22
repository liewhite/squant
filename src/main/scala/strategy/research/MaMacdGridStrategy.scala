package strategy.research

import hft.strategy.{OutcomeEvent, Strategy}

import hft.domain.*
import hft.exchange.SubscriptionKind
import hft.indicator.{Atr, KlineSeries, Macd, Sma}
import hft.messaging.{EventData, IncomeEvent, PendingOrder, StateManager}

import scala.collection.mutable

/** 均线 + MACD 方向性网格策略 (trade-native)。
  *
  * 用小时 K 线上的 [[smaPeriod]] 均线判趋势方向、MACD 柱 (histogram) 连续性判动量，分四个"档位 (slot)"
  * 各维护至多一张挂单，构成方向性网格：
  *
  *   - 加仓：价在均线**上** 且 MACD 柱连续上升 → 加仓买；价在均线**下** 且柱连续下降 → 加仓卖。
  *     续势被破坏 (不再连续同向) 即停止加仓。
  *   - 止盈/离场 (持仓即挂, 不限均线侧)：默认 **宽且静态 (largeSpacing, 让利润跑)**；
  *     **仅当"双确认反转"成立才切窄移动止盈 (exitSpacing, 随价重挂跟踪离场)**——
  *       多头反转 = 价**跌破均线** 且 MACD 柱**连续 waterBars 根在水下** (持续死叉)；空头对称。
  *
  * 为何双确认 + 多根水下：单一死叉/单根翻负在趋势回调中过于灵敏, 会把仓位反复洗出去 (whipsaw, 实测长周期净亏)。
  * 要求"均线结构破坏 + 动量持续翻向"两者同时, 才认定趋势真反转、贴身止盈离场；否则一路宽静态 hold 吃趋势。
  *
  * 设计要点 (与撮合/框架对齐)：
  *   - 仅用 GTC 限价单：trade-native 行情无 L1 盘口，限价单到达即 resting，由真实成交价穿过而 maker 成交；
  *     市价单在无盘口时会被拒 (见 SimState.onOrderArrived)，故不用。
  *   - 平仓语义由策略自持 + 撮合双保险：平仓单数量截断到当前持仓且仅在有对应持仓时挂出，撮合层 reduceOnly
  *     再按实时持仓截断 —— 平仓单绝不反向开仓；只有"加仓单"允许净持仓穿越/反向。
  *   - **加仓单 + 顺势止盈单 = 静态**：只在缺失时按当前价补挂 (成交即重锚)，resting 期间不追价 (让趋势跑)。
  *   - **动量消失止盈单 = 移动止盈** (trailingExit)：漂移超过 exitSpacing 即撤单、下一拍按新价重挂，贴现价跟踪，
  *     价格反转下行时跟着下移 (靠小反弹成交)。仍是 maker 限价单，接不住"无反弹瀑布"(那需主动 taker，另议)。
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
    /** 顺势加仓档距 (静态网格)，如 0.004 = 0.4% */
    addSpacing: Double = 0.004,
    /** 默认顺势止盈档距 (宽, 静态, 让利润跑)，如 0.05 = 5% */
    largeSpacing: Double = 0.05,
    /** MACD 反向后的移动止盈档距 (窄, 随价重挂)，如 0.004 = 0.4% */
    exitSpacing: Double = 0.004,
    /** 反向离场时止盈单是否移动 (随价重挂); false 则静态切窄但不追 */
    trailingExit: Boolean = true,
    /** 是否启用"双确认"动量离场; false=始终宽静态止盈 (只吃趋势, 不离场) */
    reversalExit: Boolean = true,
    /** 双确认离场所需的连续"水下/水上"根数 (柱持续翻负/翻正): 多头需价跌破均线 且 柱连续 waterBars 根<0 */
    waterBars: Int = 5,
    /** 【方案1】单次平仓的**最小持仓占比** (0=关闭, 沿用逐笔 min(baseQty,pos))。
      * 启用时单次平仓量 = min(pos, max(pos·closeMinFraction, baseQty))，即"至少 max(占比×持仓, 单次开仓量)"，
      * 让浮盈在反转前更快兑现，压低净值冲高回落的回吐。 */
    closeMinFraction: Double = 0.0,
    /** 【方案2】价偏离均线超过 atrStretchN×ATR 时进入移动止盈 且 停止该方向加仓 (0=关闭)。
      * N 由历史 |price−ma|/atr 分布定 (见 hft.demo.MaDeviationAnalysis)，度量趋势拉伸/超买超卖。 */
    atrStretchN: Double = 0.0,
    /** ATR 周期 (根)，方案2 用 */
    atrPeriodBars: Int = 14,
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
      with Macd with Sma with Atr:
      override protected def smaPeriod: Int = maPeriodBars
      override protected def atrPeriod: Int = atrPeriodBars
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
        // 逐笔直接读指标 (盘中量恒为最新)；收盘衍生量 (sma/atr/macd 柱历史) 已在各指标内部于收盘时
        // 算定为 O(1) 读取、水上/水下判据用 RingSeries.lastAll 免分配 —— 无策略侧缓存, 无陈旧风险。
        klines.sma match
          case None => Vector.empty // 均线未预热 -> 不动作
          case Some(ma) =>
            val pos = ss.positionSize(exchange)
            val above = price > ma
            val below = price < ma
            val rising = klines.macdHistSeries.rising(risingBars) // 续势 (加仓用)
            val falling = klines.macdHistSeries.falling(risingBars)
            // 双确认离场判据: 柱连续 waterBars 根在水下/水上 (用已收盘柱, 钝化, 抗趋势中段假信号)
            val belowWater = klines.macdHistSeries.lastAll(waterBars)(_ < 0)
            val aboveWater = klines.macdHistSeries.lastAll(waterBars)(_ > 0)
            // 方案2: 价偏离均线超过 atrStretchN×ATR -> 拉伸过度。该方向"锁盈离场(移动止盈) + 停止加仓"。
            val overstretched =
              atrStretchN > 0.0 && klines.atr.exists(a => a > 0.0 && math.abs(price - ma) / a >= atrStretchN)
            val stretchedUp = overstretched && above   // 价远在均线上 -> 多头超买: 锁多 / 停加多
            val stretchedDown = overstretched && below // 价远在均线下 -> 空头超卖: 锁空 / 停加空

            // 单次平仓量。方案1 (closeMinFraction>0): 至少 max(占比×持仓, 单次开仓量); 否则逐笔 min(baseQty,pos)。
            def closeQty(absPos: Double): Quantity =
              if closeMinFraction <= 0.0 then math.min(baseQty, absPos)
              else math.min(absPos, math.max(absPos * closeMinFraction, baseQty))

            // 每个 slot 的期望挂单 (Side, 价格, 数量, reduceOnly, 是否移动止盈)；None = 不期望。
            // 止盈两态：默认宽静态 (largeSpacing)；仅当 MACD 明确反向 (连续 N 根) 才转窄移动 (exitSpacing)。
            def spec(slot: Slot): Option[(Side, Price, Quantity, Boolean, Boolean)] = slot match
              case Slot.BuyAccum =>
                // 价远超均线 (stretchedUp) 时停止加多 (方案2)
                Option.when(above && rising && !stretchedUp && pos < maxPositionCoin)(
                  (Side.Long, price * (1 - addSpacing), baseQty, false, false)
                )
              case Slot.SellClose =>
                // 持多即挂止盈卖 (不限均线侧)：默认宽静态; 反转 (双确认 或 方案2 拉伸过度) 才切窄移动
                Option.when(pos > 0.0) {
                  val reversed = (reversalExit && below && belowWater) || stretchedUp
                  val sp = if reversed then exitSpacing else largeSpacing
                  (Side.Short, price * (1 + sp), closeQty(pos), true, reversed && trailingExit)
                }
              case Slot.SellAccum =>
                // 价远低均线 (stretchedDown) 时停止加空 (方案2)
                Option.when(below && falling && !stretchedDown && pos > -maxPositionCoin)(
                  (Side.Short, price * (1 + addSpacing), baseQty, false, false)
                )
              case Slot.BuyClose =>
                // 持空即挂止盈买：默认宽静态; 反转 (双确认 或 方案2 拉伸过度) 才切窄移动
                Option.when(pos < 0.0) {
                  val reversed = (reversalExit && above && aboveWater) || stretchedDown
                  val sp = if reversed then exitSpacing else largeSpacing
                  (Side.Long, price * (1 - sp), closeQty(-pos), true, reversed && trailingExit)
                }

            // 现有 pending 按 slot 归类 (每 slot 至多一张：只在缺失时补挂)。
            // 单趟扫描填入按 Slot.ordinal 索引的定长数组, 保留每槽首张 (与 groupBy+head 同义),
            // 避免逐笔 groupBy/Map 分配。
            val present = new Array[PendingOrder](Slot.values.length) // 引用数组, 默认 null
            ss.pendingOrders.foreach { p =>
              val i = slotOf(p.order).ordinal
              if present(i) == null then present(i) = p
            }

            val actions = mutable.ArrayBuffer.empty[OutcomeEvent]
            Slot.values.foreach { slot =>
              (spec(slot), Option(present(slot.ordinal))) match
                // 期望且缺失 -> 按当前价补挂 (网格重锚 / 移动止盈重挂)
                case (Some((side, px, qty, ro, _)), None) if qty > 0.0 =>
                  actions += OutcomeEvent.PlaceOrders(
                    Vector(mkOrder(side, px, qty, ro)),
                    f"grid:$slot ${if ro then "close" else "accum"} $side px=$px%.2f qty=$qty%.4f pos=$pos%.4f ma=$ma%.2f rise=$rising fall=$falling",
                  )
                // 移动止盈档 (trail=true): resting 价漂移超 exitSpacing -> 撤单, 下一拍按新价重挂跟踪
                case (Some((_, px, _, _, trail)), Some(p)) if trail && drifted(p, px) =>
                  cancelConfirmed(p).foreach(actions += _)
                // 不期望但仍挂着 -> 撤单 (含: 宽止盈->反向后切窄, resting 宽单漂移过大被这里撤; 仅撤已确认的)
                case (None, Some(p)) =>
                  cancelConfirmed(p).foreach(actions += _)
                // 加仓单/宽静态止盈单已挂 -> 保持 resting (不追价)；其余无事
                case _ => ()
            }
            actions.toVector

  private def slotOf(o: Order): Slot = (o.side, o.reduceOnly) match
    case (Side.Long, false)  => Slot.BuyAccum
    case (Side.Short, true)  => Slot.SellClose
    case (Side.Short, false) => Slot.SellAccum
    case (Side.Long, true)   => Slot.BuyClose

  /** resting 挂单价相对期望价漂移是否超过 exitSpacing (移动止盈触发重挂) */
  private def drifted(p: PendingOrder, desired: Price): Boolean =
    p.order.orderType match
      case OrderType.Limit(restingPx, _) => math.abs(restingPx - desired) / desired > exitSpacing
      case _                             => false

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
