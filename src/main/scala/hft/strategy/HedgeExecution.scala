package hft.strategy

import hft.domain.*
import hft.messaging.PendingOrder

import scala.collection.mutable

/** 把净 delta 拉到目标的一次对冲诉求：方向 + 币本位数量 + 参考价。 */
final case class HedgeRequest(side: Side, qty: Quantity, refPrice: Price)

/** 对冲执行器：决定**如何**把净 delta 拉到目标 (下什么单、是否追价/撤单)。
  *
  * 与"何时对冲、拉到多少"(策略层算 target/band) 解耦——新增执行方式只需新增实现 (开放封闭)，
  * 策略代码不变。每个实现自管在途单生命周期；onEvent 单线程调用，内部可变状态无需同步。
  */
trait HedgeExecution:
  /** @param req     None = 已在带内 (只需清理在途单)；Some = 需把净 delta 拉到目标
    * @param pending 当前在途对冲单 (至多一张)
    * @param now     当前市场虚拟时间 (ms)
    */
  def apply(req: Option[HedgeRequest], pending: Option[PendingOrder], now: Timestamp): Vector[OutcomeEvent]

/** 单张在途单的执行器骨架：共享"撤单去重 + 已确认才撤"逻辑。 */
abstract class SingleOrderHedgeExecution(exchange: Exchange, symbol: Symbol) extends HedgeExecution:
  /** 已发出撤单、尚未确认移除的订单，防止重复撤单 */
  protected val cancelling = mutable.Set.empty[OrderId]

  /** 同步在途状态：清掉已不在挂单簿里的待撤 id */
  protected def prune(pending: Option[PendingOrder]): Unit =
    cancelling.filterInPlace(id => pending.exists(_.order.id == id))

  /** 撤掉已确认挂单 (Created 无 orderId 不能撤，等确认)；防止重复撤单 */
  protected def cancelConfirmed(pending: PendingOrder): Option[OutcomeEvent] =
    if pending.status.isConfirmed && !cancelling.contains(pending.order.id) then
      cancelling += pending.order.id
      Some(OutcomeEvent.CancelOrder(exchange, symbol, pending.order.id))
    else None

  protected def order(side: Side, qty: Quantity, orderType: OrderType): Order =
    Order(id = "", exchange = exchange, symbol = symbol, side = side, orderType = orderType, quantity = qty, reduceOnly = false, clientOrderId = "")

/** **市价 (taker)** 对冲：越带即下市价单把净 delta 拉到目标，立即成交、无需追价。
  *
  * 同一时刻至多一张在途单 (等成交回报期间不重复下)。需行情提供 BBO (市价撮合取对手价)——
  * trade-native 回测须用 [[hft.backtest.TradeBboAugmentSource]] 附加零价差 BBO。
  */
final class MarketHedgeExecution(
    exchange: Exchange,
    symbol: Symbol,
    minHedgeQty: Quantity = 0.001,
) extends SingleOrderHedgeExecution(exchange, symbol):

  def apply(req: Option[HedgeRequest], pending: Option[PendingOrder], now: Timestamp): Vector[OutcomeEvent] =
    prune(pending)
    req match
      case None => pending.flatMap(cancelConfirmed).toVector // 带内: 撤残留 (市价一般无残留)
      case Some(HedgeRequest(side, qty, refPrice)) =>
        if pending.isDefined then Vector.empty // 在途市价单等回报中, 不重复下
        else if qty < minHedgeQty then Vector.empty
        else
          Vector(
            OutcomeEvent.PlaceOrders(
              Vector(order(side, qty, OrderType.Market)),
              f"market_hedge | $side%s qty=$qty%.4f px≈$refPrice%.4f",
            )
          )

/** **限价追价** 对冲：以最新成交价挂 PostOnly 限价 (需买挂买、需卖挂卖)，靠后续逐笔击穿成交；
  * [[repegMs]] (虚拟时间) 内未成交或方向翻转则撤单，下一周期按新价重挂 (追价)。同一时刻至多一张在途单。
  */
final class LimitRepegHedgeExecution(
    exchange: Exchange,
    symbol: Symbol,
    repegMs: Long = 3000,
    minHedgeQty: Quantity = 0.001,
) extends SingleOrderHedgeExecution(exchange, symbol):

  /** 当前在途单的挂出**虚拟时间** (ms)；用于 repeg 超时判定 (createdAt 是墙钟、回测跑得比实时快，不可用)。 */
  private var orderPlacedTs: Option[Timestamp] = None

  def apply(req: Option[HedgeRequest], pending: Option[PendingOrder], now: Timestamp): Vector[OutcomeEvent] =
    prune(pending)
    if pending.isEmpty then orderPlacedTs = None // 在途单已离场, 清空计时
    req match
      case None => pending.flatMap(cancelConfirmed).toVector
      case Some(HedgeRequest(side, qty, refPrice)) =>
        pending match
          case Some(p) =>
            if orderPlacedTs.isEmpty then orderPlacedTs = Some(now) // 防御: 在途单无计时则采用当前虚拟时间
            val sideChanged = p.order.side != side
            val stale = orderPlacedTs.exists(ts => now - ts >= repegMs)
            if sideChanged || stale then cancelConfirmed(p).toVector else Vector.empty
          case None =>
            if qty < minHedgeQty then Vector.empty
            else
              orderPlacedTs = Some(now)
              Vector(
                OutcomeEvent.PlaceOrders(
                  Vector(order(side, qty, OrderType.Limit(refPrice, TimeInForce.PostOnly))),
                  f"limit_hedge | $side%s qty=$qty%.4f px=$refPrice%.4f",
                )
              )
