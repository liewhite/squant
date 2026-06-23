package strategy.bbomaker.logic

import hft.strategy.{OutcomeEvent, Strategy}

import hft.domain.*
import hft.exchange.SubscriptionKind
import hft.messaging.{EventData, IncomeEvent, PendingOrder, StateManager, SymbolState}

import scala.collection.mutable

/** BBO 外被动做市策略。
  *
  * 行为：
  *   - 双边挂单: 买单挂在 bid 下方 offsetRatio，卖单挂在 ask 上方 offsetRatio (PostOnly 保证只做 maker)
  *   - 每边最多一张挂单，成交/撤销后在下一个行情事件重新挂出
  *   - 价格维护: 已确认挂单偏离目标价超过 repriceToleranceRatio 即撤单，撤销确认后重挂
  *   - 风控: 假设本单全部成交后的杠杆率 (|仓位| * 标记价格 / 账户净值) 必须低于
  *     maxLeverage，否则该方向不挂单——仓位只能向降杠杆方向变化
  *
  * 节奏由订单生命周期自然限速：挂单后等确认、撤单后等移除，每边同一时刻最多
  * 一个在途动作，不会向交易所倾泻请求。
  *
  * 依赖账户净值 (AccountInfoUpdate，由 Engine 启动对齐 + 周期刷新提供)，
  * 净值未知或非正时不挂任何单 (安全侧)。
  */
final class BboMakerStrategy(
    targetExchange: Exchange,
    symbol: Symbol,
    /** 挂单距 BBO 的偏移比例，0.0001 = 0.01% */
    offsetRatio: Double = 0.0001,
    /** 每张订单数量 (币本位) */
    orderSize: Quantity = 0.002,
    /** 杠杆率上限: |仓位| * 标记价格 / 账户净值 */
    maxLeverage: Double = 2.0,
    /** 挂单价偏离目标价超过该比例则撤单重挂 (默认为偏移量的一半) */
    repriceToleranceRatio: Double = 0.00005,
) extends Strategy:

  /** 已发出撤单、尚未确认移除的订单，防止重复撤单。单线程 (Executor) 访问，无需同步 */
  private val cancelling = mutable.Set.empty[OrderId]

  override def publicStreams: Map[Exchange, Set[SubscriptionKind]] =
    Map(targetExchange -> Set(SubscriptionKind.BBO(symbol), SubscriptionKind.MarkPrice(symbol)))

  override def orderTimeoutMs: Long = 5000

  override def onEvent(event: IncomeEvent, state: StateManager): Vector[OutcomeEvent] =
    event.data match
      case EventData.BboUpdate(bbo) if bbo.exchange == targetExchange && bbo.symbol == symbol =>
        quote(bbo, state)
      case _ => Vector.empty

  private def quote(bbo: BBO, state: StateManager): Vector[OutcomeEvent] =
    (for
      symbolState <- state.symbolState(symbol)
      account <- state.accountInfo(targetExchange)
      if account.equity > 0
    yield
      // 清理已确认移除的撤单记录
      cancelling.filterInPlace(id => symbolState.pendingOrders.exists(_.order.id == id))
      // 仓位估值用标记价格 (交易所保证金口径)，未到达时退化为 BBO 中间价
      val markPrice = symbolState.markPrice(targetExchange).map(_.price).getOrElse(bbo.midPrice)
      val position = symbolState.positionSize(targetExchange)
      Vector(
        quoteSide(Side.Long, bbo.bidPrice * (1 - offsetRatio), symbolState, position, markPrice, account.equity),
        quoteSide(Side.Short, bbo.askPrice * (1 + offsetRatio), symbolState, position, markPrice, account.equity),
      ).flatten
    ).getOrElse(Vector.empty)

  /** 单边报价决策: 有挂单则维护价格，无挂单则在风控允许时挂出 */
  private def quoteSide(
      side: Side,
      targetPrice: Price,
      symbolState: SymbolState,
      position: Quantity,
      markPrice: Price,
      equity: Double,
  ): Option[OutcomeEvent] =
    symbolState.pendingOrders.find(_.order.side == side) match
      case Some(pending) => maybeReprice(pending, targetPrice)
      case None          => maybePlace(side, targetPrice, position, markPrice, equity)

  /** 已确认挂单偏离目标价过远则撤单 (Created 状态无交易所 orderId，等确认后再处理) */
  private def maybeReprice(pending: PendingOrder, targetPrice: Price): Option[OutcomeEvent] =
    pending.order.orderType match
      case OrderType.Limit(restingPrice, _)
          if pending.status.isConfirmed
            && !cancelling.contains(pending.order.id)
            && math.abs(restingPrice - targetPrice) / targetPrice > repriceToleranceRatio =>
        cancelling += pending.order.id
        Some(OutcomeEvent.CancelOrder(targetExchange, symbol, pending.order.id))
      case _ => None

  /** 假设本单全部成交后的杠杆率仍低于上限才挂单 */
  private def maybePlace(
      side: Side,
      targetPrice: Price,
      position: Quantity,
      markPrice: Price,
      equity: Double,
  ): Option[OutcomeEvent] =
    val positionAfterFill = side match
      case Side.Long  => position + orderSize
      case Side.Short => position - orderSize
    val leverageAfterFill = math.abs(positionAfterFill) * markPrice / equity
    if leverageAfterFill >= maxLeverage then None
    else
      val order = Order(
        id = "",
        exchange = targetExchange,
        symbol = symbol,
        side = side,
        orderType = OrderType.Limit(targetPrice, TimeInForce.PostOnly),
        quantity = orderSize,
        reduceOnly = false,
        clientOrderId = "", // 由 Executor 生成
      )
      Some(
        OutcomeEvent.PlaceOrders(
          Vector(order),
          f"bbo_maker | $side%s target=$targetPrice%.2f levAfterFill=$leverageAfterFill%.2f",
        )
      )
