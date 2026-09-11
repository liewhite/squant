package hft.state

import hft.domain.*
import hft.event.{AnyEvent, Topics}
import org.slf4j.LoggerFactory

import scala.collection.mutable

/** 待处理订单信息 (完整订单 + 运行时状态) */
final case class PendingOrder(
    order: Order,
    status: OrderStatus,
    createdAt: Timestamp,
)

/** **一个标的** (交易所 + 交易对) 的状态。可变，仅在所属 Executor 的虚拟线程内访问，无需同步。
  *
  * ## 为什么按标的而不是按交易对
  *
  * 从前这里是 `SymbolState`：一个交易对聚合它在**所有交易所**上的状态，每个读数都是
  * `Map[Exchange, _]`，取值要写 `bbo(exchange)`。那个形状有两处代价：
  *
  *   - **读错交易所只能靠运行时守卫挡**。`positionSize(exchange)` 得先 `require` 该交易所
  *     被声明过 —— 因为"没声明因而从未对齐"与"已对齐的空仓"在那个形状里都是 0，分不出来。
  *     按标的索引之后这道守卫不需要了：没订阅的标的压根取不到状态 (`instrumentState` 给
  *     `None`)，"问错了地方"从运行时错误变成了拿不到东西。
  *   - **聚合维度容纳不了同一交易对的多个合约**。同一个 `ETH` 上有几十个期权合约、还有
  *     币本位与 U 本位两种永续 —— 它们是不同的标的，却会挤进同一个 `SymbolState`，
  *     各自的盘口和仓位互相覆盖。
  *
  * 跨所比价的策略并没有因此变麻烦：它本来就持有一组 `Instrument` (每条腿一个)，现在逐个
  * 问状态即可，不必再把 `symbol` 与 `exchange` 拆开传。
  */
final class InstrumentState(val instrument: Instrument) extends InstrumentView:
  private val logger = LoggerFactory.getLogger(classOf[InstrumentState])

  def exchange: Exchange = instrument.exchange
  def symbol: Symbol = instrument.symbol

  private var _fundingRate: Option[FundingRate] = None
  private var _bbo: Option[BBO] = None
  private var _markPrice: Option[MarkPrice] = None
  private var _indexPrice: Option[IndexPrice] = None
  private var _position: Option[Position] = None
  /** 待处理订单 (以 clientOrderId 为 key) */
  private val _pendingOrders: mutable.Map[String, PendingOrder] = mutable.Map.empty

  // ==================== 查询 ====================

  def bbo: Option[BBO] = _bbo
  def markPrice: Option[MarkPrice] = _markPrice
  def indexPrice: Option[IndexPrice] = _indexPrice
  def fundingRate: Option[FundingRate] = _fundingRate
  def position: Option[Position] = _position

  /** 仓位大小。**"没有记录 = 空仓"**，这一条有依据，不是兜底：
    *
    *   - 实盘/影子：柜台在启动对齐时为每个已声明标的**显式推一条零仓快照**
    *     (见 `TradingGateway.syncEvents`)，而策略在对齐落地之前根本不动作
    *     (见 `Executor.awaiting` 的闸门)。所以策略能看到的第一个决策时刻，快照必已到达。
    *   - 回测 / `Executor.readyToTrade`：账户按构造从零开始，"没有仓位"就是事实。
    *
    * 换句话说，这里不存在"其实有仓位但记录还没到"的窗口 —— 那个窗口被闸门挡住了。
    * 而"问了一个没订阅的标的"这种情形也不会落到这里：那时 `instrumentState` 给的是 `None`，
    * 根本拿不到本对象 (从前按交易对索引时办不到这一点，只能在这里 `require` 一道)。
    *
    * 需要区分"还没有任何快照"与"仓位为零"的调用方 (诊断、监控) 用 [[position]]，它给 `Option`。 */
  def positionSize: Coin = _position.map(_.size).getOrElse(Coin.Zero)

  def hasPendingOrders: Boolean = _pendingOrders.nonEmpty

  def hasPendingSide(side: Side): Boolean =
    _pendingOrders.values.exists(_.order.side == side)

  def pendingOrders: Iterable[PendingOrder] = _pendingOrders.values

  /** 这张单的策略标注 (见 [[hft.domain.Order.tag]])。
    *
    * 空标注与查不到都给 `None`：两者在调用方那里是同一件事 ——"按标注分派"这一支走不了。
    * 详见 [[hft.strategy.StrategyContext.orderTag]]。
    */
  private[state] def tagOf(clientOrderId: String): Option[String] =
    _pendingOrders.get(clientOrderId).map(_.order.tag).filter(_.nonEmpty)

  // ==================== 订单管理 ====================

  /** 添加待处理订单 (发送订单信号时调用) */
  def addPendingOrder(order: Order, createdAt: Timestamp): Unit =
    _pendingOrders(order.clientOrderId) = PendingOrder(order, OrderStatus.Created, createdAt)

  /** 移除指定的待处理订单 */
  def removePendingOrder(clientOrderId: String): Unit =
    _pendingOrders.remove(clientOrderId)

  /** 校验不存在超时未确认的订单，违反即抛错终止。
    *
    * Created 状态超过 timeoutMs：REST 已设置更短的超时，正常情况下下单要么明确成功
    * (私有流推送确认) 要么明确失败 (Error 事件清理 pending)，走到这里说明订单结果
    * **不确定**——清理后重下会造成敞口翻倍，唯一安全的做法是终止，由重启后的启动对齐恢复。
    * 已确认挂单 (Pending/PartiallyFilled) 由策略决定何时撤单，不参与校验。
    */
  def failOnTimedOutOrders(now: Timestamp, timeoutMs: Long): Unit =
    // 0 = 关闭（回测由确定性队列驱动，不存在网络造成的“结果不确定”）；
    // 实盘超时由 Executor 固定注入，策略不能关闭或改写。
    if timeoutMs > 0 then
      _pendingOrders.find((_, p) => p.status == OrderStatus.Created && now - p.createdAt > timeoutMs).foreach {
        (clientId, p) =>
          sys.error(
            s"[$instrument] order unconfirmed after ${now - p.createdAt}ms (timeout=${timeoutMs}ms), outcome UNKNOWN: " +
              s"clientOrderId=$clientId"
          )
      }

  // ==================== 事件处理 ====================

  /** 按 topic 更新状态。
    *
    * 事件已由总线按标的精确投递、再由 [[StateManager]] 按标的定位到本实例，
    * 故这里不再重复校验归属 —— 路由键就是从载荷派生的，不存在错配的可能。
    */
  def apply(event: AnyEvent): Unit =
    event.as(Topics.FundingRate).foreach(r => _fundingRate = Some(r))
    event.as(Topics.Bbo).foreach(b => _bbo = Some(b))
    event.as(Topics.MarkPrice).foreach(m => _markPrice = Some(m))
    event.as(Topics.IndexPrice).foreach(i => _indexPrice = Some(i))
    event.as(Topics.Position).foreach(applyPosition)
    event.as(Topics.OrderUpdate).foreach(applyOrderUpdate)

  /** 仓位由**柜台**维护，本地只是接住它的快照。
    *
    * 从前这里只认第一条 (初始加载)，之后完全靠 Fill 自己累加 —— 那时仓位在策略侧算，
    * 交易所持续推来的权威读数被全部丢弃，本地账一旦漂移就永远发现不了。现在账本在柜台
    * (见 [[hft.exchange.TradingGateway]])，它发的每一条都比上一条新，直接覆盖即可：
    * 同一个柜台的事件在总线上是 FIFO，不存在"后到的更旧"。
    */
  private def applyPosition(position: Position): Unit =
    _position = Some(position)

  private def applyOrderUpdate(update: OrderUpdate): Unit =
    logger.info(
      s"[$instrument] order status: orderId=${update.orderId} " +
        s"clientOrderId=${update.clientOrderId} status=${update.status}"
    )
    // 用 clientOrderId 跟踪订单；**没有 clientOrderId 就不是我们发起的单**。
    //
    // 这条依据现在两条路径一致: WS 推送与 REST 挂单查询都只在交易所真的带了 clOrdId/orderLinkId
    // 时给出 Some (适配层不再拿 ordId 冒充)。而 OKX 文档写明 clOrdId "will be included in the
    // response if provided in the request" —— 手工单、交易所生成的 TP/SL 都没有它。
    // 别人的单不该进本策略的挂单登记: 那会让 hasPendingOrders 恒真、槽位判断错乱。
    update.clientOrderId.foreach { clientId =>
      if update.status.isTerminal then _pendingOrders.remove(clientId)
      else if update.status.isConfirmed then
        _pendingOrders.get(clientId) match
          case Some(pending) =>
            // 交易所已确认，更新状态并回填 orderId
            val order =
              if pending.order.id.isEmpty then pending.order.copy(id = update.orderId)
              else pending.order
            _pendingOrders(clientId) = pending.copy(order = order, status = update.status)
          case None =>
            // 启动对齐时从交易所接管的既有挂单 (上一个进程留下的自己的单)。
            //
            // `reduceOnly` 取回报里的真值, 不再本地填 false —— 策略拿它给 resting 单分槽
            // (止盈槽 vs 加仓槽), 填错会让重启后真正的止盈单被归进加仓槽, 于是再挂一张,
            // 而那张真的止盈单还在簿上。
            //
            // TIF 记为 GTC 有依据而不是默认值: **还在簿上 resting 的限价单必然是 GTC 语义** ——
            // IOC/FOK 从不 resting, 而 PostOnly 只是下单时刻的约束, 对一张已经挂上的单
            // 不再有行为差别。
            //
            // `tag` 留空是**事实**而不是缺省: 策略的标注只活在下单那个进程的内存里, 不发给
            // 交易所 (见 Order.tag), 所以接管一张上个进程留下的单时它确实无从知道。策略若按
            // 标注分派, 必须能处理"没有标注"这一支 —— 接管单一直是这样, 与本字段无关。
            // 标的取本状态所属的那个 —— 这张单就挂在它上面。经 Order.on 派生而不是把
            // update 的 exchange/symbol 拆着填: 后者会把品种丢在默认值上, 于是非永续标的
            // 上接管的单带着 LinearPerp 进登记, 停机撤单时 ctx.cancel 按 order.instrument
            // 找不到状态, 直接 require 崩在"只能撤销本策略已登记的挂单"上。
            val order = Order.on(
              instrument = instrument,
              side = update.side,
              orderType = OrderType.Limit(update.price, TimeInForce.GTC),
              quantity = update.quantity,
              reduceOnly = update.reduceOnly,
              clientOrderId = clientId,
              id = update.orderId,
            )
            _pendingOrders(clientId) = PendingOrder(order, update.status, update.timestamp)
    }
