package hft.exchange

import hft.actor.{Actor, ActorContext}
import hft.domain.*
import hft.event.Commands.{AccountSync, AccountSyncReport, AccountSyncRequest, AccountSynced, OrderIntent, OutcomeEvent}
import hft.event.{AnyEvent, Event, Interest, Topics}
import org.slf4j.LoggerFactory

/** 柜台插件：**一个账户在一个交易所上的执行与汇报**。
  *
  * 它是"交易所侧知识"的唯一归属地 —— 精度、张数、最小下单量、账户当下什么样，
  * 都在这里，别处不需要知道。策略这一侧从头到尾只有币本位的意图。
  *
  * ## 五项职责
  *
  *   1. **执行**：接下单指令，对齐到交易所精度后下发。
  *   2. **精度裁决**：对齐后交易所收不下的单 (低于最小下单量、被取整成零)，以**拒单回报**
  *      回流 —— 与"交易所明确拒绝"完全同一条路径，因此策略侧的挂单登记得到统一清理，
  *      不需要第二套机制。
  *   3. **汇报**：把私有推送解析成回报事件 (见 [[AccountFeed]])，并**维护仓位**。
  *   4. **对齐**：接对齐指令，把当前持仓 / 净值 / 挂单推到总线，完成后发应答。
  *   5. **净值刷新**：净值随行情持续变动却没有推送，只能周期拉取 —— 这是柜台自己的
  *      常驻职责，不需要外部指令。
  *
  * ## 真假同形
  *
  * 虚拟柜台与真实柜台在总线上完全一样：订同样的指令、产出同样的回报。策略无法分辨，
  * 这正是"同一份逻辑同时跑实盘与影子盘"的结构基础。
  *
  * ## 仓位由柜台算，回报有固定顺序
  *
  * 仓位是**柜台的账本**，不是策略自己累加出来的：初值来自启动对齐 (交易所真实持仓)，
  * 之后每笔成交进 [[Ledger]]。策略只管读 [[Topics.Position]]。
  *
  * 从前这份账在策略侧 —— 每个策略实例各累加一份，而交易所持续推来的权威仓位被全部丢弃
  * (只认第一条)，于是本地账一旦漂移就永远发现不了。搬到柜台之后，"我累加的"与
  * "交易所报的"第一次落在同一个角色手里，对账才成为可能 (见 [[reconcile]])。
  *
  * **一笔成交产出三条回报，顺序固定**：
  * {{{
  *   仓位快照 (Position) -> 成交 (Fill) -> 订单终态 (OrderUpdate)
  * }}}
  * 仓位排第一是硬要求：策略普遍在成交回调里读 `ctx.state.positionSize` 决策，
  * 顺序反了它读到的就是成交**前**的仓位 —— 对冲量从此一直差一笔，而没有任何症状。
  * 三种柜台 (真实 / 替身 / 影子) 与回测都遵守这一条。
  *
  * ## 没有凭证就不装柜台
  *
  * "这个交易所能不能下单"因此是**装配期的事实** (装没装柜台)，由引擎的指令面校验回答，
  * 不是运行时才发现的错误 —— 从前那是一条在调用链上传递的 `Auth` 错误值。
  */
abstract class TradingGateway extends Actor:
  private val gatewayLogger = LoggerFactory.getLogger(classOf[TradingGateway])

  /** 本柜台所在的交易所 */
  def exchange: Exchange

  /** 本柜台服务的账户。真实柜台是实盘账户，虚拟柜台是影子账户 —— 两边的回报靠这个维度分开。
    *
    * 无默认值是有意的：装配处若留个"实盘"的默认值，给影子柜台装配时忘传账户，
    * 它就会去接实盘的下单指令，而编译器不会吭声。
    */
  def account: AccountId

  /** 指令的路由键 —— 下单与对齐都只投给"这个账户在这个所"的柜台 */
  final def target: AccountExchange = AccountExchange(account, exchange)

  override def name: String = s"trading-gateway@$target"

  /** 净值刷新间隔。风控 (杠杆闸门) 直接拿净值决策，所以它的**新鲜度本身是正确性问题**：
    * 读到的净值最多滞后一个周期，临界阈值应自留余量。 */
  protected def accountRefreshMs: Long = 10_000

  /** 由框架在 [[onStart]] 注入，之后只读 */
  @volatile private var ctx: ActorContext = scala.compiletime.uninitialized

  final override def interests: Set[Interest] = Set(
    Interest.Keyed(OrderIntent, Set(target)),
    Interest.Keyed(AccountSync, Set(target)),
  ) ++ extraInterests

  /** 柜台自身还要收的事件 —— 新增能力靠新增声明, 不必改基类。
    *
    * 虚拟柜台用它订阅行情 (撮合的输入就是行情)。真实柜台不需要：它的撮合在交易所那边。
    */
  protected def extraInterests: Set[Interest] = Set.empty

  final override def onStart(context: ActorContext): Unit =
    ctx = context
    connect()
    ctx.fork {
      while !ctx.sleepUnlessStopped(accountRefreshMs) do
        ctx.publish(Event.local(Topics.AccountInfo, currentAccountInfo()))
    }

  final override def onEvent(event: AnyEvent, now: Timestamp): Vector[AnyEvent] =
    event.as(OrderIntent).foreach(intent => execute(intent.outcome, now))
    // 对齐产生的事件由本方法返回、框架统一发布 —— 顺序因此是确定的:
    // 持仓 -> 净值 -> 挂单 -> 完成应答, 而应答必然排在它们之后。
    event.as(AccountSync).map(runSync).getOrElse(Vector.empty) ++ onOther(event, now)

  /** 处理 [[extraInterests]] 声明的那些事件 */
  protected def onOther(event: AnyEvent, now: Timestamp): Vector[AnyEvent] = Vector.empty

  // ==================== 执行 ====================

  private def execute(outcome: OutcomeEvent, now: Timestamp): Unit = outcome match
    case OutcomeEvent.PlaceOrders(orders, comment) =>
      // 关联订单独立并行下单：IOC 订单本身接受部分成交，敞口由策略层 rebalance 兜底
      orders.foreach { order =>
        OrderConversion.alignToExchange(order, metaOf(order.symbol)) match
          case Right(aligned) =>
            gatewayLogger.info(s"下单: ${describe(aligned)} signal=$comment")
            placeAligned(aligned, now)
          case Left(reason) =>
            // 交易所收不下 —— 走与"交易所明确拒绝"同一条回流路径, 策略侧的 pending 登记
            // 因此被统一清理。分成两套机制的话, 其中一套迟早会漏掉某种拒绝形态。
            gatewayLogger.warn(s"下单被交易所精度拒绝: $reason")
            reject(order, reason, now)
      }
    case OutcomeEvent.CancelOrder(_, symbol, ref) =>
      gatewayLogger.info(s"撤单: $exchange $symbol ${ref.raw}")
      cancelOrder(symbol, ref, now)

  /** 确定性的下单失败以 `OrderUpdate(Error)` 回流，驱动策略侧 pending order 的清理。
    *
    * 子类在收到交易所的 4xx 拒绝时也调它 —— 两种拒绝对策略是同一件事："这张单确定没成立"。
    */
  protected final def reject(order: Order, reason: String, now: Timestamp): Unit =
    ctx.publish(TradingGateway.rejection(account, exchange, order, reason, now))

  // ==================== 对齐 ====================

  /** 把账户当下的样子推到总线，最后发完成应答。
    *
    * 在 actor 线程上**同步**跑完：引擎正阻塞等这条应答 (策略要在有初始仓位之后才看到
    * 第一条行情)，异步化只会让"等什么"变得不可言说。此刻还没有订单在流动，不存在阻塞代价。
    *
    * 任何一步失败都抛异常终止进程 —— 账户状态没对上就开始交易，比不启动危险得多。
    */
  private def runSync(request: AccountSyncRequest): Vector[AnyEvent] =
    gatewayLogger.info(s"启动对齐 $target: ${request.symbols.mkString(",")} (req=${request.requestId})")
    TradingGateway.syncEvents(
      request,
      exchange,
      positions = syncPositions(request.symbols),
      accountInfo = currentAccountInfo(),
      pendingOrders = syncPendingOrders(request.symbols),
    )

  // ==================== 子类实现 ====================

  /** 建立私有连接、fork 常驻线程。此时 [[publish]] / [[fork]] 已可用 */
  protected def connect(): Unit

  /** 本所合约规格 —— 精度对齐与张数换算的依据。
    *
    * 缺失即发单方引用了本所没有的标的，是装配错误，实现方应立即终止而不是跳过：
    * 跳过的表现是这张单凭空消失。
    */
  protected def metaOf(symbol: Symbol): SymbolMeta

  /** 下发一张**已对齐到交易所精度**的订单 (仍是币本位；换成张数是实现方的最后一步)。
    *
    * `now` 是本次事件的处理时刻 —— 虚拟柜台拿它做撮合时间, 不读墙钟。
    */
  protected def placeAligned(order: Order, now: Timestamp): Unit

  protected def cancelOrder(symbol: Symbol, ref: OrderRef, now: Timestamp): Unit

  /** 查这些标的当前的持仓。没返回的标的由基类补零仓，实现方不必凑齐 */
  protected def syncPositions(symbols: Set[Symbol]): Vector[Position]

  /** 查这些标的当前的挂单 */
  protected def syncPendingOrders(symbols: Set[Symbol]): Vector[OrderUpdate]

  /** 当前账户净值与名义价值。失败即抛 —— 风控拿它决策，读不到就不该继续跑 */
  protected def currentAccountInfo(): AccountInfo

  // ==================== 子类可用的能力 ====================

  /** 把一条回报发布到总线 (从私有流线程 / REST 回调线程调用) */
  protected final def publish(event: AnyEvent): Unit = ctx.publish(event)

  /** 在本插件的作用域内 fork 一条线程 (常驻私有流循环, 或一次性的 REST 调用) */
  protected final def fork(body: => Unit): Unit = ctx.fork(body)

  /** 延迟 `delayMs` 后把一条事件发到总线 —— 虚拟柜台的下单在途与回报回传延迟靠它。
    *
    * 定时器**只负责把发布推迟到点，不触碰任何状态**：事件到点后经总线回到柜台的邮箱，
    * 仍由 actor 线程串行处理。因此柜台的状态依旧只有一个写者。
    */
  protected final def schedule(delayMs: Long, event: AnyEvent): Unit = ctx.scheduleEvent(delayMs, event)

  /** 本插件所在的并发作用域 —— 只有需要装嵌套组件的柜台 (虚拟柜台) 才用得着 */
  protected final def scope: ox.Ox = ctx.scope

  private def describe(order: Order): String =
    s"$exchange ${order.symbol} ${order.side} ${order.orderType} qty=${order.quantity} " +
      s"reduceOnly=${order.reduceOnly} clientOrderId=${order.clientOrderId}"

object TradingGateway:
  /** 仓位快照事件 —— 真假柜台同一份构造。
    *
    * **不含未实现盈亏**（恒置 0）：那要估值价，而真实柜台不订阅行情、算不了。两边都留 0
    * 是有意的 —— 一边有值一边没有，策略读到的东西就随部署形态而变。要盈亏读
    * [[Topics.AccountInfo]] 的净值，那是柜台确实算得出的。
    */
  def positionEvent(position: Position, now: Timestamp): AnyEvent =
    Event.stamped(Topics.Position, position.copy(unrealizedPnl = 0.0), now, now)

  /** 把一次对齐的结果组装成事件序列 —— **真假柜台同一份**。
    *
    * 顺序是它的全部意义: 持仓 -> 净值 -> 既有挂单 -> 完成应答。应答必须排在最后，
    * 引擎见到它就放行行情；排错了就等于"对齐没做完却已经开始交易"。
    *
    * 交易所没返回的标的**显式推零仓**：策略的状态机在收到初始值之前不该动作，
    * 而"没有推送"与"仓位为零"在它看来无从分辨 —— 它会一直等下去。
    */
  def syncEvents(
      request: AccountSyncRequest,
      exchange: Exchange,
      positions: Vector[Position],
      accountInfo: AccountInfo,
      pendingOrders: Vector[OrderUpdate],
  ): Vector[AnyEvent] =
    val account = request.account
    val bySymbol = positions.map(p => p.symbol -> p).toMap
    val positionEvents = request.symbols.toVector.sortBy(_.toString).map { symbol =>
      positionEvent(bySymbol.getOrElse(symbol, Position.empty(account, exchange, symbol)), nowMs)
    }
    val orderEvents = pendingOrders.map(Event.local(Topics.OrderUpdate, _))
    val report = Event.local(AccountSynced, AccountSyncReport(account, exchange, request.requestId))
    (positionEvents :+ Event.local(Topics.AccountInfo, accountInfo)) ++ orderEvents :+ report

  /** "这张单确定没成立"的回报 —— 精度拒绝与交易所拒绝共用一种形态。
    *
    * 策略侧的挂单登记靠它清理。做成一处：两种拒绝各造一条回报的话，
    * 其中一条迟早会漏掉某个字段，而漏掉 clientOrderId 就等于那条登记永远清不掉。
    *
    * `now` 是**本次处理时刻**而不是墙钟：回测在同一条路径上产出拒单，读墙钟会让
    * 事件时间戳跨运行不可复现 —— 观察者导出的记录、策略读 timestamp 的任何逻辑都跟着漂。
    */
  def rejection(account: AccountId, exchange: Exchange, order: Order, reason: String, now: Timestamp): AnyEvent =
    Event.stamped(
      Topics.OrderUpdate,
      OrderUpdate(
        account = account,
        orderId = "",
        clientOrderId = Some(order.clientOrderId),
        exchange = exchange,
        symbol = order.symbol,
        side = order.side,
        status = OrderStatus.Error(reason),
        price = Price.Zero,
        quantity = Coin.Zero,
        filledQuantity = Coin.Zero,
        fillSize = Coin.Zero,
        timestamp = now,
      ),
      now,
      now,
    )

/** 账户私有推送流 —— 柜台的"汇报"面。
  *
  * 与柜台拆开是因为它们的**变化原因不同**：执行与对齐是 REST 的形状 (请求-响应)，
  * 汇报是长连接的形状 (解析推送)。三家交易所的前者几乎一样、后者各不相同。
  *
  * 不接总线类型而接两个函数：本 trait 因此不知道总线的存在，测试里给它一个收集器即可。
  */
trait AccountFeed:
  def exchange: Exchange

  /** 建立连接并开始推送。
    *
    * @param account 回报归属的账户 —— 由柜台传入，本流自己不持有。
    *                账户是装配期的事实，只该有一处出处；流与柜台各存一份的失效方式是
    *                两者不一致时回报进了别的账户，而没有任何症状
    * @param publish 把解析出的回报发到总线
    * @param fork    在柜台插件的作用域内起一条常驻线程 (抛出的异常级联终止引擎)
    */
  def connect(account: AccountId, publish: AnyEvent => Unit, fork: (=> Unit) => Unit): Unit
