package hft.exchange

import hft.actor.{Actor, ActorContext, ActorSystem}
import hft.domain.*
import hft.event.Commands.{AccountSync, AccountSyncReport, AccountSyncRequest, AccountSynced, OrderIntent, OutcomeEvent}
import hft.event.{AnyEvent, CommandHandler, Event, EventBus, Interest, Topics}
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
  * **契约只有一条：[[Topics.Position]] 先于同一笔成交引发的 [[Topics.OrderUpdate]]。**
  *
  * 这条不是为了"读到最新的数"，是为了消灭一个会导致重复下单的中间状态：订单终态到达时，
  * 策略本地的挂单登记会被移除；此刻仓位若还没更新，策略看到的是"既没有挂单、仓位也不够"
  * —— 于是**再下一单**。反过来的中间状态 (仓位先增、挂单还在) 是保守的。
  * 顺序不是风格问题，是危险侧朝哪边的问题。
  *
  * 它是**构造出来的**：仓位与订单状态出自同一条订单回报，柜台记完账先发仓位再发状态，
  * 不依赖任何交易所的推送次序。三种柜台与回测都如此。
  *
  * **本契约不承诺 [[Topics.Fill]] 的位置。** 成交明细走的是另一条渠道 (Bybit 干脆是
  * 另一条频道)，交易所本身不保证它与订单回报的先后。虚拟柜台上它紧跟在仓位之后，
  * 真实柜台上则视交易所而定 —— 所以**不要在成交回调里读仓位**。要对仓位变化做反应，
  * 订 [[Topics.Position]]，那是唯一有承诺的读取点。
  *
  * ## 没有凭证就不装柜台
  *
  * "这个交易所能不能下单"因此是**装配期的事实** (装没装柜台)，由内核的命令能力校验回答，
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

  final override def interests: Set[Interest] = extraInterests

  final override def commandHandlers: Set[CommandHandler] = Set(
    CommandHandler.command(OrderIntent, target),
    CommandHandler.command(AccountSync, target),
  ) ++ extraCommandHandlers

  /** 柜台自身还要收的事件 —— 新增能力靠新增声明, 不必改基类。
    *
    * 虚拟柜台用它订阅行情 (撮合的输入就是行情)。真实柜台不需要：它的撮合在交易所那边。
    */
  protected def extraInterests: Set[Interest] = Set.empty

  /** 柜台额外承担的命令能力。与观察性 [[extraInterests]] 分开，避免观察者被当成执行者。 */
  protected def extraCommandHandlers: Set[CommandHandler] = Set.empty

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
    val snapshot = syncSnapshot(request.symbols)
    TradingGateway.syncEvents(
      request,
      exchange,
      positions = snapshot.positions,
      accountInfo = currentAccountInfo(),
      wallet = currentWallet(),
      pendingOrders = snapshot.pendingOrders,
    )

  // ==================== 子类实现 ====================

  /** 建立私有连接、fork 常驻线程。此时 [[publish]] / [[fork]] 已可用 */
  protected def connect(): Unit

  /** 账户当下的**完整**钱包 (币种 -> 余额)。对齐时发一次，见 [[TradingGateway.syncEvents]]。
    *
    * 返回全量: 未列出的币种余额就是 0。REST 的钱包接口本就一次返回整份 (两家的
    * `fetchAccountInfo` 拿的就是同一个响应, 只是从前把币种明细丢掉了)。 */
  protected def currentWallet(): Map[String, Double]

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

  /** 拉一份账户当下的样子：这些标的的持仓与挂单。没返回的标的由基类补零仓，实现方不必凑齐。
    *
    * **一次调用返回两者**，因为它们必须来自同一次读取：真实柜台要拉两趟 REST，中间夹进
    * 一笔成交就会让两份对不上 —— 账本没含它、记账进度却已含它，那笔成交从此永久漏记
    * (见 [[RestTradingGateway.syncSnapshot]] 的一致性重取)。
    *
    * 拆成两个回调的话，实现方只能用一个可变字段在两次调用之间传递快照，
    * 而"仓位先查、挂单后查"这条约定就只靠基类调用处的书写顺序维系 —— 那种约定迟早有人漏掉。
    * 一次原子读取是这个接口的形状本身该说清的事。
    */
  protected def syncSnapshot(symbols: Set[Symbol]): TradingGateway.AccountSnapshot

  /** 当前账户净值与名义价值。失败即抛 —— 风控拿它决策，读不到就不该继续跑 */
  protected def currentAccountInfo(): AccountInfo

  // ==================== 子类可用的能力 ====================

  /** 把一条回报发布到总线 (从私有流线程 / REST 回调线程调用) */
  protected final def publish(event: AnyEvent): Unit = ctx.publish(event)

  /** 在本插件的作用域内 fork 一条线程 (常驻私有流循环, 或一次性的 REST 调用) */
  protected final def fork(body: => Unit): Unit = ctx.fork(body)

  /** 协作式睡眠 —— 停机请求会立即唤醒并返回 true。转交给汇报面实现使用，见 [[AccountFeed.connect]]。 */
  protected final def sleepUnlessStopped(ms: Long): Boolean = ctx.sleepUnlessStopped(ms)

  /** 把连接、订阅或嵌套系统登记到本插件作用域 */
  protected final def manage[A](resource: A)(release: A => Unit): A = ctx.manage(resource)(release)

  /** 创建使用私有总线的子系统；其停机与失败自动链接到本柜台。 */
  protected final def childSystem(bus: EventBus): ActorSystem = ctx.childSystem(bus)

  /** 把一条外部线程来的输入排进自己的邮箱 —— **不经总线**，见 [[hft.actor.ActorContext.tell]] */
  protected final def tell(event: AnyEvent): Unit = ctx.tell(event)

  /** 延迟 `delayMs` 后把一条事件发到总线 —— 虚拟柜台的下单在途与回报回传延迟靠它。
    *
    * 定时器**只负责把发布推迟到点，不触碰任何状态**：事件到点后经总线回到柜台的邮箱，
    * 仍由 actor 线程串行处理。因此柜台的状态依旧只有一个写者。
    */
  protected final def schedule(delayMs: Long, event: AnyEvent): Unit = ctx.scheduleEvent(delayMs, event)

  private def describe(order: Order): String =
    s"$exchange ${order.symbol} ${order.side} ${order.orderType} qty=${order.quantity} " +
      s"reduceOnly=${order.reduceOnly} clientOrderId=${order.clientOrderId}"

object TradingGateway:
  /** 账户当下的样子 —— 持仓与挂单，**同一次读取的结果**。见 [[TradingGateway.syncSnapshot]] */
  final case class AccountSnapshot(positions: Vector[Position], pendingOrders: Vector[OrderUpdate])

  /** 仓位快照事件 —— 真假柜台同一份构造。
    *
    * 从前这里要把 `unrealizedPnl` 强制归零 (真实柜台不订阅行情、算不出它, 而一边有值
    * 一边没有会让策略读到的东西随部署形态而变)。现在 [[Position]] 干脆只有数量,
    * 这一步随之消失 —— **不需要归零的字段, 才是真的不会被谁读走**。
    */
  def positionEvent(position: Position, exchangeTs: Timestamp, localTs: Timestamp): AnyEvent =
    Event.stamped(Topics.Position, position, exchangeTs, localTs)

  /** 两个时间戳同源的简写 —— 本地产生的仓位快照 (对齐、虚拟柜台撮合) 用它 */
  def positionEvent(position: Position, now: Timestamp): AnyEvent = positionEvent(position, now, now)

  /** 把一次对齐的结果组装成事件序列 —— **真假柜台同一份**。
    *
    * 顺序是它的全部意义: 持仓 -> 净值 -> 钱包 -> 既有挂单 -> 完成应答。应答必须排在最后，
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
      wallet: Map[String, Double],
      pendingOrders: Vector[OrderUpdate],
  ): Vector[AnyEvent] =
    val account = request.account
    val bySymbol = positions.map(p => p.symbol -> p).toMap
    val positionEvents = request.symbols.toVector.sortBy(_.toString).map { symbol =>
      positionEvent(bySymbol.getOrElse(symbol, Position.empty(account, exchange, symbol)), nowMs)
    }
    val orderEvents = pendingOrders.map(Event.local(Topics.OrderUpdate, _))
    val report = Event.local(AccountSynced, AccountSyncReport(account, exchange, request.requestId))
    // 钱包快照是对齐的一部分, 和持仓同理: 缺了它, 策略分不清"某币余额是 0"与"还没见过它",
    // 而对 delta 对冲来说那两者的差别就是"要不要把现货算进敞口"。
    //
    // **这里是全量钱包的唯一来源**: 三家的私有钱包 WS 通道都只覆盖发生变动的币种
    // (原文见 hft.domain.Wallet), Bybit 更是明确"订阅成功时不给 snapshot"
    // ("There is no snapshot event given at the time when the subscription is successful") ——
    // 一个不做现货的账户可能几天等不到一条。之后由逐币种的 Topics.Balance 维持。
    val walletEvent = Event.local(Topics.Wallet, Wallet(account, exchange, wallet, nowMs))
    (positionEvents :+ Event.local(Topics.AccountInfo, accountInfo) :+ walletEvent) ++ orderEvents :+ report

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
        // 这些事实都是**已知的** —— 它们就在被拒的那张单上。从前填 Zero 占位, 于是策略读到
        // 一条"价格 0、数量 0"的拒单, 与交易所真实拒单 (带着原委托价与数量) 形状不同,
        // 而两者本该走同一条路径。
        price = order.orderType match
          case OrderType.Limit(px, _) => px
          case OrderType.Market       => Price.Zero // 市价单确实没有委托价
        ,
        quantity = order.quantity,
        filledQuantity = Coin.Zero, // 拒单 = 一点没成交
        reduceOnly = order.reduceOnly,
        timestamp = now,
      ),
      now,
      now,
    )
