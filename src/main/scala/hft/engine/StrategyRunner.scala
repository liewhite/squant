package hft.engine

import hft.domain.*
import hft.event.Commands.{AccountOutcome, OrderIntent, OutcomeEvent}
import hft.event.{AnyEvent, Event, Handlers, Interest, Subscription, Topics}
import hft.state.StateManager
import hft.strategy.{Strategy, StrategyContext}

/** 策略执行的**纯逻辑核心**：把一个事件喂给策略、维护策略独享状态、产出下单意图。
  *
  * 不含任何传输/并发设施 (无 fork / channel / bus)，因此可被两种驱动复用：
  *   - 实盘/模拟盘 [[Executor]]：虚拟线程串行消费总线事件，把结果发布到总线
  *   - 回测 [[hft.backtest.BacktestEngine]]：单线程虚拟时间循环里同步调用 [[accepts]]/[[onEvent]]
  *
  * **不做交易所精度对齐**：tick、最小下单量、张数换算都是交易所的事实，归柜台
  * (见 [[hft.exchange.TradingGateway]])。策略这一侧从头到尾只有币本位，
  * 收不下的单由柜台以拒单回流，与"交易所明确拒绝"走同一条清理路径。
  *
  * @param orderConfirmationTimeoutMs 下单请求等待交易所确认的上限。由驱动层按运行模式决定：
  *   实盘为严格正值；回测由确定性队列驱动，不存在网络结果不确定，因此以 0 关闭该项校验。
  * @param clientOrderIdGen client_order_id 生成器 (按交易所格式)。实盘用 UUID 保唯一；
  *   回测注入确定性自增计数 (见 [[StrategyRunner.backtest]])，使逐笔回报/CSV 跨运行可复现。
  */
final class StrategyRunner private (
    strategy: Strategy,
    /** 本实例绑定的账户 —— 装配期决定，策略自己不知道。无默认值，理由同 [[Executor]] */
    val account: AccountId,
    orderConfirmationTimeoutMs: Long,
    clientOrderIdGen: Exchange => String,
):
  /** 策略声明的处理器，账户已绑定 */
  // 只取一次：handlers 是 def，业务策略在里面捕获自身可变状态构造闭包，两次调用得到两个实例
  private val handlers: Handlers[StrategyContext] = strategy.handlers.bind(account)

  /** 策略实际的订阅范围 = 处理器派生的声明 + 框架补齐 (见 [[StrategyRunner.subscriptionFor]]) */
  val subscription: Subscription = StrategyRunner.subscriptionFor(handlers.interests, account)

  val state: StateManager = StateManager(subscription.instruments, orderConfirmationTimeoutMs)

  /** 让策略把自己准备好 —— 见 [[Strategy.prepare]]。**允许阻塞**。
    *
    * 实盘由 [[Executor.onPrepare]] 调, 回测由 [[hft.backtest.BacktestEngine.run]] 调,
    * 两条路径共用这一个入口: 就绪逻辑写一遍、两边都跑得到。少了任何一边, 一个把关键
    * 就绪逻辑写进 prepare 的策略就会在那条路径上静默地不就绪 —— 而"实盘与回测分叉"
    * 恰恰是把预热收进 prepare 想消灭的东西。
    */
  def prepare(): Unit = strategy.prepare()

  /** 这条事件是否归本策略。判据来自 [[Subscription]]，与总线索引同源 */
  def accepts(event: AnyEvent): Boolean = subscription.accepts(event)

  /** 更新状态并运行策略，返回策略产出的事件 (下单意图已分配 id、登记 pending)。
    * `now` 为当前处理时刻 (回测虚拟时间 / 实盘墙钟)，作为 pending order 的 createdAt。
    */
  def onEvent(event: AnyEvent, now: Timestamp): Vector[AnyEvent] =
    val tag = observe(event)
    handlers.dispatch(event, StrategyContext(state, account, now, tag), now).map(prepareIntent(_, now))

  /** 只更新状态，**不叫醒策略**；返回这条事件所属订单的**策略标注**
    * (见 [[hft.strategy.StrategyContext.orderTag]])。
    *
    * 启动对齐还没落地时用它：那时初始仓位与既有挂单都还在路上，策略要是此刻动作，
    * 用的就是一份残缺的世界观 (见 [[Executor]] 的闸门)。状态照收不误 —— 排队的事件
    * 一条都不能丢，只是先不据此决策。闸门期的返回值无人接，那没有关系：闸门后面没有策略
    * 在等着被分派。
    *
    * **取标注与应用事件的顺序是承重的**，所以两件事收在这一个方法里：订单进终态时挂单
    * 登记会被移除 (见 [[hft.state.InstrumentState]])，先应用再取就只剩 `None`。拆成两步交给
    * 调用方按顺序写，写反的症状是"标注恒为 None" —— 没有报错、没有异常，只是策略从此
    * 认不出自己的单。
    */
  def observe(event: AnyEvent): Option[String] =
    val tag = state.orderTagOf(event)
    state.apply(event)
    tag

  /** 对策略**真正返回**的下单意图施加发单前的两件必做事：分配 clientOrderId、
    * 以币本位登记 pending。
    *
    * 收在这里而不是在 `ctx.place` 构造时做 —— 策略可能构造了却不返回（条件分支丢弃），
    * 那样会登记一条永远不会发出的幽灵挂单。只有返回的才算数。
    * 非下单意图（策略自己的指标等）原样透传。
    *
    * 登记的是**币本位原始数量**，而柜台真正发出的是对齐到交易所精度之后的量，两者可能
    * 差一个取整。这是有意的：pending 登记的用途是超时检测与停机撤单，两者按
    * clientOrderId 认单，不比对数量；而让策略侧看到"被交易所取整之后的数"反而会让
    * 它的敞口账与自己的意图对不上。真实成交量一律以回报为准。
    */
  private def prepareIntent(produced: AnyEvent, now: Timestamp): AnyEvent =
    produced.as(OrderIntent) match
      case Some(AccountOutcome(acct, OutcomeEvent.PlaceOrders(orders, comment))) =>
        val identified = orders.map { order =>
          val withId = order.copy(clientOrderId = clientOrderIdGen(order.exchange))
          state.addPendingOrder(withId, now)
          withId
        }
        Event.stamped(OrderIntent, AccountOutcome(acct, OutcomeEvent.PlaceOrders(identified, comment)), now, now)
      case _ => produced

  /** 撤掉本策略全部挂单的信号 —— 停机收尾用。
    *
    * **在途单 (还没拿到交易所 id) 按 clientOrderId 撤**，不能跳过：策略一旦撤下就没有
    * "下一次收尾"了 (它已退订)，超时检测也随它一起停了，跳过等于把一张 GTC 单留在交易所
    * 无人跟踪。三家交易所都支持按自有 id 撤 (见 [[OrderRef]])。
    *
    * 撤一张已经成交或本就不存在的单会得到 `OrderNotFound`，那是既有的容忍路径 (非致命)。
    */
  def pendingCancels(now: Timestamp): Vector[AnyEvent] =
    // 标注取 None: 这里不在处理任何一条回报, 收尾撤的是全部挂单
    val ctx = StrategyContext(state, account, now, orderTag = None)
    state.allPendingOrders.view
      .map { p =>
        val ref = if p.order.id.nonEmpty then OrderRef.ByExchangeId(p.order.id) else OrderRef.ByClientId(p.order.clientOrderId)
        ctx.cancel(p.order.exchange, p.order.symbol, ref)
      }
      .toVector

object StrategyRunner:
  /** 实盘逻辑核心。确认超时属于执行环境，由 [[Executor]] 注入，不由策略选择。 */
  private[engine] def live(
      strategy: Strategy,
      account: AccountId,
      orderConfirmationTimeoutMs: Long,
  ): StrategyRunner =
    require(orderConfirmationTimeoutMs > 0, s"实盘订单确认超时必须为正数，实为 $orderConfirmationTimeoutMs")
    new StrategyRunner(strategy, account, orderConfirmationTimeoutMs, _.newClientOrderId)

  /** 策略声明 + 框架补齐 = 策略实际的订阅范围。
    *
    * 补齐的三类订阅**不该由策略选择**，因此不留给策略声明 —— 漏订一条持仓或订单回报，
    * 策略就会拿着错的敞口决策、或者永远清不掉一条幽灵挂单，而这些没有任何外在症状：
    *   1. 所声明标的的持仓与订单回报 (见 [[Topics.essentialPrivate]]；
    *      **成交明细不补** —— 仓位归柜台算之后策略不再非它不可，要就自己声明)；
    *   2. 所涉交易所的账户级读数 (余额 / 净值 / 希腊值)；
    *   3. 时钟 (驱动 [[hft.state.InstrumentState.failOnTimedOutOrders]])。
    *
    * 账户级读数按**交易所**补齐而不是全收：策略读不到自己没订阅的交易所的净值，
    * 而杠杆闸门正是拿净值算的。
    */
  def subscriptionFor(declared: Set[Interest], account: AccountId): Subscription =
    val base = Subscription(declared)
    val instrumentKeys = base.instruments
    val exchangeKeys = base.exchanges
    // 补齐的私有回报与账户级读数都带账户维度：同一份策略逻辑跑在实盘与影子账户上时，
    // 两个实例声明的行情完全相同，靠这个维度才分得开谁的成交是谁的。
    val privateInterests: Set[Interest] =
      if instrumentKeys.isEmpty then Set.empty
      else Topics.essentialPrivate.map(t => Interest.Keyed(t, instrumentKeys.map(AccountInstrument(account, _))))
    val accountInterests: Set[Interest] =
      if exchangeKeys.isEmpty then Set.empty
      else Topics.account.map(t => Interest.Keyed(t, exchangeKeys.map(AccountExchange(account, _))))
    Subscription(declared ++ privateInterests ++ accountInterests + Interest.All(Topics.Clock))

  /** 回测用确定性 client_order_id 生成器：自增计数 bt0/bt1/...，使逐笔回报/CSV 跨运行可复现。
    * 有状态闭包 (单回测为单线程，无并发问题)；每个 runner 独享一份，自 0 起算。 */
  def deterministicIdGen(): Exchange => String =
    var counter = 0L
    (_: Exchange) =>
      val id = s"bt$counter"
      counter += 1
      id

  /** 回测专用工厂：注入确定性 client_order_id 生成器 (杜绝 UUID 随机带来的不可复现)。
    *
    * `account` 必须与 [[hft.backtest.BacktestEngine]] 的账户一致 —— 私有回报按
    * `AccountInstrument(account, 标的)` 路由，两边不一致的话撮合发出的成交回报根本进不了
    * 本 runner 的订阅范围，策略从此收不到自己的成交，而这不会报任何错。
    * 引擎在装配期校验这一点，此处保留参数是为了不把"回测只能有一个账户"焊死。
    */
  def backtest(strategy: Strategy, account: AccountId = AccountId.Live): StrategyRunner =
    new StrategyRunner(
      strategy,
      account,
      orderConfirmationTimeoutMs = 0L,
      clientOrderIdGen = deterministicIdGen(),
    )
