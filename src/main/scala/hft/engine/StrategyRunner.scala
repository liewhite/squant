package hft.engine

import hft.domain.*
import hft.event.{AnyEvent, Interest, Subscription, Topics}
import hft.state.StateManager
import hft.strategy.{OutcomeEvent, Strategy}

/** 策略执行的**纯逻辑核心**：把一个事件喂给策略、维护策略独享状态、产出"已按交易所精度转换"的信号。
  *
  * 不含任何传输/并发设施 (无 fork / channel / bus)，因此可被两种驱动复用：
  *   - 实盘/模拟盘 [[Executor]]：虚拟线程串行消费总线事件，调用 [[onEvent]] 后把结果发布到 outcomeBus
  *   - 回测 [[hft.backtest.BacktestEngine]]：单线程虚拟时间循环里同步调用 [[accepts]]/[[onEvent]]
  *
  * 职责与原 Executor.handle 完全一致：过滤订阅范围、更新 StateManager、分配 clientOrderId、
  * 登记 pending、按 SymbolMeta 转换 (币本位->张数、价格/数量取整)。行为不变。
  *
  * @param clientOrderIdGen client_order_id 生成器 (按交易所格式)。实盘默认用 UUID 保唯一；
  *   回测注入确定性自增计数 (见 [[StrategyRunner.backtest]])，使逐笔回报/CSV 跨运行可复现。
  */
final class StrategyRunner(
    strategy: Strategy,
    symbolMetas: Map[(Exchange, Symbol), SymbolMeta],
    /** 本实例绑定的账户 —— 装配期决定，策略自己不知道。无默认值，理由同 [[Executor]] */
    val account: AccountId,
    clientOrderIdGen: Exchange => String = _.newClientOrderId,
):
  /** 策略实际的订阅范围 = 策略声明 + 框架补齐 (见 [[StrategyRunner.subscriptionFor]]) */
  val subscription: Subscription = StrategyRunner.subscriptionFor(strategy, account)

  val state: StateManager = StateManager(subscription.instruments.map(_.symbol), strategy.orderTimeoutMs)

  /** 这条事件是否归本策略。判据来自 [[Subscription]]，与总线索引同源 */
  def accepts(event: AnyEvent): Boolean = subscription.accepts(event)

  /** 更新状态并运行策略，返回**已转换为交易所格式**的信号 (下单已分配 id、登记 pending、取整)。
    * `now` 为当前处理时刻 (回测虚拟时间 / 实盘墙钟)，作为 pending order 的 createdAt (超时检测基准)。
    */
  def onEvent(event: AnyEvent, now: Timestamp): Vector[OutcomeEvent] =
    state.apply(event)
    strategy.onEvent(event, state).map {
      case OutcomeEvent.PlaceOrders(orders, comment) =>
        val converted = orders.map { order =>
          val withId = order.copy(clientOrderId = clientOrderIdGen(order.exchange))
          // 原始订单 (币本位) 登记到 pending，策略端统一看到币的数量
          state.addPendingOrder(withId, now)
          // 转换为交易所格式 (合约张数 + 价格/数量取整)
          convertOrder(withId)
        }
        OutcomeEvent.PlaceOrders(converted, comment)
      case cancel: OutcomeEvent.CancelOrder => cancel
    }

  /** 撤掉本策略全部挂单的信号 —— 停机收尾用。
    *
    * **在途单 (还没拿到交易所 id) 按 clientOrderId 撤**，不能跳过：策略一旦撤下就没有
    * "下一次收尾"了 (它已退订)，超时检测也随它一起停了，跳过等于把一张 GTC 单留在交易所
    * 无人跟踪。三家交易所都支持按自有 id 撤 (见 [[OrderRef]])。
    *
    * 撤一张已经成交或本就不存在的单会得到 `OrderNotFound`，那是既有的容忍路径 (非致命)。
    */
  def pendingCancels: Vector[OutcomeEvent] =
    state.allPendingOrders.view
      .map { p =>
        val ref = if p.order.id.nonEmpty then OrderRef.ByExchangeId(p.order.id) else OrderRef.ByClientId(p.order.clientOrderId)
        OutcomeEvent.CancelOrder(p.order.exchange, p.order.symbol, ref)
      }
      .toVector

  private def convertOrder(order: Order): Order = OrderConversion.toExchangeFormat(order, symbolMetas)

object StrategyRunner:
  /** 策略声明 + 框架补齐 = 策略实际的订阅范围。
    *
    * 补齐的三类订阅**不该由策略选择**，因此不留给策略声明 —— 漏订一条 Fill 就会让本地
    * 仓位与交易所长期发散，而这种 bug 没有任何外在症状：
    *   1. 所声明标的的私有回报 (持仓 / 订单回报 / 成交)；
    *   2. 所涉交易所的账户级读数 (余额 / 净值 / 希腊值)；
    *   3. 时钟 (驱动 [[hft.state.SymbolState.failOnTimedOutOrders]])。
    *
    * 账户级读数按**交易所**补齐而不是全收：策略读不到自己没订阅的交易所的净值，
    * 而杠杆闸门正是拿净值算的。
    */
  def subscriptionFor(strategy: Strategy, account: AccountId): Subscription =
    val declared = strategy.interests
    val base = Subscription(declared)
    val instrumentKeys = base.instruments
    val exchangeKeys = base.exchanges
    // 补齐的私有回报与账户级读数都带账户维度：同一份策略逻辑跑在实盘与影子账户上时，
    // 两个实例声明的行情完全相同，靠这个维度才分得开谁的成交是谁的。
    val privateInterests: Set[Interest] =
      if instrumentKeys.isEmpty then Set.empty
      else Topics.instrumentPrivate.map(t => Interest.Keyed(t, instrumentKeys.map(AccountInstrument(account, _))))
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

  /** 回测专用工厂：注入确定性 client_order_id 生成器 (杜绝 UUID 随机带来的不可复现)。 */
  def backtest(strategy: Strategy, symbolMetas: Map[(Exchange, Symbol), SymbolMeta]): StrategyRunner =
    StrategyRunner(strategy, symbolMetas, AccountId.Live, deterministicIdGen())
