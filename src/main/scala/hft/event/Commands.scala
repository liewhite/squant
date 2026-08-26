package hft.event

import hft.domain.*

/** 指令面 —— 总线上"请做什么"的那一半。
  *
  * ## 与数据面的区别不是风格，是校验规则
  *
  * 数据面 ([[Topics]]，"发生了什么") **可以无人订阅**：某个标的这一刻没有成交、某个账户
  * 没有持仓，都不是错误。指令面则**必须有接单者** —— 一条没人接的指令是静默失效：
  * 行情永远不会到、订单永远不会发出、启动对齐永远不完成，而没有任何外在症状。
  *
  * 引擎因此在装配期查一次总线：策略会发出的每一条指令，其 `(topic, key)` 上必须已经有
  * 订阅者 (见 [[EventBus.hasSubscriber]])。**这条校验不需要任何插件声明"我提供什么"** ——
  * 订阅本身就是声明，而订阅是插件为了工作本来就必须做的事。多一份 `provides` 声明就多一处
  * 会写错、会漏写的事实，而漏写的表现是启动被误拒。
  *
  * ## 为什么指令走总线而不是直接调用
  *
  * 直接调用要求调用方持有被调用方 —— 引擎于是得攥着一张"交易所 -> 行情流"的表，
  * "任意交易所、任意数据源"就止于这张表能装下的东西。走总线之后引擎只发指令，
  * 谁接、有几个接、接的是真交易所还是虚拟柜台，它一概不需要知道。
  *
  * ## 指令都是单向的
  *
  * 只有 [[AccountSync]] 有应答 ([[AccountSynced]])，因为"对齐完成"是启动顺序的前提条件
  * (策略必须先有初始仓位再看行情)。其余指令发出即完成：失败一律 fail-fast 抛异常终止进程
  * (见 [[hft.exchange.TradingGateway]])，所以不存在"发出去没有下文"的第三种结局。
  */
object Commands:

  // ==================== 行情订阅 ====================

  /** "请为我订阅这几条公共行情流"。
    *
    * 按交易所路由：每个行情插件只订自己那个 key，于是"这条订阅请求归谁"由投递层回答。
    *
    * 增量、幂等：同一条流被请求多次只生效一次 (由接单的插件保证)，因为策略是动态添加的，
    * 后来者声明的流集合与先前的必然重叠。
    */
  final case class MarketSubscriptionRequest(exchange: Exchange, kinds: Set[SubscriptionKind])

  object MarketSubscription extends Topic[Exchange, MarketSubscriptionRequest]("marketSubscription"):
    def keyOf(payload: MarketSubscriptionRequest): Exchange = payload.exchange

  // ==================== 启动对齐 ====================

  /** "请把这几个标的的当前账户状态推到总线上"。
    *
    * 对齐是**柜台的职责**而不是引擎的：柜台就是那个知道账户当下什么样的角色。引擎从前
    * 自己拿着 REST 客户端拉持仓/挂单/净值，那既要求它持有私有客户端，又要求它知道
    * "一个账户需要对齐哪几样东西" —— 后者是交易所侧的知识。
    *
    * @param requestId 与 [[AccountSyncReport]] 配对。引擎逐次自增，用它认领自己那条应答，
    *                  不会把上一轮的残留应答当成本轮完成
    * @param symbols   要对齐的标的 (交易所由路由键给出，故这里只有 symbol —— 少一处可以
    *                  与路由键不一致的冗余)
    */
  final case class AccountSyncRequest(
      account: AccountId,
      exchange: Exchange,
      requestId: Long,
      symbols: Set[Symbol],
  ):
    def target: AccountExchange = AccountExchange(account, exchange)

  object AccountSync extends Topic[AccountExchange, AccountSyncRequest]("accountSync"):
    def keyOf(payload: AccountSyncRequest): AccountExchange = payload.target

  /** "对齐已推完"。
    *
    * 存在的理由是**顺序**：策略要在拿到初始仓位之后才看到第一条行情，否则它会基于
    * "仓位为空"这个错误前提做第一次决策。引擎据此在发行情订阅指令之前等这条应答。
    *
    * 不含成败：对齐失败一律抛异常终止进程 (账户状态没对上就交易，比不启动危险得多)，
    * 所以收到本事件就等于成功。
    */
  final case class AccountSyncReport(account: AccountId, exchange: Exchange, requestId: Long):
    def target: AccountExchange = AccountExchange(account, exchange)

  object AccountSynced extends Topic[AccountExchange, AccountSyncReport]("accountSynced"):
    def keyOf(payload: AccountSyncReport): AccountExchange = payload.target

  // ==================== 下单 ====================

  /** 策略输出的信号 */
  enum OutcomeEvent:
    /** 下单信号 (一次决策可包含多个关联订单)
      * @param comment 信号意图描述，如 "spread_open | spread=0.30% | qty=10"
      */
    case PlaceOrders(orders: Vector[Order], comment: String)

    /** 撤单信号。撤单的终态确认 (Cancelled) 以私有流推送为准，框架不合成确认事件。
      *
      * 用 [[OrderRef]] 而非裸 id 指名订单：在途单还没有交易所 id，只能按 clientOrderId 撤。
      */
    case CancelOrder(exchange: Exchange, symbol: Symbol, ref: OrderRef)

    /** 本信号发往哪个交易所 —— 路由键的唯一出处。
      *
      * 不叫 `exchange`: 那会与 [[CancelOrder]] 自己的字段撞名。 */
    def targetExchange: Exchange = this match
      case PlaceOrders(orders, _) =>
        // 构造处已保证同所 (见 AccountOutcome 的说明)，取第一张即可
        orders.headOption.map(_.exchange).getOrElse(
          sys.error("PlaceOrders 不能为空: 一条没有订单的下单指令没有交易所可路由")
        )
      case CancelOrder(exchange, _, _) => exchange

  /** 带账户与交易所归属的策略信号 —— 一次决策要发往**哪个账户在哪个交易所的柜台**执行。
    *
    * 构造器限定 `private[hft]`：策略在 `strategy.*` 包里，因此**无法自己拼一条下单意图** ——
    * 既绕不过 [[hft.strategy.StrategyContext.place]] 的 clientOrderId 生成与 pending 登记，
    * 也冒充不了别的账户下单。后者是账户隔离这条防线上原本唯一敞开的口子。
    * 读侧 (订阅、`as`、模式匹配) 不受影响。
    *
    * **一条 PlaceOrders 里的订单必属同一交易所**：跨所的一次决策在
    * [[hft.engine.StrategyRunner]] 里按交易所拆成多条。不拆的话路由键就没有唯一的交易所可取，
    * 而"key 只到账户"会让两个所的柜台都收到同一条意图 —— 静默双执行。
    */
  final case class AccountOutcome private[hft] (account: AccountId, outcome: OutcomeEvent):
    def target: AccountExchange = AccountExchange(account, outcome.targetExchange)

  /** 策略信号的事件族。
    *
    * **按 (账户, 交易所) 路由**：实盘 Binance 柜台订 `Keyed(OrderIntent, {(Live, Binance)})`，
    * 影子盘柜台订自己那个 `Paper(n)`。于是"这条信号该由谁执行"由投递层回答，不需要任何
    * 出口再自己判一次 —— 各写各的否定条件时，新增一个交易所或一类账户不会有任何一处编译
    * 失败，失效方式是静默双执行或静默不执行。
    *
    * 策略不订阅本 topic，故信号不会回流给任何策略。
    */
  object OrderIntent extends Topic[AccountExchange, AccountOutcome]("orderIntent"):
    def keyOf(payload: AccountOutcome): AccountExchange = payload.target

  // ==================== 分组 ====================

  /** 全部指令 topic —— "这是不是一条指令"的唯一判据。
    *
    * 应答 ([[AccountSynced]]) 不在其中: 它是**回答**不是请求, 没有"必须有人接"的要求
    * (发出时等待方可能已经收够了)。
    */
  val all: Set[Topic[?, ?]] = Set(MarketSubscription, AccountSync, OrderIntent)
