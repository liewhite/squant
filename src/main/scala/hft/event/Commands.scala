package hft.event

import hft.domain.*
import hft.kernel.Cardinality

/** 指令面 —— 总线上"请做什么"的那一半。
  *
  * ## 与数据面的区别不是风格，是校验规则
  *
  * 数据面 ([[Topics]]，"发生了什么") **可以无人订阅**：某个标的这一刻没有成交、某个账户
  * 没有持仓，都不是错误。指令面则**必须有接单者** —— 一条没人接的指令是静默失效：
  * 行情永远不会到、订单永远不会发出、启动对齐永远不完成，而没有任何外在症状。
  *
  * 命令 topic 自带处理者基数，组件通过硬依赖声明自己需要哪些键。ActorSystem 在装配、发布
  * 和动态停止时校验。处理者通过 `CommandHandler` 显式承担命令；普通订阅始终只是观察者，
  * 不会因为监控或审计一条命令而改变执行基数。
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

  object MarketSubscription
      extends CommandTopic[Exchange, MarketSubscriptionRequest]("marketSubscription", Cardinality.AtLeastOne):
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
    * @param instruments 要对齐的标的。带品种 (见 [[hft.domain.InstrumentKind]]) —— 柜台据它
    *                    决定去哪个端点查挂单: 同一个 symbol 底下可能有永续、币本位、期权。
    */
  final case class AccountSyncRequest(
      account: AccountId,
      exchange: Exchange,
      requestId: Long,
      instruments: Set[Instrument],
  ):
    def target: AccountExchange = AccountExchange(account, exchange)

  object AccountSync
      extends CommandTopic[AccountExchange, AccountSyncRequest]("accountSync", Cardinality.ExactlyOne):
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
    // 构造校验：枚举的 case class 成员在 init 时跑到这里，因此不变量对全部构造点一律生效。
    this match
      case PlaceOrders(orders, comment) =>
        require(orders.nonEmpty, s"PlaceOrders 不能为空: 一条没有订单的下单指令没有交易所可路由 (comment=$comment)")
        val exchanges = orders.map(_.exchange).distinct
        require(
          exchanges.sizeIs == 1,
          s"一条 PlaceOrders 的订单必须同属一个交易所, 实际 ${exchanges.mkString(",")} " +
            s"—— 跨所决策要按交易所拆成多条 (comment=$comment)",
        )
      case CancelOrder(_, _) => ()

    /** 下单信号 (一次决策可包含多个关联订单)。
      *
      * **不变量在构造处钉死**：非空、且全部订单同属一个交易所。它曾经只写在
      * [[AccountOutcome]] 的注释里，靠 [[hft.strategy.StrategyContext]] 与
      * [[hft.engine.StrategyRunner]] 记得先按交易所拆分 —— 而 [[targetExchange]] 取的是
      * `orders.head.exchange`，混进第二个交易所的订单会被静默路由到错误的柜台，
      * 唯一症状是那些单去了别的所。三个构造点里只要有一个漏了拆分，这条不变量就破了，
      * 因此它必须由构造本身保证。
      *
      * @param comment 信号意图描述，如 "spread_open | spread=0.30% | qty=10"
      */
    case PlaceOrders(orders: Vector[Order], comment: String)

    /** 撤单信号。撤单的终态确认 (Cancelled) 以私有流推送为准，框架不合成确认事件。
      *
      * 用 [[OrderRef]] 而非裸 id 指名订单：在途单还没有交易所 id，只能按 clientOrderId 撤。
      */
    case CancelOrder(instrument: Instrument, ref: OrderRef)

    /** 本信号发往哪个交易所 —— 路由键的唯一出处。
      *
      * 不叫 `exchange`: 那会与 [[CancelOrder]] 自己的字段撞名。 */
    def targetExchange: Exchange = this match
      // 构造处 (init 校验) 已保证非空且同所，取第一张即可
      case PlaceOrders(orders, _)      => orders.head.exchange
      case CancelOrder(instrument, _) => instrument.exchange

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
  object OrderIntent
      extends CommandTopic[AccountExchange, AccountOutcome]("orderIntent", Cardinality.ExactlyOne):
    def keyOf(payload: AccountOutcome): AccountExchange = payload.target
