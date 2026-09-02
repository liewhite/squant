package hft.event

import hft.domain.{AccountExchange, AccountId, AccountInstrument, Exchange, Instrument, SubscriptionKind}

/** 一个订阅者的完整订阅范围：若干 [[Interest]] 的聚合，以及"这条事件归不归它"的判据。
  *
  * 同一份声明有三个下游，这正是它必须是**数据**的原因 (见 [[Interest]])：
  *   - [[EventBus]] 据此建投递索引；
  *   - 回测在单线程循环里直接用 [[accepts]] 过滤 (没有总线，但判据必须与实盘同一份)；
  *   - 引擎据 [[instruments]] 汇总出要向交易所订阅的行情流。
  */
final case class Subscription(interests: Set[Interest]):
  /** 这条事件是否落在本范围内 */
  def accepts(event: AnyEvent): Boolean = interests.exists(_.accepts(event))

  /** 本订阅者**交易**的标的。
    *
    * 只从框架内置的按标的路由 topic 派生 (行情 + 私有回报)，而不是"声明里出现过的任何
    * Instrument"。两者是不同的事实：用户自定义的按标的路由 topic (例如订阅另一个策略在某
    * 标的上的指标) 表示**关注**，不表示**交易**。
    *
    * 混为一谈的后果是补过头 —— 一个只看指标的监控/元策略会被补上该标的的私有回报订阅，
    * 进而被引擎拉去做持仓对齐、要求 SymbolMeta，而它根本不交易那个标的。
    */
  def instruments: Set[Instrument] = interests.flatMap {
    // 按**类型**判定而非查一张内置表：用户自定义的行情源继承 MarketTopic 即被认作交易标的，
    // 而普通的 Topic[Instrument, P]（如别人的指标）不会 —— 判定不依赖"记得登记进某个 Set"。
    case Interest.Keyed(_: MarketTopic[?], keys) => keys.collect { case i: Instrument => i }
    case Interest.Keyed(t, keys) if Topics.instrumentPrivate.exists(_ eq t) =>
      keys.collect { case ai: AccountInstrument => ai.instrument }
    case _ => Set.empty[Instrument]
  }

  /** 要向各交易所订阅的公共行情流。
    *
    * 与 [[instruments]] 同源同判据：都从本订阅范围直接派生，不存在第二份声明。
    * 每个 [[MarketTopic]] 自己回答"我对应哪条流"（[[MarketTopic.streamKind]]），
    * 所以新增行情源不需要在别处登记，用户自定义的行情源同样成立。
    *
    * 公共行情用 [[Interest.All]] 声明会直接报错：全量订阅没有标的集合，
    * 框架无从知道该向交易所订哪些流 —— 静默订不到远比启动即失败糟糕。
    */
  def marketStreams: Set[(Exchange, SubscriptionKind)] = interests.flatMap {
    case Interest.Keyed(t: MarketTopic[?], keys) =>
      keys.collect { case i: Instrument => (i.exchange, t.streamKind(i.symbol)) }
    case Interest.All(t: MarketTopic[?]) =>
      sys.error(
        s"公共行情 topic '$t' 只能用 Interest.Keyed 声明: Interest.All 没有标的集合, " +
          "框架无从知道该向交易所订阅哪些流"
      )
    case _ => Set.empty[(Exchange, SubscriptionKind)]
  }

  /** 行情订阅指令的**派生结果**：一个交易所一条指令。
    *
    * 与 [[marketRequirements]] 成对：一处声明 (interests)，两处派生。收在这里是因为
    * `Engine.watchMarket` 与 `StrategySession` 从前各写了一遍同样的 groupMap，
    * 而"该向哪个交易所订哪些流"只该有一个答案。 */
  def marketRequests: Set[(Exchange, Set[SubscriptionKind])] =
    marketStreams.groupMap(_._1)(_._2).view.mapValues(_.toSet).toSet

  /** 涉及的全部交易所：交易标的所属的，加上账户级声明直接指名的 */
  def exchanges: Set[Exchange] =
    instruments.map(_.exchange) ++ keysOf(Topics.account).map(_.exchange)

  /** 启动对齐要覆盖的 (账户, 交易所) —— **闸门等谁、引擎发给谁，必须是同一个集合**。
    *
    * 两处各自推导过一次，而且推得不一样：执行器的闸门按 [[exchanges]] 等，引擎却按
    * `instruments.groupMap(_.exchange)` 发指令 —— 后者不含"账户级声明直接指名的交易所"
    * (一个用 `custom` 订了某所净值/希腊值、却不在该所交易的策略就落在这个差集里)。
    * 结果是引擎不为它发对齐指令、latch 也不等它，于是**引擎打印"启动对齐完成"照常放行，
    * 而那个策略永久停在闸门后**：状态照收、只观察不动作、不抛异常、不打日志。
    *
    * 所以这个集合必须有唯一的出处。谁要用就问它要，别再各算一遍。
    */
  def alignmentTargets(account: AccountId): Set[AccountExchange] =
    exchanges.map(AccountExchange(account, _))

  private def keysOf[K](topics: Set[Topic[K, ?]]): Set[K] =
    topics.flatMap(t => interests.flatMap(_.keysOf(t)))

object Subscription:
  val empty: Subscription = Subscription(Set.empty)
