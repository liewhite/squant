package hft.strategy

import hft.domain.*
import hft.event.{AnyEvent, Handlers, MarketTopic, Topic, Topics}

/** 策略侧的处理器声明 —— 用**账户无关**的语气写，账户维度由装配期补上。
  *
  * 策略不知道自己跑在实盘还是影子账户上（这是有意的：同一份逻辑要能两边都跑），
  * 可私有回报的路由键偏偏含账户。所以策略说的是"我自己的成交"而不是
  * "Paper(1) 在 BTCUSDT 上的成交"，[[bind]] 时才补齐。
  *
  * 这样策略既写不出错误的账户，也不必为此知道账户是什么。
  */
final class StrategyHandlers private (
    private val build: (AccountId, Set[Instrument]) => Handlers[StrategyContext],
    private val declaredInstruments: Set[Instrument],
    /** 是否声明过依赖标的集合的处理器 (own/account)，供 [[bind]] 的一致性检查 */
    private val needsInstruments: Boolean,
):
  /** 公共行情：无账户归属，key 就是标的。
    *
    * 参数限定为 [[MarketTopic]] 而非任意 `Topic[Instrument, P]` —— 用它声明即表示
    * **交易该标的**，框架会据此补齐私有回报、做启动对齐。只是想读一条按标的路由的
    * 自定义事件（例如别的策略的指标）请用 [[custom]]，那不代表交易。
    */
  def market[P](topic: MarketTopic[P], instruments: Set[Instrument])(
      f: (P, StrategyContext, Timestamp) => Vector[AnyEvent]
  ): StrategyHandlers =
    StrategyHandlers(
      (acct, insts) => build(acct, insts).on(topic, instruments)(f),
      declaredInstruments ++ instruments,
      needsInstruments,
    )

  def market[P](topic: MarketTopic[P], instrument: Instrument)(
      f: (P, StrategyContext, Timestamp) => Vector[AnyEvent]
  ): StrategyHandlers = market(topic, Set(instrument))(f)

  /** **本策略本账户**在所声明标的上的私有回报（成交 / 订单回报 / 持仓） */
  def own[P](topic: Topic[AccountInstrument, P])(
      f: (P, StrategyContext, Timestamp) => Vector[AnyEvent]
  ): StrategyHandlers =
    StrategyHandlers(
      (acct, insts) => build(acct, insts).on(topic, insts.map(AccountInstrument(acct, _)))(f),
      declaredInstruments,
      needsInstruments = true,
    )

  /** **本账户**在所涉交易所上的账户级读数（余额 / 净值 / 希腊值） */
  def account[P](topic: Topic[AccountExchange, P])(
      f: (P, StrategyContext, Timestamp) => Vector[AnyEvent]
  ): StrategyHandlers =
    StrategyHandlers(
      (acct, insts) => build(acct, insts).on(topic, insts.map(i => AccountExchange(acct, i.exchange)))(f),
      declaredInstruments,
      needsInstruments = true,
    )

  /** 时钟节拍 */
  def onClock(f: (StrategyContext, Timestamp) => Vector[AnyEvent]): StrategyHandlers =
    StrategyHandlers((acct, insts) => build(acct, insts).onTick(Topics.Clock)(f), declaredInstruments, needsInstruments)

  /** 自定义 topic（用户自己的事件族，账户无关） */
  def custom[K, P](topic: Topic[K, P], keys: Set[K])(
      f: (P, StrategyContext, Timestamp) => Vector[AnyEvent]
  ): StrategyHandlers =
    StrategyHandlers((acct, insts) => build(acct, insts).on(topic, keys)(f), declaredInstruments, needsInstruments)

  /** 策略声明要交易/观察的标的 —— 框架据此订阅行情、补齐私有回报、做启动对齐 */
  def instruments: Set[Instrument] = declaredInstruments

  /** 绑定账户，得到可直接分发的处理器。
    *
    * `own`/`account` 的 key 集合由 `market` 声明的标的推出。若声明了前者却没有后者，
    * 得到的会是空 key 集合 —— 按 [[hft.event.Interest]] 的语义"空集即不订阅"，
    * 那是**静默收不到任何私有回报**。装配期直接拒绝，这正是本设计要消灭的失效类别。
    */
  def bind(account: AccountId): Handlers[StrategyContext] =
    require(
      !needsInstruments || declaredInstruments.nonEmpty,
      "策略声明了 own/account 处理器却没有用 market 声明任何标的: " +
        "私有回报与账户级读数的订阅范围由交易标的推出, 缺了它这些处理器永远收不到事件",
    )
    build(account, declaredInstruments)

object StrategyHandlers:
  val empty: StrategyHandlers = new StrategyHandlers((_, _) => Handlers.empty, Set.empty, needsInstruments = false)

  private def apply(
      build: (AccountId, Set[Instrument]) => Handlers[StrategyContext],
      instruments: Set[Instrument],
      needsInstruments: Boolean,
  ): StrategyHandlers = new StrategyHandlers(build, instruments, needsInstruments)
