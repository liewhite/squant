package hft.event

import hft.domain.*

/** 框架内置的事件族。
  *
  * 三档路由维度，对应三种事实：
  *   - **按标的** ([[Instrument]])：公共行情。无账户归属 —— 一份数据服务所有账户，
  *     实盘策略与影子策略看的是同一条 BBO，不必也不该订两遍。
  *   - **按账户标的** ([[AccountInstrument]])：私有回报 (持仓/订单回报/成交)。
  *   - **按账户交易所** ([[AccountExchange]])：账户级读数 (余额/净值/希腊值)。
  *
  * 私有回报带账户维度，是订单归属的**结构保证**：实盘策略与影子策略即便交易同一标的，
  * 也从投递层就收不到对方的成交与订单回报。只按标的路由的话两者会互相收养对方的挂单，
  * 各自还以为账户总仓位是自己的敞口 —— 决策依据错了却没有任何症状。
  *
  * 账户级读数同时按交易所过滤，是另一条越界防线：策略读不到自己没订阅的交易所的净值，
  * 而杠杆闸门正是拿净值算的。
  *
  * 用户自定义事件不必也不该加进这里，见 [[Topic]] 的用法示例。
  */
object Topics:
  // ==================== 公共行情 (按标的) ====================

  object Bbo extends MarketTopic[BBO]("bbo"):
    def keyOf(p: BBO): Instrument = p.instrument
    def streamKind(symbol: Symbol): SubscriptionKind = SubscriptionKind.BBO(symbol)

  /** 公共成交印记 (市场匿名成交)：策略信号与模拟撮合的价格来源，非本账户成交 */
  object Trade extends MarketTopic[MarketTrade]("trade"):
    def keyOf(p: MarketTrade): Instrument = p.instrument
    def streamKind(symbol: Symbol): SubscriptionKind = SubscriptionKind.Trade(symbol)

  object MarkPrice extends MarketTopic[hft.domain.MarkPrice]("markPrice"):
    def keyOf(p: hft.domain.MarkPrice): Instrument = p.instrument
    def streamKind(symbol: Symbol): SubscriptionKind = SubscriptionKind.MarkPrice(symbol)

  object IndexPrice extends MarketTopic[hft.domain.IndexPrice]("indexPrice"):
    def keyOf(p: hft.domain.IndexPrice): Instrument = p.instrument
    def streamKind(symbol: Symbol): SubscriptionKind = SubscriptionKind.IndexPrice(symbol)

  object FundingRate extends MarketTopic[hft.domain.FundingRate]("fundingRate"):
    def keyOf(p: hft.domain.FundingRate): Instrument = p.instrument
    def streamKind(symbol: Symbol): SubscriptionKind = SubscriptionKind.FundingRate(symbol)

  // ==================== 账户私有回报 (按标的) ====================

  object Position extends Topic[AccountInstrument, hft.domain.Position]("position"):
    def keyOf(p: hft.domain.Position): AccountInstrument = AccountInstrument(p.account, p.instrument)

  object OrderUpdate extends Topic[AccountInstrument, hft.domain.OrderUpdate]("orderUpdate"):
    def keyOf(p: hft.domain.OrderUpdate): AccountInstrument = AccountInstrument(p.account, p.instrument)

  /** 本账户成交明细。
    *
    * **不是仓位的依据** —— 仓位由柜台的账本维护 (见 [[hft.exchange.TradingGateway]])，
    * 这一条是给要看成交本身的人：绩效统计、成交记录、滑点分析。
    * 框架不给策略补齐它，要就自己声明。 */
  object Fill extends Topic[AccountInstrument, hft.domain.Fill]("fill"):
    def keyOf(p: hft.domain.Fill): AccountInstrument = AccountInstrument(p.account, p.instrument)

  // ==================== 账户级读数 (按交易所) ====================

  object Balance extends Topic[AccountExchange, hft.domain.Balance]("balance"):
    def keyOf(p: hft.domain.Balance): AccountExchange = AccountExchange(p.account, p.exchange)

  /** 完整钱包快照 —— 未列出的币种余额为 0。
    *
    * **唯一生产者是启动对齐时的一次 REST 钱包查询** (`TradingGateway.currentWallet`)：
    * 三家的钱包 WS 通道都只覆盖发生变动的币种, 给不出"这就是整份钱包"这个更强的事实。
    * 之后由逐币种的 [[Balance]] 推送维持。见 [[hft.domain.Wallet]]。 */
  object Wallet extends Topic[AccountExchange, hft.domain.Wallet]("wallet"):
    def keyOf(p: hft.domain.Wallet): AccountExchange = AccountExchange(p.account, p.exchange)

  object AccountInfo extends Topic[AccountExchange, hft.domain.AccountInfo]("accountInfo"):
    def keyOf(p: hft.domain.AccountInfo): AccountExchange = AccountExchange(p.account, p.exchange)

  /** 账户级期权希腊字母 (按币种聚合)：key 只到交易所，币种在载荷的 `ccy` 里 */
  object Greeks extends Topic[AccountExchange, hft.domain.Greeks]("greeks"):
    def keyOf(p: hft.domain.Greeks): AccountExchange = AccountExchange(p.account, p.exchange)

  // ==================== 全局 ====================

  /** 定时节拍 (驱动订单超时检测等)。无路由维度，订阅方式只有 [[Interest.All]] */
  object Clock extends Topic[Unit, Unit]("clock"):
    def keyOf(p: Unit): Unit = ()

  /** 一条 Clock 事件 (载荷为空，只有时间戳有意义) */
  def clockAt(ts: Timestamp): Event[Unit, Unit] = Event.stamped(Clock, (), ts, ts)

  // ==================== 分组 (框架派生订阅用) ====================

  /** 公共行情 topic：需要**向交易所订阅**才会有数据 */
  val market: Set[MarketTopic[?]] = Set(Bbo, Trade, MarkPrice, IndexPrice, FundingRate)

  /** 归属某账户某标的的私有回报：由柜台推送，无需订阅。
    *
    * 用途是**判定"这个 topic 属于私有回报"**（据此从订阅声明反推交易标的），
    * 与"框架该给策略补齐哪些"是两件事，见 [[essentialPrivate]]。 */
  val instrumentPrivate: Set[Topic[AccountInstrument, ?]] = Set(Position, OrderUpdate, Fill)

  /** 框架**必定补齐**给策略的私有回报 —— 缺了它们策略无法正确工作：
    *   - [[Position]]：敞口。由柜台维护并推送，策略读它决策
    *   - [[OrderUpdate]]：挂单生命周期。超时检测与停机撤单都靠它
    *
    * [[Fill]] **不在其中**。它曾经是必修课，因为仓位靠策略自己累加成交得出；
    * 现在仓位归柜台算（见 [[hft.exchange.TradingGateway]]），策略侧就没有任何东西
    * 非它不可了。想看成交明细（滑点统计、成交记录）的策略自己声明 `own(Topics.Fill)`
    * —— 那本来就是它该说的话。
    *
    * 少补一条不是省事：成交是热路径，多策略部署下每笔成交都要白投几份。
    */
  val essentialPrivate: Set[Topic[AccountInstrument, ?]] = Set(Position, OrderUpdate)

  /** 账户级读数 */
  val account: Set[Topic[AccountExchange, ?]] = Set(Balance, Wallet, AccountInfo, Greeks)
