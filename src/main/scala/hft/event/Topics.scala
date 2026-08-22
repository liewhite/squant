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
  private def instrumentOf(exchange: Exchange, symbol: Symbol): Instrument = Instrument(exchange, symbol)

  // ==================== 公共行情 (按标的) ====================

  object Bbo extends Topic[Instrument, BBO]("bbo"):
    def keyOf(p: BBO): Instrument = instrumentOf(p.exchange, p.symbol)

  /** 公共成交印记 (市场匿名成交)：策略信号与模拟撮合的价格来源，非本账户成交 */
  object Trade extends Topic[Instrument, MarketTrade]("trade"):
    def keyOf(p: MarketTrade): Instrument = instrumentOf(p.exchange, p.symbol)

  object MarkPrice extends Topic[Instrument, hft.domain.MarkPrice]("markPrice"):
    def keyOf(p: hft.domain.MarkPrice): Instrument = instrumentOf(p.exchange, p.symbol)

  object IndexPrice extends Topic[Instrument, hft.domain.IndexPrice]("indexPrice"):
    def keyOf(p: hft.domain.IndexPrice): Instrument = instrumentOf(p.exchange, p.symbol)

  object FundingRate extends Topic[Instrument, hft.domain.FundingRate]("fundingRate"):
    def keyOf(p: hft.domain.FundingRate): Instrument = instrumentOf(p.exchange, p.symbol)

  // ==================== 账户私有回报 (按标的) ====================

  object Position extends Topic[AccountInstrument, hft.domain.Position]("position"):
    def keyOf(p: hft.domain.Position): AccountInstrument = AccountInstrument(p.account, instrumentOf(p.exchange, p.symbol))

  object OrderUpdate extends Topic[AccountInstrument, hft.domain.OrderUpdate]("orderUpdate"):
    def keyOf(p: hft.domain.OrderUpdate): AccountInstrument = AccountInstrument(p.account, instrumentOf(p.exchange, p.symbol))

  /** 本账户成交 (乐观更新仓位的依据) */
  object Fill extends Topic[AccountInstrument, hft.domain.Fill]("fill"):
    def keyOf(p: hft.domain.Fill): AccountInstrument = AccountInstrument(p.account, instrumentOf(p.exchange, p.symbol))

  // ==================== 账户级读数 (按交易所) ====================

  object Balance extends Topic[AccountExchange, hft.domain.Balance]("balance"):
    def keyOf(p: hft.domain.Balance): AccountExchange = AccountExchange(p.account, p.exchange)

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
  val market: Set[Topic[Instrument, ?]] = Set(Bbo, Trade, MarkPrice, IndexPrice, FundingRate)

  /** 归属某账户某标的的私有回报：由账户流推送，无需订阅 */
  val instrumentPrivate: Set[Topic[AccountInstrument, ?]] = Set(Position, OrderUpdate, Fill)

  /** 账户级读数 */
  val account: Set[Topic[AccountExchange, ?]] = Set(Balance, AccountInfo, Greeks)
