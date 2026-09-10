package hft.state

import hft.domain.*

/** 策略能看到的**单标的**状态 —— 只读。
  *
  * 与 [[InstrumentState]]（实现）分开，是因为两者的能力面本就不同：框架要写（应用事件、
  * 登记挂单、超时校验），策略只该读。此前策略拿到的是实现本身，于是能 `positions.clear()`、
  * 能往 `bbos` 里塞假行情、能调 `addPendingOrder` 登记一条框架不知道来源的挂单
  * —— 而框架的停机收尾会去撤它。
  *
  * 这条边界不靠"策略作者别那么写"的约定，靠类型：写方法根本不在这个接口上。
  *
  * 读数不再按交易所取（从前是 `bbo(exchange)`）——本视图就是**一个标的**的状态，
  * 交易所已经在 [[instrument]] 里。跨所策略逐个标的问即可，见 [[InstrumentState]]。
  */
trait InstrumentView:
  def instrument: Instrument
  def exchange: Exchange
  def symbol: Symbol

  def bbo: Option[BBO]
  def markPrice: Option[MarkPrice]
  def indexPrice: Option[IndexPrice]
  def fundingRate: Option[FundingRate]

  def position: Option[Position]
  /** 仓位大小。没有记录 = 空仓，依据见 [[InstrumentState.positionSize]] */
  def positionSize: Coin

  /** 本策略在该标的上的挂单。[[PendingOrder]] 是不可变数据，读它不会动到框架状态 */
  def pendingOrders: Iterable[PendingOrder]
  def hasPendingOrders: Boolean
  def hasPendingSide(side: Side): Boolean

/** 策略能看到的**账户级**状态 —— 只读。
  *
  * 不含 `apply`（事件应用）与 `addPendingOrder`（挂单登记）：那两件事由
  * [[hft.engine.StrategyRunner]] 在固定的位置做，策略插一脚的后果分别是"同一条事件重复
  * 计入仓位"和"登记一条无主挂单"，两者都没有外在症状。
  */
trait StateView:
  /** 某标的的状态视图；标的不在订阅范围内时 `None`。
    *
    * "问了一个没订阅的标的"因此拿不到任何读数，而不是拿到一份看着正常的空状态 ——
    * 从前按交易对索引时做不到这一点，只能在 `positionSize(exchange)` 上加一道运行时守卫。 */
  def instrumentState(instrument: Instrument): Option[InstrumentView]

  /** USDT 余额，None 表示该交易所数据尚未到达 */
  def usdtBalance(exchange: Exchange): Option[Double]
  def totalUsdtBalance: Double

  /** 账户信息 (equity + notional 原子性保证)，None 表示数据尚未到达 */
  def accountInfo(exchange: Exchange): Option[AccountInfo]
  def equity(exchange: Exchange): Option[Double]
  def totalEquity: Double

  /** 账户级期权希腊字母 (含现货修正)。greeks 与该币种余额均到达才返回 */
  def greeks(exchange: Exchange, ccy: String): Option[Greeks]

  /** 这条 greeks 读数到本地多久了 (毫秒) —— **陈旧判断用它**。
    *
    * 载荷里的 `Greeks.timestamp` 是交易所钟 (各家还不一致), 拿它减本地的 `now` 是跨时钟域
    * 相减, 差出来的是"陈旧度 + 时钟偏斜"。见 [[hft.state.StateManager.greeksAt]]。 */
  def greeksAgeMs(exchange: Exchange, ccy: String, now: Timestamp): Option[Long]

  /** 本策略在所有标的上的挂单 */
  def allPendingOrders: Iterable[PendingOrder]
  def hasPendingOrders(instrument: Instrument): Boolean
