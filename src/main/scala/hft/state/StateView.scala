package hft.state

import hft.domain.*

/** 策略能看到的**单标的**状态 —— 只读。
  *
  * 与 [[SymbolState]]（实现）分开，是因为两者的能力面本就不同：框架要写（应用事件、
  * 登记挂单、超时校验），策略只该读。此前策略拿到的是实现本身，于是能 `positions.clear()`、
  * 能往 `bbos` 里塞假行情、能调 `addPendingOrder` 登记一条框架不知道来源的挂单
  * —— 而框架的停机收尾会去撤它。
  *
  * 这条边界不靠"策略作者别那么写"的约定，靠类型：写方法根本不在这个接口上。
  */
trait SymbolView:
  def symbol: Symbol

  def bbo(exchange: Exchange): Option[BBO]
  def lastTrade(exchange: Exchange): Option[MarketTrade]
  /** 最新成交价 (trade-only 行情下的价格基准) */
  def lastTradePrice(exchange: Exchange): Option[Price]
  def markPrice(exchange: Exchange): Option[MarkPrice]
  def indexPrice(exchange: Exchange): Option[IndexPrice]
  def fundingRate(exchange: Exchange): Option[FundingRate]

  def position(exchange: Exchange): Option[Position]
  /** 仓位大小。无仓位记录等价于空仓 */
  def positionSize(exchange: Exchange): Coin
  def hasPositions: Boolean
  /** 多空仓位大小: (多头总量(正), 空头总量(负)) */
  def positionSizes: (Coin, Coin)

  /** 本策略在该标的上的挂单。[[PendingOrder]] 是不可变数据，读它不会动到框架状态 */
  def pendingOrders: Iterable[PendingOrder]
  def hasPendingOrders: Boolean
  def hasPendingSide(side: Side): Boolean

  /** 日化费率最高/最低的交易所 (跨所资费套利用) */
  def bestShortExchange: Option[(Exchange, FundingRate)]
  def bestLongExchange: Option[(Exchange, FundingRate)]

/** 策略能看到的**账户级**状态 —— 只读。
  *
  * 不含 `apply`（事件应用）与 `addPendingOrder`（挂单登记）：那两件事由
  * [[hft.engine.StrategyRunner]] 在固定的位置做，策略插一脚的后果分别是"同一条事件重复
  * 计入仓位"和"登记一条无主挂单"，两者都没有外在症状。
  */
trait StateView:
  /** 某标的的状态视图；标的不在订阅范围内时 None */
  def symbolState(symbol: Symbol): Option[SymbolView]

  /** USDT 余额，None 表示该交易所数据尚未到达 */
  def usdtBalance(exchange: Exchange): Option[Double]
  def totalUsdtBalance: Double

  /** 账户信息 (equity + notional 原子性保证)，None 表示数据尚未到达 */
  def accountInfo(exchange: Exchange): Option[AccountInfo]
  def equity(exchange: Exchange): Option[Double]
  def totalEquity: Double

  /** 账户级期权希腊字母 (含现货修正)。greeks 与该币种余额均到达才返回 */
  def greeks(exchange: Exchange, ccy: String): Option[Greeks]

  /** 本策略在所有标的上的挂单 */
  def allPendingOrders: Iterable[PendingOrder]
  def hasPendingOrders(symbol: Symbol): Boolean
