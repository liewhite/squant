package hft.messaging

import hft.domain.*

import scala.collection.mutable

/** 状态管理器 - 每个策略 (Executor) 独享一份，管理其订阅范围内的全部交易状态。
  *
  * 可变状态，仅在所属 Executor 的虚拟线程内访问，无需同步。
  */
final class StateManager(symbols: Iterable[Symbol], orderTimeoutMs: Long):
  private val states: Map[Symbol, SymbolState] =
    symbols.map(s => s -> SymbolState(s)).toMap
  private val balances: mutable.Map[Exchange, Double] = mutable.Map.empty
  private val accountInfos: mutable.Map[Exchange, AccountInfo] = mutable.Map.empty
  /** 原始账户级希腊字母 (按 (交易所, 币种) 索引)，delta 未含现货修正 */
  private val greeksRaw: mutable.Map[(Exchange, String), Greeks] = mutable.Map.empty
  /** 各币种现金余额 (按 (交易所, 币种) 索引)，用于修正 greeks delta 的现货敞口 */
  private val cashBalances: mutable.Map[(Exchange, String), Double] = mutable.Map.empty

  // ==================== 下单接口 ====================

  /** 添加 pending order (由 StrategyRunner 调用，clientOrderId 已生成)。
    * `now` 为当前处理时刻 (回测虚拟时间 / 实盘墙钟)，作为 createdAt 超时检测基准。
    * symbol 不在订阅范围内时抛异常 (表示策略配置错误，应立即暴露)
    */
  def addPendingOrder(order: Order, now: Timestamp): Unit =
    states
      .getOrElse(order.symbol, sys.error(s"Symbol not found in StateManager: ${order.symbol}"))
      .addPendingOrder(order, now)

  // ==================== 状态查询 ====================

  def symbolState(symbol: Symbol): Option[SymbolState] = states.get(symbol)

  /** USDT 余额，None 表示该交易所数据尚未到达 */
  def usdtBalance(exchange: Exchange): Option[Double] = balances.get(exchange)

  def totalUsdtBalance: Double = balances.values.sum

  /** 账户信息 (equity + notional 原子性保证)，None 表示数据尚未到达 */
  def accountInfo(exchange: Exchange): Option[AccountInfo] = accountInfos.get(exchange)

  def equity(exchange: Exchange): Option[Double] = accountInfos.get(exchange).map(_.equity)

  def totalEquity: Double = accountInfos.values.map(_.equity).sum

  def accountNotional(exchange: Exchange): Option[Double] = accountInfos.get(exchange).map(_.notional)

  def totalAccountNotional: Double = accountInfos.values.map(_.notional).sum

  /** 账户级期权希腊字母 (含现货修正)。
    *
    * 返回的 delta = 原始期权 delta + 该币种现金余额 (cashBal)，即叠加现货敞口后的总 delta。
    * 仅当 greeks 与 cashBal 均已到达时返回 Some——缺任一项都无法给出正确的总敞口 (与参考实现一致)。
    */
  def greeks(exchange: Exchange, ccy: String): Option[Greeks] =
    for
      g <- greeksRaw.get((exchange, ccy))
      cashBal <- cashBalances.get((exchange, ccy))
    yield g.copy(delta = g.delta + cashBal)

  def hasPendingOrders(symbol: Symbol): Boolean =
    states.get(symbol).exists(_.hasPendingOrders)

  // ==================== 事件处理 ====================

  /** 处理事件，更新状态 */
  def apply(event: IncomeEvent): Unit = event.data match
    case EventData.BalanceUpdate(balance) =>
      if balance.asset == USDT then balances(balance.exchange) = balance.available
      // 所有币种余额都缓存一份，供 greeks delta 的现货修正使用 (ccy 即 asset)
      cashBalances((balance.exchange, balance.asset)) = balance.available
    case EventData.AccountInfoUpdate(exchange, info) =>
      accountInfos(exchange) = info
    case EventData.GreeksUpdate(g) =>
      greeksRaw((g.exchange, g.ccy)) = g
    case EventData.Clock =>
      states.values.foreach(_.failOnTimedOutOrders(event.localTs, orderTimeoutMs))
    case _ =>
      // Symbol 事件: 委托对应 SymbolState 处理。
      // 事件已由 Executor 按 (exchange, symbol) 过滤，symbol 必然已注册，
      // 查找失败说明路由逻辑有 bug，应立即暴露
      val symbol = event.symbol.getOrElse(sys.error(s"Symbol event must have symbol: $event"))
      states
        .getOrElse(symbol, sys.error(s"Symbol not found in StateManager (routing bug): $symbol"))
        .apply(event)
