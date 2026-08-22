package hft.state

import hft.domain.*
import hft.event.{AnyEvent, Topic, Topics}

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

  /** 按 topic 更新状态。
    *
    * 账户级读数 (余额/净值/希腊值) 与标的事件分两路：前者按交易所存，后者按 symbol 委托
    * 给对应的 [[SymbolState]]。事件能到达这里就说明订阅声明里有它，故标的必然已注册 ——
    * 找不到只可能是路由 bug，立即暴露而不是静默丢弃。
    */
  def apply(event: AnyEvent): Unit =
    event.as(Topics.Balance).foreach { balance =>
      if balance.asset == USDT then balances(balance.exchange) = balance.available
      // 所有币种余额都缓存一份，供 greeks delta 的现货修正使用 (ccy 即 asset)
      cashBalances((balance.exchange, balance.asset)) = balance.available
    }
    event.as(Topics.AccountInfo).foreach(info => accountInfos(info.exchange) = info)
    event.as(Topics.Greeks).foreach(g => greeksRaw((g.exchange, g.ccy)) = g)
    event.as(Topics.Clock).foreach(_ => states.values.foreach(_.failOnTimedOutOrders(event.localTs, orderTimeoutMs)))
    // 只有框架内置的按标的路由 topic 才进 SymbolState。用户自定义的、同样以 Instrument 为 key
    // 的 topic (如订阅别的策略在某标的上的指标) 不代表交易该标的，其标的未必注册过 ——
    // 不加这道判别的话，它的首条事件就会撞上下面的 fail-fast 把引擎拉崩。
    if StateManager.instrumentTopics.contains(event.topic) then
      event.key match
        case instrument: Instrument =>
          states
            .getOrElse(
              instrument.symbol,
              sys.error(
                s"Symbol not found in StateManager (routing bug): $instrument —— " +
                  "策略若要交易该标的, 需为它声明至少一条公共行情 Interest"
              ),
            )
            .apply(event)
        case _ => ()

object StateManager:
  /** 框架内置的按标的路由 topic —— 只有它们的事件进 [[SymbolState]] */
  private val instrumentTopics: Set[Topic[?, ?]] = (Topics.market ++ Topics.instrumentPrivate).toSet
