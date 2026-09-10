package hft.exchange

import hft.domain.*
import org.slf4j.LoggerFactory

/** 只读替身：查询照常走真实交易所，**任何会改变账户的调用一律拒绝**。
  *
  * 用来验证接线 —— 行情流、状态聚合、策略产出信号、pending 清理这整条链路都真跑，只是订单出不去。
  *
  * ## 为什么是一个客户端实现，而不是一个 `dryRun` 开关
  *
  * 从前这是中心化下单出口的构造参数，在下单和撤单两处 `if dryRun then ...`。
  * 但"不真交易"在本框架里**已经有一个多态答案**：换掉 gateway（[[hft.sim.SimulatedExchange]]
  * 就是这么做的，策略对真假无感知）。同一个概念两套机制，其中一套还是布尔开关。
  *
  * 换成实现之后，下单出口一个分支都不剩，而且**不需要新增任何路径**：拒单走的正是既有的
  * "交易所明确拒绝 (4xx)"通道 —— 订单确定未成立、以 `OrderUpdate(Error)` 回流策略、pending
  * 被清理，与真实拒单逐字同构。
  *
  * @param delegate 真实交易客户端（有凭证；dry-run 只拦写入，读取照常）。只读调用透传给它，dry-run 看到的行情与账户读数都是真的
  */
final class DryRunClient(delegate: TradingClient) extends TradingClient:
  private val logger = LoggerFactory.getLogger(classOf[DryRunClient])

  override def exchange: Exchange = delegate.exchange

  // ==================== 只读：透传 ====================

  override def fetchAllSymbolMetas(): Either[ExchangeError, Vector[SymbolMeta]] = delegate.fetchAllSymbolMetas()
  override def fetchPendingOrders(instrument: Instrument): Either[ExchangeError, Vector[OrderUpdate]] = delegate.fetchPendingOrders(instrument)
  override def fetchAccountInfo(): Either[ExchangeError, AccountInfo] = delegate.fetchAccountInfo()
  override def fetchWallet(): Either[ExchangeError, Map[String, Double]] = delegate.fetchWallet()
  override def fetchPositions(): Either[ExchangeError, Vector[Position]] = delegate.fetchPositions()

  // ==================== 写入：拒绝 ====================

  /** 以**明确拒绝**形态返回 —— 让它走既有的"确定性失败"通道，而不是新造一条 dry-run 专用路径。
    *
    * 这也正是 dry-run 的事实：订单确定没有成立。 */
  override def placeOrder(order: ExchangeOrder): Either[ExchangeError, OrderId] =
    logger.warn(
      s"[DRY-RUN] 未下单: ${order.exchange} ${order.symbol} ${order.side} ${order.orderType} " +
        s"qty=${order.quantity.value} reduceOnly=${order.reduceOnly} clientOrderId=${order.clientOrderId}"
    )
    Left(ExchangeError.Rejected("dry-run", "order not placed"))

  /** dry-run 下没有真实挂单可撤，回 `OrderNotFound` —— 这既是事实，也正好落在
    * [[TradingGateway]] 既有的容忍分支上（撤一张已不存在的单非致命）。 */
  override def cancelOrder(instrument: Instrument, ref: OrderRef): Either[ExchangeError, Unit] =
    logger.warn(s"[DRY-RUN] 未撤单: $instrument ${ref.raw}")
    Left(ExchangeError.OrderNotFound("dry-run: no live order to cancel"))

