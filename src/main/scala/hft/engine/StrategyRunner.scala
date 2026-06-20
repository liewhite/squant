package hft.engine

import hft.domain.*
import hft.messaging.{IncomeEvent, StateManager}
import hft.strategy.{OutcomeEvent, Strategy}

/** 策略执行的**纯逻辑核心**：把一个事件喂给策略、维护策略独享状态、产出"已按交易所精度转换"的信号。
  *
  * 不含任何传输/并发设施 (无 fork / channel / bus)，因此可被两种驱动复用：
  *   - 实盘/模拟盘 [[Executor]]：虚拟线程串行消费总线事件，调用 [[onEvent]] 后把结果发布到 outcomeBus
  *   - 回测 [[hft.backtest.BacktestEngine]]：单线程虚拟时间循环里同步调用 [[accepts]]/[[onEvent]]
  *
  * 职责与原 Executor.handle 完全一致：过滤订阅范围、更新 StateManager、分配 clientOrderId、
  * 登记 pending、按 SymbolMeta 转换 (币本位->张数、价格/数量取整)。行为不变。
  *
  * @param clientOrderIdGen client_order_id 生成器 (按交易所格式)。实盘默认用 UUID 保唯一；
  *   回测注入确定性自增计数 (见 [[StrategyRunner.backtest]])，使逐笔回报/CSV 跨运行可复现。
  */
final class StrategyRunner(
    strategy: Strategy,
    symbolMetas: Map[(Exchange, Symbol), SymbolMeta],
    clientOrderIdGen: Exchange => String = _.newClientOrderId,
):
  /** 策略订阅的 (exchange, symbol) 集合，用于事件过滤 */
  val subscriptions: Set[(Exchange, Symbol)] =
    strategy.publicStreams.toSet.flatMap { (exchange, kinds) =>
      kinds.map(k => (exchange, k.subscribedSymbol))
    }

  val state: StateManager = StateManager(subscriptions.map(_._2), strategy.orderTimeoutMs)

  /** 全局事件 (无路由键) 广播；symbol 事件仅接收订阅范围内的 */
  def accepts(event: IncomeEvent): Boolean =
    event.routing.forall(subscriptions.contains)

  /** 更新状态并运行策略，返回**已转换为交易所格式**的信号 (下单已分配 id、登记 pending、取整)。
    * `now` 为当前处理时刻 (回测虚拟时间 / 实盘墙钟)，作为 pending order 的 createdAt (超时检测基准)。
    */
  def onEvent(event: IncomeEvent, now: Timestamp): Vector[OutcomeEvent] =
    state.apply(event)
    strategy.onEvent(event, state).map {
      case OutcomeEvent.PlaceOrders(orders, comment) =>
        val converted = orders.map { order =>
          val withId = order.copy(clientOrderId = clientOrderIdGen(order.exchange))
          // 原始订单 (币本位) 登记到 pending，策略端统一看到币的数量
          state.addPendingOrder(withId, now)
          // 转换为交易所格式 (合约张数 + 价格/数量取整)
          convertOrder(withId)
        }
        OutcomeEvent.PlaceOrders(converted, comment)
      case cancel: OutcomeEvent.CancelOrder => cancel
    }

  /** 币本位数量 -> 合约张数，价格/数量按交易所精度取整。
    * 缺少 SymbolMeta 说明策略交易了未预加载的 symbol，是配置错误，立即终止
    */
  private def convertOrder(order: Order): Order =
    val meta = symbolMetas.getOrElse(
      (order.exchange, order.symbol),
      sys.error(s"SymbolMeta not found for ${order.exchange} ${order.symbol}, cannot convert order"),
    )
    val quantity = meta.roundSizeDown(meta.coinToQty(order.quantity))
    val orderType = order.orderType match
      case OrderType.Market            => OrderType.Market
      case OrderType.Limit(price, tif) => OrderType.Limit(meta.roundPrice(price), tif)
    order.copy(quantity = quantity, orderType = orderType)

object StrategyRunner:
  /** 回测用确定性 client_order_id 生成器：自增计数 bt0/bt1/...，使逐笔回报/CSV 跨运行可复现。
    * 有状态闭包 (单回测为单线程，无并发问题)；每个 runner 独享一份，自 0 起算。 */
  def deterministicIdGen(): Exchange => String =
    var counter = 0L
    (_: Exchange) =>
      val id = s"bt$counter"
      counter += 1
      id

  /** 回测专用工厂：注入确定性 client_order_id 生成器 (杜绝 UUID 随机带来的不可复现)。 */
  def backtest(strategy: Strategy, symbolMetas: Map[(Exchange, Symbol), SymbolMeta]): StrategyRunner =
    StrategyRunner(strategy, symbolMetas, deterministicIdGen())
