package hft.strategy

import hft.domain.*
import hft.event.{AnyEvent, Interest, Topic}
import hft.state.StateManager

/** 策略输出的信号 */
enum OutcomeEvent:
  /** 下单信号 (一次决策可包含多个关联订单)
    * @param comment 信号意图描述，如 "spread_open | spread=0.30% | qty=10"
    */
  case PlaceOrders(orders: Vector[Order], comment: String)
  /** 撤单信号。撤单的终态确认 (Cancelled) 以私有流推送为准，框架不合成确认事件 */
  case CancelOrder(exchange: Exchange, symbol: Symbol, orderId: OrderId)

/** 策略信号的事件族。
  *
  * 定义在这里而不是 [[hft.event.Topics]]：载荷 [[OutcomeEvent]] 属于策略层，而 `hft.event`
  * 是它的下游依赖 —— 放进框架内置 topic 会造出一条反向依赖边。这正是 [[Topic]] 作为
  * **开放扩展点**的用法：任何模块都能定义自己的事件族，框架无需知情。
  *
  * 无路由维度 (`K = Unit`)：一次决策可以包含跨交易所、跨标的的多张订单，没有单一的路由键。
  * 执行出口用 `Interest.All(OrderIntent)` 订阅，策略不订阅它，故信号不会回流给任何策略。
  */
object OrderIntent extends Topic[Unit, OutcomeEvent]("orderIntent"):
  def keyOf(payload: OutcomeEvent): Unit = ()

/** 策略接口，用户实现此 trait 定义自己的策略逻辑。
  *
  * 策略是纯函数式的：接收事件和状态，返回要执行的动作。
  * onEvent 由框架保证在单一虚拟线程上串行调用，策略内部状态无需同步。
  */
trait Strategy:
  /** 策略要收哪些事件。
    *
    * 只需声明**公共行情**与用户自定义 topic，且公共行情必须用 [[Interest.Keyed]] 精确到
    * 标的 (框架要据此决定向交易所订阅哪些流，[[Interest.All]] 给不出这个信息)。
    *
    * 框架会自动补齐三类**不该由策略选择**的订阅，见 [[hft.engine.StrategyRunner]]：
    *   - 所声明标的的私有回报 (持仓/订单回报/成交) —— 漏订会让本地仓位与交易所发散；
    *   - 所涉交易所的账户级读数 (余额/净值/希腊值)；
    *   - 时钟 (订单超时检测的驱动)。
    */
  def interests: Set[Interest]

  /** 订单超时时间 (毫秒): Created 状态超过该时长未获交易所确认则视为丢失，自动清理 */
  def orderTimeoutMs: Long

  /** 处理事件，可产出零到多个信号 */
  def onEvent(event: AnyEvent, state: StateManager): Vector[OutcomeEvent]
