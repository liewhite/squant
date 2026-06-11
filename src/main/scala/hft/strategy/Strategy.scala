package hft.strategy

import hft.domain.*
import hft.exchange.SubscriptionKind
import hft.messaging.{IncomeEvent, StateManager}

/** 策略输出的信号 */
enum OutcomeEvent:
  /** 下单信号 (一次决策可包含多个关联订单)
    * @param comment 信号意图描述，如 "spread_open | spread=0.30% | qty=10"
    */
  case PlaceOrders(orders: Vector[Order], comment: String)
  /** 撤单信号。撤单的终态确认 (Cancelled) 以私有流推送为准，框架不合成确认事件 */
  case CancelOrder(exchange: Exchange, symbol: Symbol, orderId: OrderId)

/** 策略接口，用户实现此 trait 定义自己的策略逻辑。
  *
  * 策略是纯函数式的：接收事件和状态，返回要执行的动作。
  * onEvent 由框架保证在单一虚拟线程上串行调用，策略内部状态无需同步。
  */
trait Strategy:
  /** 策略需要订阅的公共数据流 */
  def publicStreams: Map[Exchange, Set[SubscriptionKind]]

  /** 订单超时时间 (毫秒): Created 状态超过该时长未获交易所确认则视为丢失，自动清理 */
  def orderTimeoutMs: Long

  /** 处理事件，可产出零到多个信号 */
  def onEvent(event: IncomeEvent, state: StateManager): Vector[OutcomeEvent]
