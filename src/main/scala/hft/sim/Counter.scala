package hft.sim

import hft.domain.*
import hft.event.AnyEvent

/** 一条待投递的柜台输出：事件本身 + 它该延迟多久才送达。
  *
  * 延迟是**柜台语义**的一部分（订单在途、回报回传都要花时间），不是某个驱动的实现细节。
  * 把它做成数据之后，三个驱动就只剩两点差别：现在几点、事件往哪送。
  */
final case class Delayed[+A](delayMs: Long, value: A)

/** 柜台的一条输入命令 —— 三个驱动共用的形态。
  *
  * 此前 [[hft.backtest.BacktestEngine]]、[[SimulatedExchange]]、[[PaperCounter]] 各自声明了
  * 一份三个 case 完全相同的 enum。同一个概念三处定义，给撮合加一个入口就要记得改三遍，
  * 而漏掉一处不会编译失败 —— 那一处只是从此收不到这类命令。
  */
enum CounterInput:
  /** 上游行情到达（实时，用于撮合） */
  case Market(ev: AnyEvent)
  /** 策略的下单在途结束，抵达撮合 */
  case OrderArrived(order: Order, orderId: OrderId)
  /** 策略的撤单在途结束，抵达撮合 */
  case CancelArrived(ref: OrderRef)

/** 虚拟柜台的纯核心：撮合 + 延迟决策。
  *
  * 三个驱动共用它，于是它们的差异只剩两件事 —— **现在几点**（回测虚拟时间 / 实盘墙钟）与
  * **事件往哪送**（回测入优先队列 / 实盘进定时器再发总线）。
  *
  * 为什么延迟归核心：影子盘存在的理由是预测实盘，回测存在的理由是预测两者。三者若各写一份
  * 延迟模型，改了其中一份不会有任何编译错误，它们只是从此对同一个策略给出不同结论 ——
  * 而"结论应当一致"正是这套东西全部的价值所在。语义放一处，怎么等留给驱动。
  *
  * 什么**没**收进来：orderId 生成。三个驱动各自一个 `var` 自增就够 —— 柜台插件的下单在
  * 自己的 actor 线程上串行处理，回测在单线程里推进，都不需要原子量；回测还额外要求它
  * 确定性自增才能复现。各一行、机制与驱动绑定，收进核心只会逼出一个"把 id 生成器传进来"
  * 的参数，不会让任何一处更简单。
  */
object Counter:

  /** 撮合一条命令：落地新状态，产出带回传延迟的回报。
    *
    * 输出**不含行情回显** —— 转发行情是网关职责，见 [[SimState.onMarket]] 的说明。
    */
  def step(
      state: SimState,
      exchange: Exchange,
      input: CounterInput,
      now: Timestamp,
      config: SimConfig,
  ): (SimState, Vector[Delayed[AnyEvent]]) =
    val (next, replies) = input match
      case CounterInput.Market(ev)              => state.onMarket(exchange, ev, now)
      case CounterInput.OrderArrived(order, id) => state.onOrderArrived(exchange, order, id, now)
      case CounterInput.CancelArrived(ref)      => state.onCancelArrived(exchange, ref, now)
    (next, replies.map(toStrategy(config, _)))

  /** 交易所侧事件回传到策略的延迟。
    * 撮合回报走它，扮演网关时转发的行情也走它 —— 两者都是"交易所那边发生的事传到策略手上"。 */
  def toStrategy[A](config: SimConfig, value: A): Delayed[A] =
    Delayed(config.exchangeToStrategyDelayMs, value)

  /** 策略的下单/撤单抵达撮合前的在途延迟 —— 与回传延迟对称，同属柜台语义。
    *
    * 只用于 [[CounterInput.OrderArrived]] / [[CounterInput.CancelArrived]]。行情不走这条路
    * （它来自上游而非策略），给它打上在途延迟是没有意义的。
    */
  def inbound(config: SimConfig, input: CounterInput): Delayed[CounterInput] =
    Delayed(config.orderToExchangeDelayMs, input)
