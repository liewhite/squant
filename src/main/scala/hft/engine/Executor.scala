package hft.engine

import hft.actor.Actor
import hft.domain.*
import hft.event.{AnyEvent, Event, Interest, Subscription}
import hft.strategy.{OrderIntent, Strategy}

/** 策略执行器：把一个 [[Strategy]] 包装成引擎里的 [[Actor]]。
  *
  * 只做两件事 —— 声明策略的订阅范围、把策略产出的信号包成 [[OrderIntent]] 事件。
  * 策略逻辑、状态维护与精度转换全在 [[StrategyRunner]]，与回测共用同一份
  * (回测在单线程虚拟时间循环里直接调用同一个 runner)。
  *
  * 此前它自带 `fork { while true ... }` 与总线引用；现在这些都归 [[hft.actor.ActorSystem]]，
  * 于是策略实例可以被动态起停 —— 这正是"按模拟盘表现起停实盘策略"的着力点。
  */
final class Executor(strategy: Strategy, symbolMetas: Map[(Exchange, Symbol), SymbolMeta]) extends Actor:
  private val runner = StrategyRunner(strategy, symbolMetas)

  override def name: String = s"executor(${strategy.getClass.getSimpleName})"

  /** 本策略的订阅范围 = 策略声明 + 框架补齐。引擎据此向交易所订阅行情、做启动对齐 */
  def subscription: Subscription = runner.subscription

  override def interests: Set[Interest] = runner.subscription.interests

  override def onEvent(event: AnyEvent, now: Timestamp): Vector[AnyEvent] =
    runner.onEvent(event, now).map(outcome => Event.local(OrderIntent, outcome))

  /** 停机收尾：撤掉本策略还挂在交易所的单。
    *
    * 在退订之前发出，那时总线与下单出口都还活着。不平仓 —— 平不平、怎么平是**策略之外**
    * 的决定 (换个策略接管、还是真的清掉敞口)，框架替它决定会在撤下实例时制造非预期的市价单。
    */
  override def onStop(now: Timestamp): Vector[AnyEvent] =
    runner.pendingCancels.map(outcome => Event.local(OrderIntent, outcome))
