package hft.engine

import hft.actor.Actor
import hft.domain.*
import hft.event.{AnyEvent, Interest, Subscription}
import hft.strategy.Strategy

/** 策略执行器：把一个 [[Strategy]] 包装成引擎里的 [[Actor]]。
  *
  * 只做两件事 —— 声明策略的订阅范围、把策略产出的事件交出去。策略逻辑与状态维护全在
  * [[StrategyRunner]]，与回测共用同一份 (回测在单线程虚拟时间循环里直接调用同一个 runner)。
  */
final class Executor(
    strategy: Strategy,
    /** 本实例绑定的账户 —— 装配期决定。同一份策略逻辑可以同时跑实盘与影子盘，
      * 两个实例只有这一处不同，策略代码不必知情。
      *
      * **没有默认值是有意的**：payload 的 account 必填，是因为"漏一处、默认推断成实盘"
      * 是危险侧失效；装配处若留个 `= Live` 的默认值，等于把同一个坑重新挖开 ——
      * 给影子策略装配时忘传账户，它就真金白银在实盘上跑，而编译器不会吭声。 */
    val account: AccountId,
) extends Actor:
  private val runner = StrategyRunner(strategy, account)

  override def name: String = s"executor(${strategy.getClass.getSimpleName}@$account)"

  /** 本策略的订阅范围 = 策略声明 + 框架补齐。引擎据此校验指令有人接、发对齐与行情订阅指令 */
  def subscription: Subscription = runner.subscription

  override def interests: Set[Interest] = runner.subscription.interests

  /** 策略产出什么就发什么 —— 下单意图、也可以是它自己的指标事件。
    * 账户由 [[hft.strategy.StrategyContext]] 在构造下单意图时补上，这里不再包一层。 */
  override def onEvent(event: AnyEvent, now: Timestamp): Vector[AnyEvent] = runner.onEvent(event, now)

  /** 停机收尾：撤掉本策略还挂在交易所的单。
    *
    * 在退订之前发出，那时总线与柜台都还活着。不平仓 —— 平不平、怎么平是**策略之外**
    * 的决定 (换个策略接管、还是真的清掉敞口)，框架替它决定会在撤下实例时制造非预期的市价单。
    */
  override def onStop(now: Timestamp): Vector[AnyEvent] = runner.pendingCancels(now)
