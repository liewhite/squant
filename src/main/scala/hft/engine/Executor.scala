package hft.engine

import hft.actor.Actor
import hft.domain.*
import hft.event.Commands.AccountSynced
import hft.event.{AnyEvent, Interest, Subscription}
import hft.strategy.Strategy

/** 策略执行器：把一个 [[Strategy]] 包装成引擎里的 [[Actor]]。
  *
  * 只做两件事 —— 声明策略的订阅范围、把策略产出的事件交出去。策略逻辑与状态维护全在
  * [[StrategyRunner]]，与回测共用同一份 (回测在单线程虚拟时间循环里直接调用同一个 runner)。
  */
final class Executor private (
    strategy: Strategy,
    /** 本实例绑定的账户 —— 装配期决定。同一份策略逻辑可以同时跑实盘与影子盘，
      * 两个实例只有这一处不同，策略代码不必知情。
      *
      * **没有默认值是有意的**：payload 的 account 必填，是因为"漏一处、默认推断成实盘"
      * 是危险侧失效；装配处若留个 `= Live` 的默认值，等于把同一个坑重新挖开 ——
      * 给影子策略装配时忘传账户，它就真金白银在实盘上跑，而编译器不会吭声。 */
    val account: AccountId,
    /** 是否要等启动对齐落地才叫醒策略。见 [[awaiting]] */
    awaitAlignment: Boolean,
) extends Actor:
  private val runner = StrategyRunner(strategy, account)

  /** 还没等到对齐的那些 (账户, 交易所)。空集之前，策略只观察、不动作。
    *
    * ## 为什么闸门在这里，而不是靠引擎的装配顺序
    *
    * 引擎确实是"对齐完成之后才发行情订阅指令"，但那只挡得住**尚未流动**的行情。
    * 一旦某个标的的行情已经在流 —— 撤下一个策略再装回来、实盘与影子先后装载、
    * 扫描器先把标的订上了 —— 新装的策略 spawn 即开始收行情，而它的初始仓位还在
    * 几次 REST 往返之外。那一刻它看到的是"仓位为零、没有挂单"，据此做的第一个决策
    * 就是重复开仓，或者对着不存在的敞口对冲。没有任何症状。
    *
    * 把闸门放在策略这一侧，"对齐先于行情"就从**指令发出顺序**升级为**策略可见顺序**，
    * 上面那几种场景一并覆盖。
    */
  private var awaiting: Set[AccountExchange] = if awaitAlignment then alignmentTargets else Set.empty

  override def name: String = s"executor(${strategy.getClass.getSimpleName}@$account)"

  /** 本策略的订阅范围 = 策略声明 + 框架补齐。引擎据此校验指令有人接、发对齐与行情订阅指令 */
  def subscription: Subscription = runner.subscription

  override def interests: Set[Interest] =
    runner.subscription.interests + Interest.Keyed(AccountSynced, alignmentTargets)

  /** 本策略涉及的 (账户, 交易所) —— 对齐应答按它路由。
    *
    * 由自己算而不是让引擎告知：等谁的对齐，从订阅范围直接读得出来。让装配方传进来的话，
    * 就多了一个"必须在 spawn 之前调用"的时序约定 —— 而这种约定迟早有人漏掉。
    */
  def alignmentTargets: Set[AccountExchange] =
    runner.subscription.exchanges.map(AccountExchange(account, _))

  /** 策略产出什么就发什么 —— 下单意图、也可以是它自己的指标事件。
    * 账户由 [[hft.strategy.StrategyContext]] 在构造下单意图时补上，这里不再包一层。 */
  override def onEvent(event: AnyEvent, now: Timestamp): Vector[AnyEvent] =
    // 对齐应答是给闸门看的, 不归策略 —— 它不在策略的订阅范围里, 别喂给状态机
    event.as(AccountSynced).foreach(report => awaiting -= report.target)
    if !runner.accepts(event) then Vector.empty
    else if awaiting.nonEmpty then
      // 对齐未落地: 状态照收 (排队的事件一条都不能丢), 但先不叫醒策略
      runner.observe(event)
      Vector.empty
    else runner.onEvent(event, now)

  /** 停机收尾：撤掉本策略还挂在交易所的单。
    *
    * 在退订之前发出，那时总线与柜台都还活着。不平仓 —— 平不平、怎么平是**策略之外**
    * 的决定 (换个策略接管、还是真的清掉敞口)，框架替它决定会在撤下实例时制造非预期的市价单。
    */
  override def onStop(now: Timestamp): Vector[AnyEvent] = runner.pendingCancels(now)

object Executor:
  /** 装进引擎的执行器 —— **自带对齐闸门**：初始仓位落地之前只观察、不动作。
    *
    * 闸门在构造时就位，不靠装配方记得调一下 —— spawn 之后事件循环立即开跑，
    * 那种"必须在某步之前调用"的约定迟早有人漏掉。
    */
  def apply(strategy: Strategy, account: AccountId): Executor =
    new Executor(strategy, account, awaitAlignment = true)

  /** 世界已经就绪的执行器 —— **不等对齐**。
    *
    * 给的是那些"前提由调用方自己保证"的场景：单元测试直接喂事件、回测的世界从第一条
    * 行情开始。它们本身就是那个前提，再等一次对齐只会永远等下去。
    *
    * 实盘装配一律走 [[apply]]，那条路上没人替策略保证初始仓位已经到手。
    */
  def readyToTrade(strategy: Strategy, account: AccountId): Executor =
    new Executor(strategy, account, awaitAlignment = false)
