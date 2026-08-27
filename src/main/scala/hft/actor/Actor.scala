package hft.actor

import hft.domain.Timestamp
import hft.event.{AnyEvent, Interest}

/** 引擎里一个有生命周期的组件。
  *
  * 全框架只有这一种组件形态 —— 策略执行器、下单出口、时钟、账户轮询、虚拟柜台、监控与
  * 指标导出，都是它的实现。此前这些各写各的 `fork { while true ... }`，新增一个就要回到
  * 引擎装配处插一段代码；现在只需实现本 trait 并 [[ActorSystem.spawn]]。
  *
  * ## 两种形态，一个 trait
  *
  *   - **事件驱动** (策略、下单出口、指标 sink)：声明 [[interests]]，在 [[onEvent]] 里
  *     消费并产出事件。这是纯函数形态 —— 可单测、可回测、可被动态起停。
  *   - **自驱动** (WS 连接、定时器、REST 轮询)：在 [[onStart]] 里 fork 自己的常驻线程。
  *
  * 一个 actor 可以两者兼有 (例如虚拟柜台既消费下单意图，又自驱动地转发行情)。
  *
  * ## 停机
  *
  * [[onStop]] 调用一次，可以产出最后一批事件 (撤单、平仓意图) —— 那时总线与下游消费者
  * 都还活着。它在**退订与排空邮箱之后**跑：邮箱里积压的事件先被处理完，再收尾。
  *
  * 三条路都会走到它：显式 [[ActorSystem.stop]]、[[ActorSystem.requestShutdown 请求停机]]
  * (含中断信号)、以及**任何组件的失败**。最后一条是有意的 —— 私有流断线一类的子任务失败
  * 从前直接炸穿作用域、把所有 `onStop` 一并跳过，于是撤单指令漏发、挂单留在交易所无人
  * 跟踪。现在失败先触发一遍有序停机 (逆装配序, 每个 `onStop` 都跑到)，收尾完了才让进程
  * 非零退出：**fail-fast 与"收尾要跑到"不冲突，只是先后问题**。
  *
  * **[[onStart]] 里 fork 的线程不受 [[ActorSystem.stop]] 控制**，它们的生命周期绑在根作用域
  * 上，随进程结束。要能被动态起停的 actor 必须走事件驱动形态，或者在自己的循环里用
  * [[ActorContext.sleepUnlessStopped]] 代替裸 sleep。这条限制是如实的：一个阻塞在 socket
  * 读上的线程没有办法被协作式地叫停，而假装能停会让停机链在那里静默地等下去。
  */
trait Actor:
  /** 诊断用名字，进日志与错误信息 */
  def name: String

  /** 要收哪些事件。空集 = 纯生产者，不消费任何事件 */
  def interests: Set[Interest] = Set.empty

  /** 启动钩子：fork 自己的常驻线程、spawn 子 actor。在开始消费事件之前调用一次 */
  def onStart(ctx: ActorContext): Unit = ()

  /** 处理一条事件，产出零到多条新事件 (由框架发布到总线) */
  def onEvent(event: AnyEvent, now: Timestamp): Vector[AnyEvent] = Vector.empty

  /** 停机收尾：产出最后一批事件。此时总线与下游都还活着 */
  def onStop(now: Timestamp): Vector[AnyEvent] = Vector.empty
