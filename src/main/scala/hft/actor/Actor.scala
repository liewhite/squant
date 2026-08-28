package hft.actor

import hft.domain.Timestamp
import hft.event.{AnyEvent, CommandHandler, Interest}
import hft.kernel.CapabilityProvider

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
  * 跟踪。现在失败先触发一遍依赖拓扑停机 (每个 `onStop` 都跑到)，收尾完了才让进程
  * 非零退出：**fail-fast 与"收尾要跑到"不冲突，只是先后问题**。
  *
  * [[ActorContext.fork]] 创建的任务受组件作用域管理：停止时先发停止信号并中断，资源释放后
  * 等待退出；超时或异常都会成为可见的停机失败。外部连接等资源用 [[ActorContext.manage]]
  * 登记，框架会在 `onStop` 之后逆序释放，避免任务或连接悄悄活过所属组件。
  */
trait Actor:
  /** 诊断用名字，进日志与错误信息 */
  def name: String

  /** 要收哪些事件。空集 = 纯生产者，不消费任何事件 */
  def interests: Set[Interest] = Set.empty

  /** 本组件实际执行的命令。声明本身同时建立投递与能力，不从普通订阅方式猜测。 */
  def commandHandlers: Set[CommandHandler] = Set.empty

  /** 本组件提供的非命令能力。命令能力由 [[commandHandlers]] 自动派生，无需重复声明。 */
  def capabilities: Set[CapabilityProvider] = Set.empty

  /** 正常运行所必需的命令处理能力。
    *
    * 消息流不能推出硬依赖：观察者订阅一条行情，不代表没有它就必须停；策略能发布下单命令，
    * 却无法从 `interests` 看出它依赖柜台。因此硬依赖单独声明，由系统在任何启动副作用之前校验。
    */
  def requirements: Set[Requirement] = Set.empty

  /** 可回滚装配钩子：建立本地状态、登记资源、同步 spawn 子组件。
    *
    * 此阶段命令处理能力尚不可见，且禁止发布命令。失败时框架会回收整批组件，因此这里不能
    * 执行无法由 [[onStop]] 或 [[ActorContext.manage]] 补偿的外部副作用。
    */
  def onPrepare(ctx: ActorContext): Unit = ()

  /** 启动钩子：建立外部连接和受管任务；处理能力与总线输出仍属于未提交事务。
    *
    * `publish` 的消息先缓存在本组件，整批钩子成功后才与命令处理能力一起对外可见。外部 IO
    * 则无法事务回滚：若同批其他组件启动失败，框架会调用 [[onStop]] 补偿。
    */
  def onStart(ctx: ActorContext): Unit = ()

  /** 处理一条事件，产出零到多条新事件 (由框架发布到总线) */
  def onEvent(event: AnyEvent, now: Timestamp): Vector[AnyEvent] = Vector.empty

  /** 停机收尾：产出最后一批事件。此时总线与下游都还活着 */
  def onStop(now: Timestamp): Vector[AnyEvent] = Vector.empty
