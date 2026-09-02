package hft.strategy

/** 策略接口。
  *
  * 策略是**纯函数式**的：处理器接收事件与状态、返回要发出的事件，不碰总线、不起线程。
  * 框架保证同一策略实例的处理器在单一虚拟线程上串行调用，内部可变状态无需同步。
  *
  * ## 只需声明一处
  *
  * [[handlers]] 同时给出"订阅什么"与"怎么处理" —— 订阅声明从处理器派生，
  * 因此不存在"订阅了不处理"或"处理了没订阅"。处理器拿到的载荷已是具体类型，
  * 不需要模式匹配、也不会漏掉兜底分支。
  *
  * {{{
  * def handlers = StrategyHandlers.empty
  *   .market(Topics.Bbo, instrument) { (bbo, ctx, now) => ctx.place(myOrder(bbo), "quote") }
  *   .own(Topics.Fill)               { (fill, ctx, now) => Vector.empty }
  *   .account(Topics.Greeks)         { (g, ctx, now) => if g.ccy == ccy then hedge(ctx) else Vector.empty }
  * }}}
  *
  * ## 账户无关
  *
  * 策略不知道自己跑在实盘还是影子账户上 —— 同一份逻辑要能两边同时跑。私有回报用
  * `own`/`account` 以账户无关的语气声明，装配期才绑定；下单经 [[StrategyContext.place]]，
  * 账户由它补。
  *
  * 下单指令的形态见 [[hft.event.Commands]] —— 它属于**指令面契约**（策略与柜台之间），
  * 不属于策略层：定义在这里的话，柜台插件就得反过来依赖策略包。
  */
trait Strategy:
  /** 订阅与处理 —— 一处声明 */
  def handlers: StrategyHandlers

  /** 下单请求的确认超时 (毫秒)。`0` = **关闭这项校验**，见下。
    *
    * ## 超时的后果是终止进程，不是"自动清理"
    *
    * `Created` 状态超过该时长仍未获交易所确认 -> 抛错终止。
    *
    * 理由：REST 有更短的超时 (见 `hft.exchange.RestTransport.ReadTimeout`)，所以正常情况下
    * 一次下单要么明确成功 (私有流推确认)、要么明确失败 (拒单回报清掉 pending)。走到这个超时
    * 说明订单**结果不确定** —— 清理后重下会让敞口翻倍，唯一安全的做法是终止，由重启后的
    * 启动对齐恢复一致 (见 `hft.state.SymbolState.failOnTimedOutOrders`)。
    *
    * 这句话此前写的是"视为丢失、自动清理"，与实现完全相反：按文档去配这个值的策略作者，
    * 会以为超时是一次无害的本地清理。
    *
    * ## `0` = 关闭，且只允许在回测/测试里
    *
    * 回测与单测里下单确认是同步的，不存在"结果不确定"这种状态，那道校验没有对象。
    * 这条语义此前只存在于实现的 `if timeoutMs > 0` 里，契约一个字没提 —— 于是一个实盘策略
    * 把它配成 0 就静默关掉了唯一能发现"订单结果不确定"的机制。
    * 现在实盘装配路径 (`Executor.apply`) 显式拒绝 0，见那里的 require。 */
  def orderTimeoutMs: Long

  /** 就绪 —— 在看到第一条事件之前把自己准备好，**允许阻塞**。
    *
    * 由 [[hft.engine.Executor]] 在事件循环开跑之前同步调用一次。指标预热 (拉历史 K 线喂热
    * MACD/ATR/σ) 这类"需要时间才能就绪"的事情放这里。
    *
    * ## 为什么是策略的职责而不是装配方的
    *
    * 从前预热由启动器负责：拉 K 线、调 `strategy.prewarmXxx(bars)`、处理失败，三个启动器
    * 各写一遍。**忘了调没有任何症状** —— 策略照跑，只是在头几十根 bar 里按退化的参数交易
    * (MACD 方向恒为 0 使死区退化为对称、σ 取下限使对冲偏频)。而回测那边又靠"多拉一段
    * warmup 数据"实现同一件事：同一个需求两种实现，虚实分叉。
    *
    * "必须在装配的某一步之前调用"这种约定迟早有人漏掉，所以把它收进组件自己 ——
    * 与对齐闸门从装配期搬进 [[hft.engine.Executor]] 构造器是同一个道理。
    *
    * ## 阻塞是安全的
    *
    * 此刻邮箱已经接线，但事件循环与命令能力仍在启动事务的栅栏后。阻塞只推迟整批提交，
    * 不会漏掉接线后到达的观察事件。它与对齐闸门管的是两件事：这里是"策略自己就绪没有"，
    * 闸门是"初始仓位到手没有"，两道各司其职。
    *
    * **失败要不要致命由策略决定**：降级运行 (记一条说清后果的 error) 还是抛异常终止进程，
    * 框架不替它选 —— 预热缺席对不同策略的代价不一样。
    */
  def prepare(): Unit = ()

object Strategy:
  /** 下单确认超时的**推荐默认值**。
    *
    * 它衡量的是"下单请求发出到拿到交易所确认"这一跳的时延上界，因此与报价 TTL、重挂节奏、
    * 挂单能挂多久**都无关** —— 已确认的 resting 单本来就豁免这项校验
    * (见 `hft.state.SymbolState.failOnTimedOutOrders` 只检查 `OrderStatus.Created`)。
    *
    * 三个策略此前都按"订单超时需 > requote, 否则正常挂单会被当超时清理"来配它 (于是取了
    * TTL 的 3 倍、甚至 1 年)，那句话来自一份写错的契约：把"确认超时"当成了"挂单存活上限"。
    *
    * 取值依据：REST 读超时是 3s (`hft.exchange.RestTransport.ReadTimeout`)，留几倍余量给
    * 私有流推送的抖动。真正被强制的关系是 `orderTimeoutMs > ReadTimeout`，由实盘装配路径
    * (`hft.engine.Executor.apply`) 校验 —— 这里只是一个不必每个策略各想一遍的推荐值。 */
  val RecommendedOrderTimeoutMs: Long = 15_000
