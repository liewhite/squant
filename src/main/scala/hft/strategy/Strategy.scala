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

  /** 订单超时时间 (毫秒): Created 状态超过该时长未获交易所确认则视为丢失，自动清理 */
  def orderTimeoutMs: Long
