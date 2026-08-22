package hft.strategy

import hft.domain.*
import hft.event.Topic

/** 策略输出的信号 */
enum OutcomeEvent:
  /** 下单信号 (一次决策可包含多个关联订单)
    * @param comment 信号意图描述，如 "spread_open | spread=0.30% | qty=10"
    */
  case PlaceOrders(orders: Vector[Order], comment: String)
  /** 撤单信号。撤单的终态确认 (Cancelled) 以私有流推送为准，框架不合成确认事件。
    *
    * 用 [[OrderRef]] 而非裸 id 指名订单：在途单还没有交易所 id，只能按 clientOrderId 撤。
    */
  case CancelOrder(exchange: Exchange, symbol: Symbol, ref: OrderRef)

/** 带账户归属的策略信号 —— 一次决策要发往哪个账户执行。
  *
  * 构造器限定 `private[hft]`：策略在 `strategy.*` 包里，因此**无法自己拼一条下单意图** ——
  * 既绕不过 [[StrategyContext.place]] 的 clientOrderId 生成 / pending 登记 / 精度换算，
  * 也冒充不了别的账户下单。后者是账户隔离这条防线上原本唯一敞开的口子。
  * 读侧（订阅、`as`、模式匹配）不受影响。
  */
final case class AccountOutcome private[hft] (account: AccountId, outcome: OutcomeEvent)

/** 策略信号的事件族。
  *
  * 定义在这里而不是 [[hft.event.Topics]]：载荷属于策略层，而 `hft.event` 是它的下游依赖
  * —— 放进框架内置 topic 会造出一条反向依赖边。这正是 [[Topic]] 作为**开放扩展点**的用法：
  * 任何模块都能定义自己的事件族，框架无需知情。
  *
  * **按账户路由**：实盘出口订阅 `Keyed(OrderIntent, {Live})`，每个虚拟柜台订阅自己那个
  * `Paper(n)`。于是"这条信号该由谁执行"由投递层回答，不需要每个出口再自己判一次
  * ——两个出口各写各的否定条件时，新增一类账户不会有任何一处编译失败，失效方式是静默
  * 双执行或静默不执行。
  *
  * 一次决策可以包含跨交易所、跨标的的多张订单，所以 key 只到账户，不含标的。
  * 策略不订阅本 topic，故信号不会回流给任何策略。
  */
object OrderIntent extends Topic[AccountId, AccountOutcome]("orderIntent"):
  def keyOf(payload: AccountOutcome): AccountId = payload.account

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
  */
trait Strategy:
  /** 订阅与处理 —— 一处声明 */
  def handlers: StrategyHandlers

  /** 订单超时时间 (毫秒): Created 状态超过该时长未获交易所确认则视为丢失，自动清理 */
  def orderTimeoutMs: Long
