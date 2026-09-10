package hft.strategy

import hft.domain.*
import hft.event.Commands.{AccountOutcome, OrderIntent, OutcomeEvent}
import hft.event.{AnyEvent, Event, Topic}
import hft.state.StateView

/** 策略在处理一条事件时能做的事。
  *
  * 账户是**构造能力而非可读数据**：策略能用它发单，却读不到它的值。这样同一份逻辑既能跑
  * 实盘也能跑影子盘，而策略里写不出任何依赖"我是哪个账户"的分支。
  *
  * 下单一律走 [[place]]：发出去之前还有两件必做的事 —— 生成 clientOrderId、把订单以币本位
  * 登记进 pending（超时检测与停机撤单靠它）。这两件由 [[hft.engine.StrategyRunner]] 对
  * **处理器返回的**下单意图统一施加，本类只负责构造。
  *
  * 交易所精度对齐**不在这里，也不在策略侧** —— 那是柜台的事 (见
  * [[hft.exchange.TradingGateway]])：tick、最小下单量、张数换算都是交易所的事实，
  * 策略发的是币本位的意图，柜台负责把它变成交易所收得下的样子，收不下就以拒单回流。
  *
  * 构造与副作用分开是有原因的：若在构造时就登记 pending，策略把 `place(...)` 的结果丢弃
  * （条件分支没返回它）或中途抛异常，就会留下一条永远不会发出的幽灵挂单。
  * 现在只有**真正返回**的意图才算数。
  *
  * @param now 本次事件的处理时刻（回测虚拟时间 / 实盘墙钟）。事件时间戳取自它而非墙钟，
  *            回测才能"同一输入必得同一结果"。
  * @param orderTag 本次事件若是一条订单回报，这里是**当初下这张单时给的标注**（见 [[orderTag]]）
  */
final class StrategyContext private[hft] (
    /** 本策略订阅范围内的聚合状态 —— **只读视图**。
      *
      * 给的是 [[StateView]] 而不是实现: 事件应用与挂单登记由框架在固定位置做，
      * 策略插一脚的后果分别是"同一条事件重复计入仓位"与"登记一条无主挂单"，
      * 两者都没有外在症状。够不着，就不必靠记性。 */
    val state: StateView,
    private val account: AccountId,
    private val now: Timestamp,
    /** 本次事件若是一条 [[hft.event.Topics.OrderUpdate]]，这里是当初下这张单时给的
      * [[hft.domain.Order.tag]] —— **"这条回报是我哪一张单的"**。
      *
      * 没有它的话策略认不出自己的回报：clientOrderId 由框架在处理器返回之后才生成，策略手里
      * 那个是空串。三个业务策略此前各自发明了一套替代办法 (按 `reduceOnly` 分槽、按
      * (交易所, 标的) 认领、立"同时刻只有一张在途单"的不变量)，代价是同一标的上挂多张
      * 可区分的单这件事写不出来。
      *
      * `None` 的含义是**这条回报没有本策略的标注**，四种情形：非回报事件；下单时没标注；
      * 重启后从交易所接管的既有挂单 (标注是上一个进程内存里的事实，见 [[hft.domain.Order.tag]])；
      * 以及回报本身没带 clientOrderId (那压根不是本策略发的单，见 [[hft.state.InstrumentState]])。
      * 它们在决策上是同一件事 ——"按标注分派"这一支走不了 —— 所以不细分。
      *
      * **不要指望用 `state` 去细分它们**：处理器看到的状态已经应用过本次事件，而订单进终态时
      * 挂单登记就被移除了 —— 恰恰在最需要区分的那一刻，`pendingOrders` 里已经没有这张单。
      * 手上只有 `update.clientOrderId`：有值即"本账户发出的单"(手工单与交易所自建的 TP/SL
      * 都不带它)，但它区分不了"本进程未标注"与"上个进程留下的"。真要区分后者，得有别的机制，
      * 现有查询给不出。
      *
      * **[[hft.event.Topics.Fill]] 上没有** —— `Fill` 载荷不带订单身份 (见 [[hft.domain.Fill]])，
      * 框架无从知道它属于哪一张单。要按标注认成交，订 `own(Topics.OrderUpdate)` 看累计成交量，
      * 那本来就是记账的唯一依据 (见 [[hft.exchange.TradingGateway]])。
      *
      * **无默认值**：构造点只有三个 (事件分派、停机撤单、测试)，显式写 `None` 的代价近乎为零，
      * 而给一个默认值恰好制造本机制要消灭的那种静默 —— 将来新增一处分派忘了传标注，
      * 编译照过，症状是"标注恒为 None"，策略从此认不出自己的单。
      */
    val orderTag: Option[String],
):
  /** 下单。`orders` 用**币本位**数量，柜台负责换算成交易所格式。
    *
    * 返回**每个交易所一条**下单意图：下单指令按 (账户, 交易所) 路由，一条意图只能有一个
    * 交易所。跨所的一次决策因此在这里按交易所分组 —— 由框架拆，而不是要求策略记得
    * 分开调用：忘了拆的后果是路由键取了第一张单的交易所，另一个所的订单被发去错误的柜台。
    *
    * 空 `orders` 得到空结果 —— 没有订单的下单指令没有交易所可路由，也没有任何意义。
    *
    * `comment` 与 [[hft.domain.Order.tag]] 是两件事，别混用：前者描述**这一次决策**
    * （一批单共一条，只进日志，如 `"cross_arb_open | edge=3.2bp"`），后者标注**这一张单**
    * （进挂单登记，回报到达时经 [[orderTag]] 交还）。要按它认单就写 tag，写 comment 认不回来。
    */
  def place(orders: Vector[Order], comment: String): Vector[AnyEvent] =
    orders
      .groupBy(_.exchange)
      .toVector
      .sortBy(_._1.toString) // 分组顺序确定 —— 回测要"同一输入必得同一结果"
      .map { (_, sameExchange) =>
        Event.stamped(OrderIntent, AccountOutcome(account, OutcomeEvent.PlaceOrders(sameExchange, comment)), now, now)
      }

  def place(order: Order, comment: String): Vector[AnyEvent] = place(Vector(order), comment)

  /** 撤销本策略已登记的挂单。撤单终态以私有流推送为准，框架不合成确认事件。
    *
    * 收 [[Instrument]] 而不是拆开的 `(exchange, symbol)`：策略手里本来就是标的
    * （行情声明用的同一个值），拆开传等于让调用方与本方法各拼一次同一个键。 */
  def cancel(instrument: Instrument, ref: OrderRef): AnyEvent =
    val owned = state.instrumentState(instrument).exists(_.pendingOrders.exists { pending =>
      val order = pending.order
      (ref match
        case OrderRef.ByExchangeId(id) => id.nonEmpty && order.id == id
        case OrderRef.ByClientId(id)   => id.nonEmpty && order.clientOrderId == id
      )
    })
    require(
      owned,
      s"只能撤销本策略已登记的挂单: instrument=$instrument ref=$ref",
    )
    Event.stamped(
      OrderIntent,
      AccountOutcome(account, OutcomeEvent.CancelOrder(instrument.exchange, instrument.symbol, ref)),
      now,
      now,
    )

  /** 发一条自定义事件 —— 策略自己的指标、信号、给别的组件的提示。
    *
    * 这是"策略对外输出"的通道：外部只要订阅那个 [[Topic]] 就能收到，框架不需要知情。
    *
    * 时间戳取自本次事件的处理时刻，不读墙钟 —— 否则回测里观察者看到的时间戳跨运行不可复现。
    *
    * **不要订阅自己 emit 的 topic 并在该处理器里再 emit**：那会自激励（回测在同一虚拟时刻
    * 无限入队、实盘经总线循环），失效形态是挂死而不是报错。
    */
  def emit[K, P](topic: Topic[K, P], payload: P): AnyEvent = Event.stamped(topic, payload, now, now)
