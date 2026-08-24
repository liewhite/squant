package hft.strategy

import hft.domain.*
import hft.event.{AnyEvent, Event, Topic}
import hft.state.StateView

/** 策略在处理一条事件时能做的事。
  *
  * 账户是**构造能力而非可读数据**：策略能用它发单，却读不到它的值。这样同一份逻辑既能跑
  * 实盘也能跑影子盘，而策略里写不出任何依赖"我是哪个账户"的分支。
  *
  * 下单一律走 [[place]]：发出去之前还有三件必做的事 —— 生成 clientOrderId、把订单以币本位
  * 登记进 pending（超时检测与停机撤单靠它）、按交易所精度换算。这三件由
  * [[hft.engine.StrategyRunner]] 对**处理器返回的**下单意图统一施加，本类只负责构造。
  *
  * 构造与副作用分开是有原因的：若在构造时就登记 pending，策略把 `place(...)` 的结果丢弃
  * （条件分支没返回它）或中途抛异常，就会留下一条永远不会发出的幽灵挂单 —— 对
  * `orderTimeoutMs` 很大的 GTC 策略等于永久挂账。现在只有**真正返回**的意图才算数。
  *
  * @param now 本次事件的处理时刻（回测虚拟时间 / 实盘墙钟）。事件时间戳取自它而非墙钟，
  *            回测才能"同一输入必得同一结果"。
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
):
  /** 下单。`orders` 用**币本位**数量，框架负责换算成交易所格式 */
  def place(orders: Vector[Order], comment: String): AnyEvent =
    Event.stamped(OrderIntent, AccountOutcome(account, OutcomeEvent.PlaceOrders(orders, comment)), now, now)

  def place(order: Order, comment: String): AnyEvent = place(Vector(order), comment)

  /** 撤单。撤单终态以私有流推送为准，框架不合成确认事件 */
  def cancel(exchange: Exchange, symbol: Symbol, ref: OrderRef): AnyEvent =
    Event.stamped(OrderIntent, AccountOutcome(account, OutcomeEvent.CancelOrder(exchange, symbol, ref)), now, now)

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
