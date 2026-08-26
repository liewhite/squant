package hft.exchange

import hft.domain.*
import hft.event.Topic

/** 汇报面解析出来的一条账户变动 —— **柜台的内部语言，不上总线**。
  *
  * ## 为什么不让汇报面直接发事件
  *
  * 从前它拿着一个 `publish` 回调，解析完直接发到总线。那让三件本该由柜台负责的事漏了出去：
  *
  *   1. **顺序**：一笔成交要产出「仓位 → 成交 → 订单状态」，而汇报面各家的推送次序不同
  *      (Binance 先状态后成交、OKX 反过来、Bybit 干脆是两条频道)。让它们各自发，
  *      顺序就成了交易所的实现细节。
  *   2. **记账**：仓位是柜台的账本，得先记账才谈得上发仓位事件。
  *   3. **账户归属**：谁的回报是装配期的事实，只有柜台知道 —— 从前每家适配层都得接一个
  *      account 参数，那是同一个事实的三份副本。
  *
  * 改成"只解析、不发布"之后，这三件事都收在柜台一处，各家适配层只剩它真正独有的部分：
  * 怎么把交易所的 JSON 读成这些形态。
  *
  * ## 成交报的是累计量，不是本次增量
  *
  * [[Executed.cumulativeQty]] 是**该订单到此刻为止的累计成交量**。柜台据它算增量
  * (`累计 − 已记账`)，于是三种麻烦一起消失：重复推送 (增量为零)、跨频道乱序 (谁先到谁记账，
  * 后到的增量为零)、丢一条推送 (下一条的累计量会把账补上)。
  *
  * 交易所只给单笔量的 (Bybit 的 execution 频道)，由该适配层自己累加 —— 它有 orderId，
  * 累加是本地的。进程不重连、重启走启动对齐，所以这份累计不会跨越断线残留。
  */
enum AccountReport:
  /** 一笔成交。`cumulativeQty` 是**该订单的累计成交量**，不是本次增量 */
  case Executed(
      orderId: OrderId,
      symbol: Symbol,
      side: Side,
      price: Price,
      cumulativeQty: Coin,
      timestamp: Timestamp,
  )

  /** 订单状态变化。带累计成交量 —— 有些交易所 (Bybit 的 order 频道) 只在这里给得出它。
    *
    * **两个价格不能混**：`price` 是**委托价** (市价单没有，多家给空串)，只用于回报给策略看；
    * `avgFillPrice` 是**累计成交均价**，柜台从这条补记账时用的是它。
    * 拿委托价去记账，市价单会把持仓均价记成 0，平仓时算出一笔巨额假亏损 —— 而净值从此失真，
    * 没有任何报错。
    */
  case OrderStatusChanged(
      orderId: OrderId,
      clientOrderId: Option[String],
      symbol: Symbol,
      side: Side,
      status: OrderStatus,
      price: Price,
      avgFillPrice: Price,
      quantity: Coin,
      filledQuantity: Coin,
      timestamp: Timestamp,
  )

  /** 某币种的钱包余额 */
  case BalanceChanged(currency: String, amount: Double, timestamp: Timestamp)

  /** 账户净值与总名义价值 */
  case EquityChanged(equity: Double, notional: Double, timestamp: Timestamp)

  /** 账户级期权希腊值 (按币种聚合) */
  case GreeksChanged(ccy: String, delta: Double, gamma: Double, theta: Double, vega: Double, timestamp: Timestamp)

  /** **交易所报告的**持仓 —— 只用于对账，不进总线。
    *
    * 总线上的仓位只有一个来源：柜台的账本。这一条是拿来跟账本比对的另一份读数
    * (见 [[TradingGateway.reconcile]])。
    */
  case PositionReported(symbol: Symbol, size: Coin, timestamp: Timestamp)

/** 账户私有推送流 —— 柜台的"汇报"面，**只解析，不发布**。
  *
  * 与柜台拆开是因为它们的**变化原因不同**：执行与对齐是 REST 的形状 (请求-响应)，
  * 汇报是长连接的形状 (解析推送)。三家交易所的前者几乎一样、后者各不相同。
  *
  * 本 trait 不知道总线、不知道账户、不知道回报该按什么顺序出去 —— 那些都是柜台的事。
  * 测试里给它一个收集器即可。
  */
trait AccountFeed:
  def exchange: Exchange

  /** 建立连接并开始解析。
    *
    * @param sink  把解析出的每一条变动交给柜台。**在连接线程上调用**，柜台负责串行化
    * @param fork  在柜台插件的作用域内起一条常驻线程 (抛出的异常级联终止引擎)
    */
  def connect(sink: AccountReport => Unit, fork: (=> Unit) => Unit): Unit

/** 汇报面送进柜台邮箱的一跳 —— **不发到总线**，由 [[hft.actor.ActorContext.tell]] 直投。
  *
  * 包成事件只是为了搭邮箱那趟车 (邮箱里流的是事件)；没有任何人订阅这个 topic。
  */
private[exchange] final case class GatewayInbox(report: AccountReport)

private[exchange] object GatewayInboxes extends Topic[Unit, GatewayInbox]("gatewayInbox"):
  // 没有路由键: 它从不经总线, 直投邮箱的那一条不需要被谁挑出来。
  // 上一版留了个 target 字段, 那是绕总线时代的遗迹 —— 留着会让人以为存在按 key 分发。
  def keyOf(payload: GatewayInbox): Unit = ()
