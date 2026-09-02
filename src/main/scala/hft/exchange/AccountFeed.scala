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
  * ## 记账只认订单回报，成交只是明细
  *
  * 仓位由 [[OrderStatusChanged.filledQuantity]] (交易所报的**累计成交量**) 驱动，
  * [[Executed]] 不参与记账。
  *
  * 这不是分工偏好，是**一致性要求**：一条订单回报里，"这单成交到 0.5 了"与"这单终态了"
  * 是同一个事实的两面，天然同步。柜台据它记账，就能在同一条报告里先发仓位、再发订单状态
  * —— 策略永远看不到"挂单已消失、仓位还没更新"那个中间状态。而那个状态是危险侧的：
  * 策略会认为自己既没有单也没有仓位，于是**再下一单**。
  *
  * 让成交频道也参与记账则会引出一串补丁：只给单笔量的交易所要本地累加、累加要按成交 id
  * 去重、累加会漂出浮点尾巴、两个来源还会互相不一致。源头单一，这些全都不存在。
  */
enum AccountReport:
  /** 一笔成交的**明细** —— 交易所报的这一笔是什么价、多少量。
    *
    * **不参与记账**（仓位由 [[OrderStatusChanged]] 的累计量驱动）。它只变成总线上的
    * [[hft.event.Topics.Fill]]，服务于绩效统计、成交记录、滑点分析这些要看成交本身的人。
    */
  case Executed(
      symbol: Symbol,
      side: Side,
      price: Price,
      qty: Coin,
      timestamp: Timestamp,
  )

  /** 订单状态变化。**这是记账的唯一依据** —— `filledQuantity` 是交易所报的累计成交量。
    *
    * **两个价格不能混**：`price` 是**委托价** (市价单没有，多家给空串)，只用于回报给策略看；
    * `avgFillPrice` 是**累计成交均价**，柜台记账时用的是它。
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
      /** 是否只减仓 —— 三家的挂单查询与订单推送都返回它。策略拿它给 resting 单分槽,
        * 本地伪造一个 false 会让重启后真正的止盈单被归错槽、于是再挂一张。 */
      reduceOnly: Boolean,
      timestamp: Timestamp,
  )

  /** 某币种钱包余额的**当前值** (不是变化量)，一次只说一个币种。
    *
    * 三家的私有钱包推送都是这个形态：**值是绝对余额，但推送只覆盖发生变动的币种**
    * (Binance `ACCOUNT_UPDATE.B` 的 `wb`、OKX `account` 的 `event_update`、Bybit `wallet` 的 `coin[]`)。
    * 因此**没有任何 WS 通道能给出"这就是整份钱包"这个更强的事实** —— 那份全量只能来自
    * 启动对齐时的 REST 钱包 (见 [[hft.domain.Wallet]] 与 `TradingGateway.currentWallet`)。
    *
    * 曾经这里还有一个 `WalletSnapshot`，由 OKX/Bybit 的 WS 推送产出。依据是错的：
    *   - OKX 文档写明只有 initial/regular snapshot 是全量，`event_update` 只带变化币种，
    *     且快照本身可能分页 (`curPage`/`lastPage`)；
    *   - Bybit 文档写明"订阅成功时不给 snapshot"，也从未声明 `coin[]` 是全量
    *     (官方示例里 `totalWalletBalance` 远大于唯一列出的那条 BTC 的 `usdValue`)。
    *   下游对每条推送做整表替换，于是**任何一次只有 USDT 变动的推送都会把 ETH 现货抹成 0**，
    *   而 delta 对冲正拿这个数当敞口 —— Bybit 侧还没有周期性全量推送, 这个错误不自愈。 */
  case BalanceChanged(currency: String, amount: Double, timestamp: Timestamp)

  /** 账户净值与总名义价值 */
  case EquityChanged(equity: Double, timestamp: Timestamp)

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
    * ## 实现方不得重放已投递过的报告
    *
    * 柜台的幂等靠"累计成交量单调递增"成立，而订单终态之后它只保留一分钟墓碑。
    * 过了那之后同一订单的报告若再次出现，会被当作全新的累计量重记一遍 —— 仓位近乎翻倍。
    * 目前这条自然成立 (本框架 fail-fast 不重连、交易所不主动重放)，写在这里是因为
    * 将来给某个实现加"重连 + 回放"时，那个 bug 会零症状地复活。
    *
    * @param sink  把解析出的每一条变动交给柜台。**在连接线程上调用**，柜台负责串行化
    * @param fork  在柜台插件的作用域内起一条常驻线程 (抛出的异常级联终止引擎)
    * @param sleepUnlessStopped
    *   协作式睡眠：返回 true 表示停机已请求，循环应立即退出。**心跳、轮询一类的常驻循环
    *   必须用它，不要用裸 `Thread.sleep`** —— 后者只能靠中断打断，于是每次停机都多一次
    *   "能不能按时退出"的不确定，超时就会让整个系统进入 Quarantined。
    *   与 [[hft.actor.ActorContext.sleepUnlessStopped]] 是同一个能力，只是经由柜台转交。
    */
  def connect(
      sink: AccountReport => Unit,
      fork: (=> Unit) => Unit,
      sleepUnlessStopped: Long => Boolean,
  ): Unit

/** 汇报面送进柜台邮箱的一跳 —— **不发到总线**，由 [[hft.actor.ActorContext.tell]] 直投。
  *
  * 包成事件只是为了搭邮箱那趟车 (邮箱里流的是事件)；没有任何人订阅这个 topic。
  */
private[exchange] final case class GatewayInbox(report: AccountReport)

private[exchange] object GatewayInboxes extends Topic[Unit, GatewayInbox]("gatewayInbox"):
  // 没有路由键: 它从不经总线, 直投邮箱的那一条不需要被谁挑出来。
  // 上一版留了个 target 字段, 那是绕总线时代的遗迹 —— 留着会让人以为存在按 key 分发。
  def keyOf(payload: GatewayInbox): Unit = ()
