package hft.exchange

import hft.domain.*
import hft.event.{AnyEvent, Event, Interest, Topic, Topics}
import org.slf4j.LoggerFactory

/** 私有推送到达柜台 —— 从汇报面的连接线程串行化回 actor 线程的那一跳。
  *
  * 走总线而不是直接调用：账本的写者必须只有 actor 线程一个，而 [[AccountFeed]] 在
  * 自己的连接线程上解析推送。key 是 (账户, 交易所)，因此只有本柜台会收到自己的回报。
  */
private[exchange] final case class FeedReport(target: AccountExchange, event: AnyEvent)

private[exchange] object FeedReports extends Topic[AccountExchange, FeedReport]("gatewayFeedReport"):
  def keyOf(payload: FeedReport): AccountExchange = payload.target

/** 真实交易所的柜台：REST 执行 + REST 对齐 + 私有推送汇报。
  *
  * 三家交易所在这一层几乎一样 —— 差异全在私有推送的解析上，那部分由注入的
  * [[AccountFeed]] 承担。所以这是一个**具体类**而不是又一层抽象：各交易所只提供
  * "客户端 + 私有流"这一对零件，不再各写一遍下单错误分类与对齐编排。
  *
  * ## 错误分类是这一层的核心判断
  *
  * 见各分支注释。要点：**"订单是否已到达交易所"不确定时一律终止进程**，
  * 因为本地状态无法保证正确，而重启后的启动对齐有答案。
  *
  * @param client       私有 REST。**拿得到这个类型本身就意味着凭证已具备**
  * @param feed         私有推送流
  * @param account      本柜台服务的账户
  * @param metas        本所合约规格。装配期一次性加载 —— 缺一个标的的规格就发不出它的单，
  *                     与其在首笔下单时炸，不如启动时就失败
  */
final class RestTradingGateway(
    client: TradingClient,
    feed: AccountFeed,
    override val account: AccountId,
    metas: Map[Symbol, SymbolMeta],
    override protected val accountRefreshMs: Long = 10_000,
) extends TradingGateway:
  require(
    client.exchange == feed.exchange,
    s"柜台的 REST 客户端 (${client.exchange}) 与私有流 (${feed.exchange}) 不是同一个交易所",
  )
  private val logger = LoggerFactory.getLogger(classOf[RestTradingGateway])

  /** 本柜台的账本 —— **仓位的唯一算处**。初值来自对齐，之后每笔成交进账。
    * 唯一写者是 actor 线程 (推送经 [[FeedReports]] 串行化进来)。 */
  private var ledger: Ledger = Ledger.empty(account, cash = 0.0)

  override def exchange: Exchange = client.exchange

  /** 汇报面推来的一切先进本柜台的邮箱，由 actor 线程处理后再出总线 ——
    * 于是"柜台是回报的唯一发布者"成立，账本也只有一个写者。 */
  override protected def extraInterests: Set[Interest] = Set(Interest.Keyed(FeedReports, Set(target)))

  override protected def connect(): Unit =
    feed.connect(account, event => publish(Event.local(FeedReports, FeedReport(target, event))), body => fork(body))

  override protected def onOther(event: AnyEvent, now: Timestamp): Vector[AnyEvent] =
    event.as(FeedReports).map(report => handleReport(report.event, now)).getOrElse(Vector.empty)

  /** 处理一条私有推送。三条通道：
    *   - **成交**：进账本，并在它**前面**发一份仓位快照 (顺序见 [[TradingGateway]])
    *   - **交易所报的仓位**：只用来**对账**，不进总线 —— 总线上的仓位只有一个来源，就是本账本
    *   - **其余** (订单回报 / 余额 / 净值 / 希腊值)：原样转发
    */
  private def handleReport(report: AnyEvent, now: Timestamp): Vector[AnyEvent] =
    report.as(Topics.Fill) match
      case Some(fill) =>
        ledger = ledger.applyFill(exchange, fill.symbol, fill.side, fill.price, fill.size)
        Vector(TradingGateway.positionEvent(positionOf(fill.symbol), now), report)
      case None =>
        report.as(Topics.Position) match
          case Some(reported) => reconcile(reported); Vector.empty
          case None           => Vector(report)

  private def positionOf(symbol: Symbol): Position =
    ledger.positions.getOrElse(symbol, Position.empty(account, exchange, symbol))

  /** 拿交易所报的仓位与本账本比对。**只告警，不覆盖。**
    *
    * 不覆盖是因为跨频道没有顺序保证：成交与仓位走的是两条频道，交易所先推仓位、后推那笔
    * 成交是可能的 —— 覆盖之后再把那笔成交记进账本，就双重计数了。这正是"不做快照覆盖式
    * 对账"这条老结论的由来。
    *
    * 那漂移怎么修？**发一条账户对齐指令**：REST 查的是快照，不参与推送的流竞争，
    * 柜台会据此重置账本并广播新仓位 (见 [[syncPositions]])。检测用推送 (便宜、及时)、
    * 修复用 REST (权威、无竞态)，两件事分开。
    *
    * 触发漂移的是账本看不见的东西：强平、手动干预、资金费结算、以及任何一笔漏收的成交。
    * 不报出来的话，策略会一直按一个错的敞口对冲，而这没有任何外在症状。
    */
  private def reconcile(reported: Position): Unit =
    val mine = positionOf(reported.symbol).size
    // 容差取一个最小变动单位: 更小的差异是浮点噪声, 不是漂移
    val tolerance = metas.get(reported.symbol).map(_.sizeStep).getOrElse(0.0)
    if (mine - reported.size).abs.value > tolerance then
      logger.error(
        s"!!! 仓位漂移 $target ${reported.symbol}: 本地账本=${mine.value} 交易所=${reported.size.value} " +
          "(强平/手动干预/资金费/漏收成交都会造成)。策略正按本地账本决策, 需人工确认; " +
          "确认后可发一条账户对齐指令让柜台按 REST 快照重置"
      )

  override protected def metaOf(symbol: Symbol): SymbolMeta =
    metas.getOrElse(symbol, sys.error(s"$exchange 没有 $symbol 的合约规格, 无法发单 (装配时未加载?)"))

  /** 每个 REST 调用 fork 独立虚拟线程，互不阻塞，也不阻塞柜台的事件循环。
    *
    * "不真下单"不是这里的开关，而是换一个客户端实现 ([[DryRunClient]]) ——
    * 它以 4xx 拒单形态返回，正好落在下面第一条通道上，本类因此一个分支都不需要。
    */
  override protected def placeAligned(order: Order, now: Timestamp): Unit =
    fork {
      client.placeOrder(OrderConversion.toExchangeOrder(order, metaOf(order.symbol))) match
        case Right(orderId) =>
          // 订单确认 (Pending/Filled) 以私有流推送为准，这里只记录
          logger.info(s"下单已受理: $exchange ${order.symbol} orderId=$orderId clientOrderId=${order.clientOrderId}")
        case Left(e @ ExchangeError.Http(status, _)) if status == 429 || status == 418 =>
          // 限频/封禁: 说明"订单生命周期自然限速"的假设已被打破，
          // 按拒单回流会形成"拒单->重挂->更多请求"的重试风暴，必须终止
          throw IllegalStateException(s"被交易所限频, 终止: $exchange ${order.symbol} ${e.message}")
        case Left(e @ ExchangeError.Http(status, _)) if status >= 400 && status < 500 =>
          // 交易所明确拒绝，订单确定未成立 -> 回流策略 (与精度拒绝同一条路径)
          logger.warn(s"下单被拒: $exchange ${order.symbol} ${e.message}")
          reject(order, e.message, now)
        case Left(e) =>
          // 网络/超时/5xx: 订单是否成立不确定，本地状态无法保证正确
          throw IllegalStateException(s"下单结果不确定, 终止: $exchange ${order.symbol} ${e.message}")
    }

  override protected def cancelOrder(symbol: Symbol, ref: OrderRef, now: Timestamp): Unit =
    fork {
      client.cancelOrder(symbol, ref) match
        case Right(())                                => logger.info(s"撤单已受理: $exchange $symbol ${ref.raw}")
        case Left(ExchangeError.OrderNotFound(reason)) =>
          // 订单已成交/已撤销，终态同样由私有流推送，撤单失败非致命
          logger.info(s"订单已不在: $exchange $symbol ${ref.raw} ($reason)")
        case Left(e) =>
          throw IllegalStateException(s"撤单结果不确定, 终止: $exchange $symbol ${ref.raw} ${e.message}")
    }

  /** 账户归属由柜台盖章，不信客户端填的那个 —— 各家适配层历史上都硬编码成实盘，
    * 而"这份回报属于哪个账户"是装配期的事实，只有柜台知道。 */
  /** 拉真实持仓并**据此重置账本** —— 对齐是账本唯一的权威初值来源。
    *
    * 这也是漂移之后的修复入口：REST 是快照，不参与推送的流竞争。
    */
  override protected def syncPositions(symbols: Set[Symbol]): Vector[Position] =
    client.fetchPositions() match
      case Right(positions) =>
        val mine = positions.filter(p => symbols.contains(p.symbol)).map(_.copy(account = account))
        ledger = Ledger(account, mine.map(p => p.symbol -> p).toMap, cash = 0.0)
        mine
      case Left(e) => throw IllegalStateException(s"$exchange 拉取初始持仓失败: ${e.message}")

  override protected def syncPendingOrders(symbols: Set[Symbol]): Vector[OrderUpdate] =
    symbols.toVector.sortBy(_.toString).flatMap { symbol =>
      client.fetchPendingOrders(symbol) match
        case Right(updates) => updates.map(_.copy(account = account))
        case Left(e)        => throw IllegalStateException(s"$exchange $symbol 拉取既有挂单失败: ${e.message}")
    }

  override protected def currentAccountInfo(): AccountInfo =
    client.fetchAccountInfo() match
      case Right(info) => info.copy(account = account)
      case Left(e)     => throw IllegalStateException(s"$exchange 拉取账户信息失败: ${e.message}")

object RestTradingGateway:
  /** 装好柜台，合约规格取自客户端 (见 [[ExchangeClient.symbolMetas]]，进程内只拉一次)。
    *
    * 规格在**装配期**加载并且失败即终止：缺一个标的的规格就发不出它的单，
    * 与其在首笔下单时才炸，不如启动时就说清楚。
    */
  def load(
      client: TradingClient,
      feed: AccountFeed,
      account: AccountId,
      accountRefreshMs: Long = 10_000,
  ): RestTradingGateway =
    RestTradingGateway(client, feed, account, client.symbolMetas, accountRefreshMs)
