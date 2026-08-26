package hft.exchange

import hft.domain.*
import hft.event.{AnyEvent, Event, Interest, Topic, Topics}
import org.slf4j.LoggerFactory

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
    * 唯一写者是 actor 线程 (推送经 [[hft.actor.ActorContext.tell]] 串行化进来)。 */
  private var ledger: Ledger = Ledger.empty(account, cash = 0.0)

  /** 每张订单**已记进账本**的累计成交量 —— 增量记账的依据，也是去重的依据。
    *
    * 汇报面报的是累计量，柜台只记 `累计 − 已记账` 那部分。于是重复推送记不进第二次、
    * 跨频道乱序谁先到谁记账、丢一条推送也能被下一条的累计量补回来。
    *
    * **终态之后不能立刻删**：订单状态与成交常走两条频道，"已成交"先到、那笔成交的推送
    * 随后才到是常态。删了记账进度，晚到的那条就会被当成新成交记第二遍 —— 仓位凭空翻倍。
    * 所以终态只立墓碑，过了 [[RestTradingGateway.SettledRetentionMs]] 再清。
    */
  private val settled = scala.collection.mutable.Map.empty[OrderId, RestTradingGateway.Settlement]

  /** 已经对齐过的标的 —— 对账只在这些标的上做。
    *
    * 交易所推的是**整个账户**的仓位变动 (Binance 的 ACCOUNT_UPDATE、OKX 订阅即推全量 SWAP)，
    * 里面有手动持仓、别的机器人的持仓。拿它们跟一本只装了本策略标的的账本比，
    * 每一条推送都会报一次漂移 —— 告警一旦成了噪声就等于没有告警。
    */
  private var syncedSymbols: Set[Symbol] = Set.empty

  /** 最后一笔成交入账的时刻 —— 对账要等安静下来再做，见 [[reconcile]] */
  private var lastSettledAt: Timestamp = 0L

  override def exchange: Exchange = client.exchange

  /** 汇报面解析出的每一条都排进本柜台的邮箱 (不经总线)，由 actor 线程按序消费 ——
    * 于是"柜台是回报的唯一发布者"成立，账本也只有一个写者。 */
  override protected def connect(): Unit =
    feed.connect(report => tell(Event.local(GatewayInboxes, GatewayInbox(target, report))), body => fork(body))

  override protected def onOther(event: AnyEvent, now: Timestamp): Vector[AnyEvent] =
    event.as(GatewayInboxes).map(inbox => handle(inbox.report, now)).getOrElse(Vector.empty)

  /** 把一条账户变动翻译成总线上的回报。**顺序在这里构造**：账本一变就先发仓位快照。 */
  private def handle(report: AccountReport, now: Timestamp): Vector[AnyEvent] = report match
    case AccountReport.Executed(orderId, symbol, side, price, cumulativeQty, ts) =>
      settleUpTo(orderId, symbol, side, price, cumulativeQty, ts)

    case AccountReport.OrderStatusChanged(orderId, clientOrderId, symbol, side, status, price, quantity, filledQuantity, ts) =>
      // 有些交易所只在订单回报里给得出累计成交量 (Bybit 的 order 频道)。先按它补记账，
      // 增量为零就什么也不发生 —— 成交频道已经记过了。
      val settlement =
        if filledQuantity.isZero then Vector.empty
        else settleUpTo(orderId, symbol, side, price, filledQuantity, ts)
      if status.isTerminal then
        settled.updateWith(orderId)(_.map(_.copy(terminalAt = Some(ts))))
        evictSettled(ts)
      val update = OrderUpdate(account, orderId, clientOrderId, exchange, symbol, side, status, price, quantity, filledQuantity, ts)
      settlement :+ Event.stamped(Topics.OrderUpdate, update, ts, now)

    case AccountReport.BalanceChanged(currency, amount, ts) =>
      Vector(Event.stamped(Topics.Balance, Balance(account, exchange, currency, amount, ts), ts, now))

    case AccountReport.EquityChanged(equity, notional, ts) =>
      Vector(Event.stamped(Topics.AccountInfo, AccountInfo(account, exchange, equity, notional), ts, now))

    case AccountReport.GreeksChanged(ccy, delta, gamma, theta, vega, ts) =>
      Vector(Event.stamped(Topics.Greeks, Greeks(account, exchange, ccy, delta, gamma, theta, vega, ts), ts, now))

    case AccountReport.PositionReported(symbol, size, _) =>
      reconcile(symbol, size, now)
      Vector.empty // 总线上的仓位只有一个来源: 本账本

  /** 把某订单的账记到 `cumulativeQty` 为止，产出「仓位快照 → 成交」。
    *
    * 增量为零 (重复推送、乱序后到的那条) 时什么都不发 —— 幂等是这套记账的立身之本。
    */
  private def settleUpTo(
      orderId: OrderId,
      symbol: Symbol,
      side: Side,
      price: Price,
      cumulativeQty: Coin,
      ts: Timestamp,
  ): Vector[AnyEvent] =
    val already = settled.get(orderId).map(_.cumulative).getOrElse(Coin.Zero)
    val delta = cumulativeQty - already
    if !(delta > Coin.Zero) then Vector.empty
    else
      settled(orderId) = settled.get(orderId) match
        case Some(prev) => prev.copy(cumulative = cumulativeQty)
        case None       => RestTradingGateway.Settlement(cumulativeQty, terminalAt = None)
      lastSettledAt = ts
      ledger = ledger.applyFill(exchange, symbol, side, price, delta)
      val position = positionOf(symbol)
      logger.info(
        s"成交入账 $target $symbol $side ${delta.value} @ ${price.value} -> 仓位 ${position.size.value} (order=$orderId)"
      )
      Vector(
        TradingGateway.positionEvent(position, ts),
        Event.stamped(Topics.Fill, Fill(account, exchange, symbol, side, price, delta, ts), ts, ts),
      )

  /** 清掉早已终态、不会再有晚到成交的记账进度。只在订单进终态时扫一次，频率与终态同阶 */
  private def evictSettled(now: Timestamp): Unit =
    settled.filterInPlace((_, s) => !s.terminalAt.exists(now - _ > RestTradingGateway.SettledRetentionMs))

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
  private def reconcile(symbol: Symbol, reported: Coin, now: Timestamp): Unit =
    // 三道闸, 每一道挡的都是一种会把真漂移淹掉的噪声:
    //   1. 没对齐过的标的 —— 账本里压根没有它, 那是别人的仓位
    //   2. 刚成交完 —— 成交与仓位走不同频道, 交易所先推仓位时账本还没记上那一笔
    if !syncedSymbols.contains(symbol) then return
    if now - lastSettledAt < RestTradingGateway.ReconcileQuietMs then return
    val mine = positionOf(symbol).size
    // 3. 容差取一个最小变动单位: 更小的差异是浮点噪声, 不是漂移
    val tolerance = metas.get(symbol).map(_.sizeStep).getOrElse(0.0)
    if (mine - reported).abs.value > tolerance then
      logger.error(
        s"!!! 仓位漂移 $target $symbol: 本地账本=${mine.value} 交易所=${reported.value} " +
          "(强平/手动干预/资金费/漏收成交都会造成)。策略正按本地账本决策, 需人工确认; " +
          "确认后**先撤掉在途单**再发账户对齐指令让柜台按 REST 快照重置 —— " +
          "有单在途时重置会把一笔既在快照里、推送又还在路上的成交记两遍"
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
        settled.clear() // 账本重置, 记账进度跟着归零
        syncedSymbols ++= symbols
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
  /** 一张订单的记账进度。`terminalAt` 有值表示已终态，只等过保留期被清掉 */
  private final case class Settlement(cumulative: Coin, terminalAt: Option[Timestamp])

  /** 订单终态后，记账进度还要留多久。
    *
    * 留着是为了让晚到的成交推送认得出"这笔已经记过了"。跨频道的迟到在秒级，
    * 取一分钟留足余量 —— 代价只是多占一会儿内存，而清早了就是仓位翻倍。
    */
  val SettledRetentionMs: Long = 60_000

  /** 对账前要安静多久。
    *
    * 成交与仓位在多数交易所走的是两条频道，交易所先推仓位、后推那笔成交是常态。
    * 那个窗口里账本必然落后一笔，此刻对账报出来的全是假漂移。跨频道的时间差在秒级以内，
    * 取 5 秒留足余量 —— 对账本来就不是实时的事，它要抓的是持续存在的偏差。
    */
  val ReconcileQuietMs: Long = 5_000

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
