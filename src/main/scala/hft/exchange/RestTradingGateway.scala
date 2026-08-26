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

  /** 成交明细独立累出来的账本 —— **第二意见**。
    *
    * 它与 [[ledger]] 走的是两条完全不同的渠道：一个来自订单回报的累计成交量、一个来自
    * 成交推送的逐笔明细。两者对不上就说明**我们这边**有问题 (少解析了一条推送、
    * 字段读错了、去重去多了)，而不是账户被外部改了 —— 后者要看 [[reportedPositions]]。
    * 三方比对的价值全在这个区分上: 内部 bug 与外部事实要用不同的方式处理。
    */
  private var fillLedger: Ledger = Ledger.empty(account, cash = 0.0)

  /** 交易所报的最新仓位 —— **第三方读数**，不是账本 */
  private val reportedPositions = scala.collection.mutable.Map.empty[Symbol, Coin]

  /** 连续对不上的次数：(标的, 比对名) -> 次数。偶尔一次是时序窗口, 连续多次才是真问题 */
  private val disagreements = scala.collection.mutable.Map.empty[(Symbol, String), Int]

  /** 上次对账时刻 —— 时钟一秒一拍, 对账不必那么勤 */
  private var lastReconciledAt: Timestamp = 0L

  override def exchange: Exchange = client.exchange

  /** 汇报面解析出的每一条都排进本柜台的邮箱 (不经总线)，由 actor 线程按序消费 ——
    * 于是"柜台是回报的唯一发布者"成立，账本也只有一个写者。 */
  /** 除下单与对齐之外还收时钟 —— 三方对账按节拍做 (见 [[reconcile]]) */
  override protected def extraInterests: Set[Interest] = Set(Interest.All(Topics.Clock))

  override protected def connect(): Unit =
    feed.connect(report => tell(Event.local(GatewayInboxes, GatewayInbox(report))), body => fork(body))

  override protected def onOther(event: AnyEvent, now: Timestamp): Vector[AnyEvent] =
    event.as(Topics.Clock).foreach { _ =>
      if now - lastReconciledAt >= RestTradingGateway.ReconcileIntervalMs then
        lastReconciledAt = now
        reconcile()
    }
    event.as(GatewayInboxes).map(inbox => handle(inbox.report, now)).getOrElse(Vector.empty)

  /** 把一条账户变动翻译成总线上的回报。**顺序在这里构造**：账本一变就先发仓位快照。
    *
    * 先按标的分流：交易所推的是**整个账户**的动静，而本柜台管的是引擎交给它对齐的那些标的
    * (见 [[syncedSymbols]])。别人的标的不归它记账，也不归它对账。
    */
  private def handle(report: AccountReport, now: Timestamp): Vector[AnyEvent] =
    symbolOf(report) match
      case Some(symbol) if !syncedSymbols.contains(symbol) =>
        logger.debug(s"$target 忽略不归本柜台管的标的: $symbol")
        Vector.empty
      case _ => translate(report, now)

  /** 这条报告说的是哪个标的。账户级读数 (余额/净值/希腊值) 没有标的，不参与分流 */
  private def symbolOf(report: AccountReport): Option[Symbol] = report match
    case r: AccountReport.Executed           => Some(r.symbol)
    case r: AccountReport.OrderStatusChanged => Some(r.symbol)
    case r: AccountReport.PositionReported   => Some(r.symbol)
    case _                                   => None

  private def translate(report: AccountReport, now: Timestamp): Vector[AnyEvent] = report match
    case AccountReport.Executed(symbol, side, price, qty, ts) =>
      // 成交明细不进主账本 (仓位由订单回报的累计量驱动)，但要进第二本账 —— 它是对账的
      // 另一条独立渠道，两本对不上就说明我们这边漏了什么。
      fillLedger = fillLedger.applyFill(exchange, symbol, side, price, qty)
      Vector(Event.stamped(Topics.Fill, Fill(account, exchange, symbol, side, price, qty, ts), ts, now))

    case AccountReport.OrderStatusChanged(orderId, clientOrderId, symbol, side, status, price, avgFillPrice, quantity, filledQuantity, ts) =>
      // 记账用**成交均价**而不是委托价: 市价单的委托价是空的, 拿它记账会把持仓均价记成 0,
      // 平仓时算出一笔巨额假亏损, 而净值从此失真、没有任何报错。
      val settlement =
        if filledQuantity.isZero then Vector.empty
        else settleUpTo(orderId, symbol, side, avgFillPrice, filledQuantity, ts, now)
      if status.isTerminal then
        settled.updateWith(orderId)(_.map(_.copy(terminalAt = Some(now))))
        evictSettled(now)
      val update = OrderUpdate(account, orderId, clientOrderId, exchange, symbol, side, status, price, quantity, filledQuantity, ts)
      // 顺序在这里构造: 仓位先于订单状态。反过来的话, 策略会看到"挂单已消失、仓位还没更新"
      // —— 它据此认为自己既没单也没仓位, 于是再下一单。那是危险侧的中间状态。
      settlement :+ Event.stamped(Topics.OrderUpdate, update, ts, now)

    case AccountReport.BalanceChanged(currency, amount, ts) =>
      Vector(Event.stamped(Topics.Balance, Balance(account, exchange, currency, amount, ts), ts, now))

    case AccountReport.EquityChanged(equity, notional, ts) =>
      Vector(Event.stamped(Topics.AccountInfo, AccountInfo(account, exchange, equity, notional), ts, now))

    case AccountReport.GreeksChanged(ccy, delta, gamma, theta, vega, ts) =>
      Vector(Event.stamped(Topics.Greeks, Greeks(account, exchange, ccy, delta, gamma, theta, vega, ts), ts, now))

    case AccountReport.PositionReported(symbol, size, _) =>
      reportedPositions(symbol) = size // 只记下来, 按节拍统一对账
      Vector.empty // 总线上的仓位只有一个来源: 本账本

  /** 把某订单的账记到 `cumulativeQty` 为止，产出「仓位快照 → 成交」。
    *
    * 增量为零 (重复推送、乱序后到的那条) 时什么都不发 —— 幂等是这套记账的立身之本。
    */
  /** 把某订单的账记到 `cumulativeQty` 为止，产出仓位快照。
    *
    * 增量为零 (重复推送、或本条回报没带来新成交) 时什么都不发 —— 幂等是这套记账的立身之本。
    * **不产出成交明细**：那是 [[AccountReport.Executed]] 的事，两者的来源与语义都不同
    * (一个是交易所报的这一笔、一个是累计量的差)。
    */
  private def settleUpTo(
      orderId: OrderId,
      symbol: Symbol,
      side: Side,
      price: Price,
      cumulativeQty: Coin,
      ts: Timestamp,
      now: Timestamp,
  ): Vector[AnyEvent] =
    val already = settled.get(orderId).map(_.cumulative).getOrElse(Coin.Zero)
    val delta = cumulativeQty - already
    // 半个最小变动单位以下视作零 —— 两个精确值相减仍会留下浮点尾巴
    // (0.8 - 0.3 = 0.5000000000000001)，严格比较会让它产出一笔量级 1e-16 的幻影成交。
    if delta.value <= dustOf(symbol) then Vector.empty
    else if price.value <= 0.0 then
      // 修过一次的 bug 值得一道守卫: 拿委托价 (市价单为空) 记账会把持仓均价记成 0,
      // 平仓时算出巨额假亏损而毫无报错。以后任何适配层填错价格字段, 这里立刻可见。
      logger.error(
        s"!!! $target $symbol order=$orderId 成交均价为 ${price.value}, 拒绝入账 —— " +
          "记零价会污染持仓均价与已实现盈亏; 这多半是适配层填了委托价而非成交均价"
      )
      Vector.empty
    else
      settled(orderId) = settled.get(orderId) match
        case Some(prev) => prev.copy(cumulative = cumulativeQty)
        case None       => RestTradingGateway.Settlement(cumulativeQty, firstSeenAt = now, terminalAt = None)
      ledger = ledger.applyFill(exchange, symbol, side, price, delta)
      val position = positionOf(symbol)
      logger.info(
        s"成交入账 $target $symbol $side ${delta.value} @ ${price.value} -> 仓位 ${position.size.value} (order=$orderId)"
      )
      // localTs 取本地时刻 —— 它是延迟度量的基准, 盖成交易所时间会让事件看起来零延迟
      Vector(TradingGateway.positionEvent(position, ts, now))

  /** 清记账进度 —— **只清已终态且过了保留期的**。
    *
    * 非终态的条目**永远不清**，哪怕它看起来已经很久没动静：在累计量记账的体系里，
    * 清掉一张还活着的订单的进度，下一条带真累计量的回报就会以"已记 0"重新记一遍，
    * **此前入账的全部数量再记一次** —— 仓位近乎翻倍，且没有自愈路径。
    * 而"部分成交之后继续挂着"是完全正常的形态 (GTC、冰山单)，按"存在多久"去判它是不是
    * 僵尸，判据本身就不成立。
    *
    * 长期没动静的非终态条目只**告警**：那通常意味着丢了一条订单推送，值得查。
    * 条目本身几十字节，真丢了推送的订单本就该人工介入，重启对齐时自然清零。
    *
    * 在订单终态与对账两处触发。只靠终态触发的话，一段时间没有订单终态就不清了。
    */
  private def evictSettled(now: Timestamp): Unit =
    // 先挑出要告警的, 再改、再清 —— 遍历一个 map 的同时改它是自找麻烦
    val stale = settled.iterator.collect {
      case (orderId, s)
          if s.terminalAt.isEmpty && !s.staleWarned && now - s.firstSeenAt > RestTradingGateway.StaleSettlementMs =>
        orderId -> s
    }.toVector
    stale.foreach { (orderId, s) =>
      settled(orderId) = s.copy(staleWarned = true) // 只报一次, 别每次对账都刷屏
      logger.warn(
        s"$target order=$orderId 有成交 ${s.cumulative.value} 却长期没等到终态回报 —— " +
          "通常意味着丢了一条订单推送, 值得查。记账进度保留 (清掉会让下一条累计量从零重记, 仓位翻倍)"
      )
    }
    settled.filterInPlace((_, s) => RestTradingGateway.retains(s, now))

  /** 视作零的量级 —— **币本位**。
    *
    * `sizeStep` 是**张**，而这里比的是币 (见 [[SymbolMeta.minOrderSize]] 的说明)：
    * 直接拿它当阈值，在 contractSize=0.01 的品种上会把阈值放大一百倍，
    * 于是真实成交被静默丢弃 —— 不入账、不发回报、连告警都没有。
    */
  private def dustOf(symbol: Symbol): Double =
    metas.get(symbol).map(meta => meta.toCoin(Contracts(meta.sizeStep)).value / 2).getOrElse(0.0)

  private def positionOf(symbol: Symbol): Position =
    ledger.positions.getOrElse(symbol, Position.empty(account, exchange, symbol))

    /** 三方对账：两本独立记出来的账 + 交易所报的读数，两两比对。
    *
    * 三条渠道各说各话时，**谁跟谁对不上**决定了这是什么性质的问题：
    *
    *   - **成交明细账 vs 订单回报账**：两者都是我们自己记的，来源却完全不同 (逐笔明细 /
    *     累计成交量)。对不上说明**我们这边有 bug** —— 少解析了一条推送、字段读错了、
    *     去重去多了。这不是外部世界的事，是代码的事。
    *   - **订单回报账 vs 交易所仓位**：账户被外部改了 (强平、手动干预、资金费结算)，
    *     或者我们漏收了回报。这是外部事实，只能报出来等人处置。
    *
    * 只有这个区分能让告警变得可行动：内部 bug 要改代码，外部事实要人去交易所看一眼。
    * 两方对账做不到 —— 它只能说"对不上"。
    *
    * **偶尔一次不算数**。成交与仓位走不同频道、订单回报与成交推送也不同步，任一时刻
    * 三本账都可能差着一笔。连续 [[RestTradingGateway.DisagreementsBeforeAlarm]] 次
    * 仍然对不上才升级为告警 —— 时序窗口撑不了那么久，撑得住的只有真问题。
    */
  private def reconcile(): Unit =
    syncedSymbols.toVector.sorted.foreach { symbol =>
      val verdicts = RestTradingGateway.compare(
        byOrders = positionOf(symbol).size,
        byFills = fillLedger.positions.get(symbol).map(_.size).getOrElse(Coin.Zero),
        reported = reportedPositions.get(symbol),
        tolerance = dustOf(symbol) * 2,
      )
      verdicts.foreach(record(symbol, _))
    }

  /** 记一次比对结果：一致就清零，不一致就累加，连续够多次才喊。
    *
    * 中间那几次只留 debug —— 它们多半是时序窗口，报出来会把真问题淹掉。
    */
  private def record(symbol: Symbol, verdict: RestTradingGateway.Verdict): Unit =
    import RestTradingGateway.Verdict
    val key = (symbol, verdict.kind)
    verdict match
      case _: Verdict.Agreed => disagreements.remove(key)
      case mismatch =>
        val times = disagreements.getOrElse(key, 0) + 1
        disagreements(key) = times
        if times == RestTradingGateway.DisagreementsBeforeAlarm then logger.error(s"!!! $target $symbol ${mismatch.explain}")
        else if times < RestTradingGateway.DisagreementsBeforeAlarm then
          logger.debug(s"$target $symbol ${verdict.kind} 对账第 $times 次不一致 (未到告警阈值, 多半是时序窗口)")
        // 超过阈值之后不再重复刷屏, 首次告警已经说清楚了

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

  /** 拉真实持仓并**据此重置账本** —— 对齐是账本唯一的权威初值来源。
    *
    * 这也是漂移之后的修复入口：REST 是快照，不参与推送的流竞争。
    *
    * 账户归属由柜台盖章，不信客户端填的那个 —— 各家适配层历史上都硬编码成实盘，
    * 而"这份回报属于哪个账户"是装配期的事实，只有柜台知道。
    */
  override protected def syncPositions(symbols: Set[Symbol]): Vector[Position] =
    client.fetchPositions() match
      case Right(positions) =>
        val mine = positions.filter(p => symbols.contains(p.symbol)).map(_.copy(account = account))
        val refreshed = mine.map(p => p.symbol -> p).toMap
        // **只重置这批标的**。引擎为每批新加的策略都会发一次对齐指令 (symbols 只含那一批),
        // 整本替换会把先装的策略的仓位连同它的记账进度一起抹掉 —— 那些策略从此按零仓决策,
        // 而没有任何症状。这批标的里交易所没返回的视作零仓, 从账本里摘掉旧值。
        ledger = Ledger(account, (ledger.positions -- symbols) ++ refreshed, cash = 0.0)
        fillLedger = Ledger(account, (fillLedger.positions -- symbols) ++ refreshed, cash = 0.0)
        // 记账进度不清: 这批标的还活着的订单会被挂单快照覆盖 (见 syncPendingOrders),
        // 已终态的靠墓碑自然过期。清掉反而会让晚到的回报从零重记。
        reportedPositions --= symbols
        disagreements.filterInPlace((key, _) => !symbols.contains(key._1))
        syncedSymbols ++= symbols
        mine
      case Left(e) => throw IllegalStateException(s"$exchange 拉取初始持仓失败: ${e.message}")

  /** 拉既有挂单，并**用它们的已成交量初始化记账进度**。
    *
    * 少了这一步，对齐后的第一条订单回报会把对齐之前就已经成交的量再记一遍：
    * 拉到的仓位里本已含着那 0.3，而记账进度是空的，于是 `cumExecQty = 0.5` 被算成增量 0.5
    * 而不是 0.2 —— 仓位凭空多出 0.3，且没有任何症状。
    *
    * 只在"对齐时账户里有部分成交的挂单"才触发，不常见，但一触发就是仓位错。
    */
  override protected def syncPendingOrders(symbols: Set[Symbol]): Vector[OrderUpdate] =
    val orders = symbols.toVector.sortBy(_.toString).flatMap { symbol =>
      client.fetchPendingOrders(symbol) match
        case Right(updates) => updates.map(_.copy(account = account))
        case Left(e)        => throw IllegalStateException(s"$exchange $symbol 拉取既有挂单失败: ${e.message}")
    }
    val now = nowMs
    orders.filter(_.filledQuantity.nonZero).foreach { order =>
      settled(order.orderId) = RestTradingGateway.Settlement(order.filledQuantity, firstSeenAt = now, terminalAt = None)
      logger.info(s"接管既有挂单 $target ${order.symbol} order=${order.orderId} 已成交 ${order.filledQuantity.value}")
    }
    orders

  override protected def currentAccountInfo(): AccountInfo =
    client.fetchAccountInfo() match
      case Right(info) => info.copy(account = account)
      case Left(e)     => throw IllegalStateException(s"$exchange 拉取账户信息失败: ${e.message}")

object RestTradingGateway:
  /** 多久对一次账。时钟一秒一拍, 对账不必那么勤 —— 它抓的是持续存在的偏差 */
  val ReconcileIntervalMs: Long = 5_000

  /** 一次三方比对的结论。
    *
    * 分成两类不是分类癖 —— **它们要用完全不同的方式处理**：内部不一致要改代码，
    * 外部不一致要人去交易所看一眼。两方对账给不出这个区分，它只能说"对不上"。
    */
  private[exchange] enum Verdict(val kind: String):
    case Agreed(k: String) extends Verdict(k)
    /** 两条内部渠道对不上 —— 我们这边有 bug */
    case Internal(byOrders: Coin, byFills: Coin) extends Verdict("内部")
    /** 账本与交易所对不上 —— 账户被外部改了, 或漏收了回报 */
    case External(byOrders: Coin, reported: Coin) extends Verdict("外部")

    def explain: String = this match
      case Agreed(_) => "一致"
      case Internal(byOrders, byFills) =>
        s"两条内部渠道对不上: 订单回报账=${byOrders.value} 成交明细账=${byFills.value}。" +
          "**这是我们这边的 bug** —— 少解析了一条推送 / 字段读错 / 去重去多了, 与账户被外部改动无关, 请查适配层"
      case External(byOrders, reported) =>
        s"账本与交易所对不上: 本地=${byOrders.value} 交易所=${reported.value}。" +
          "强平/手动干预/资金费/漏收回报都会造成。策略正按本地账本决策, 需人工确认; " +
          "确认后**先撤掉在途单**再发账户对齐指令让柜台按 REST 快照重置 —— " +
          "有单在途时重置会把一笔既在快照里、回报又还在路上的成交记两遍"

  /** 三方比对 —— 纯函数，判定与告警节流分开，前者才是要盯住的那部分。
    *
    * 交易所读数缺席时 (还没推过) 只做内部比对：那不是"一致"，是"无从比较"。
    */
  private[exchange] def compare(byOrders: Coin, byFills: Coin, reported: Option[Coin], tolerance: Double): Vector[Verdict] =
    val internal =
      if (byOrders - byFills).abs.value <= tolerance then Verdict.Agreed("内部")
      else Verdict.Internal(byOrders, byFills)
    val external = reported.map { r =>
      if (byOrders - r).abs.value <= tolerance then Verdict.Agreed("外部") else Verdict.External(byOrders, r)
    }
    internal +: external.toVector

  /** 连续对不上多少次才升级为告警。
    *
    * 偶尔一次是时序窗口 (成交与仓位走不同频道, 账本落后一笔是常态), 那种落后会在
    * 下一条回报到达后自行消失。连续三次跨越十几秒仍然对不上, 时序窗口解释不了。
    */
  val DisagreementsBeforeAlarm: Int = 3

  /** 一张订单的记账进度。`terminalAt` 有值表示已终态，只等过保留期被清掉 */
  private[exchange] final case class Settlement(
      cumulative: Coin,
      firstSeenAt: Timestamp,
      terminalAt: Option[Timestamp],
      staleWarned: Boolean = false,
  )

  /** 这条记账进度此刻还该不该留着 —— [[RestTradingGateway.evictSettled]] 的判据。
    *
    * 提成纯函数是为了测得动：判据错了 (清掉活着的订单) 的后果是仓位翻倍，
    * 而那要在实盘上跑一小时才看得见。
    */
  private[exchange] def retains(settlement: Settlement, now: Timestamp): Boolean =
    settlement.terminalAt.forall(at => now - at <= SettledRetentionMs)

  /** 订单终态后，记账进度还要留多久。
    *
    * 留着是为了让晚到的成交推送认得出"这笔已经记过了"。跨频道的迟到在秒级，
    * 取一分钟留足余量 —— 代价只是多占一会儿内存，而清早了就是仓位翻倍。
    */
  val SettledRetentionMs: Long = 60_000

  /** 一张单停在"有成交、无终态"多久之后值得报一句。
    *
    * **只报，不清** (见 [[RestTradingGateway.evictSettled]])。取一小时：分批成交的挂单
    * 挂上几小时是正常的，所以这条告警是"值得看一眼"而不是"一定有问题"。
    */
  val StaleSettlementMs: Long = 60 * 60 * 1000


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
