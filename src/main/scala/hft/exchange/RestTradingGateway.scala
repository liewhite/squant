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

  override def exchange: Exchange = client.exchange

  /** 仓位账本 + 记账进度 + 三方对账 —— 全在 [[PositionBook]] 里，本类只管把消息喂进去、
    * 把结果发出来。唯一写者是 actor 线程 (推送经 [[hft.actor.ActorContext.tell]] 串行化进来)。 */
  private val book = PositionBook(account, exchange, dustOf)

  /** 上次对账时刻 —— 时钟一秒一拍, 对账不必那么勤 */
  private var lastAuditAt: Timestamp = 0L

  /** 除下单与对齐之外还收时钟 —— 对账与记账进度的清理按节拍走 */
  override protected def extraInterests: Set[Interest] = Set(Interest.All(Topics.Clock))

  /** 汇报面解析出的每一条都排进本柜台的邮箱 (不经总线)，由 actor 线程按序消费 ——
    * 于是"柜台是回报的唯一发布者"成立，账本也只有一个写者。 */
  override protected def connect(): Unit =
    feed.connect(
      report => tell(Event.local(GatewayInboxes, GatewayInbox(report))),
      body => fork(body),
      ms => sleepUnlessStopped(ms),
    )

  override protected def onOther(event: AnyEvent, now: Timestamp): Vector[AnyEvent] =
    event.as(Topics.Clock).foreach { _ =>
      if now - lastAuditAt >= RestTradingGateway.AuditIntervalMs then
        lastAuditAt = now
        book.audit(now).foreach(report)
    }
    event.as(GatewayInboxes).map(inbox => handle(inbox.report, now)).getOrElse(Vector.empty)

  /** 把一条账户变动翻译成总线上的回报。
    *
    * 先按标的分流：交易所推的是**整个账户**的动静，而本柜台管的是引擎交给它对齐的那些标的。
    * 别人的标的不归它记账，也不归它对账。对齐之前 [[PositionBook.manages]] 一律为假 ——
    * 那时它既不知道自己管什么，账本也还没有初值，处理了只会算错。
    */
  private def handle(report: AccountReport, now: Timestamp): Vector[AnyEvent] =
    instrumentOf(report) match
      case Some(instrument) if !book.manages(instrument) =>
        logger.debug(s"$target 忽略不归本柜台管的标的: $instrument")
        Vector.empty
      case _ => translate(report, now)

  /** 这条报告说的是哪个标的。账户级读数 (余额/净值/希腊值) 没有标的，不参与分流 */
  private def instrumentOf(report: AccountReport): Option[Instrument] = report match
    case r: AccountReport.Executed           => Some(r.instrument)
    case r: AccountReport.OrderStatusChanged => Some(r.instrument)
    case r: AccountReport.PositionReported   => Some(r.instrument)
    case _                                   => None

  private def translate(report: AccountReport, now: Timestamp): Vector[AnyEvent] = report match
    case AccountReport.Executed(instrument, side, price, qty, ts) =>
      // 成交明细不进主账本 (仓位由订单回报的累计量驱动)，但要进第二本账 —— 它是对账的
      // 另一条独立渠道，两本对不上就说明我们这边漏了什么。
      book.recordFill(instrument, side, qty)
      Vector(Event.stamped(
        Topics.Fill,
        Fill(account, exchange, instrument.symbol, side, price, qty, ts, kind = instrument.kind),
        ts,
        now,
      ))

    case AccountReport.OrderStatusChanged(
          orderId, clientOrderId, instrument, side, status, price, avgFillPrice, quantity, filledQuantity, reduceOnly, ts
        ) =>
      // 记账用**成交均价**而不是委托价: 市价单的委托价是空的, 拿它记账会把持仓均价记成 0。
      val settlement =
        if filledQuantity.isZero then Vector.empty
        else settle(orderId, instrument, side, avgFillPrice, filledQuantity, ts, now)
      if status.isTerminal then book.markTerminal(orderId, now)
      val update =
        OrderUpdate(
          account, orderId, clientOrderId, exchange, instrument.symbol, side, status, price, quantity,
          filledQuantity, reduceOnly, ts, kind = instrument.kind,
        )
      // 顺序在这里构造: 仓位先于订单状态。反过来的话, 策略会看到"挂单已消失、仓位还没更新"
      // —— 它据此认为自己既没单也没仓位, 于是再下一单。那是危险侧的中间状态。
      settlement :+ Event.stamped(Topics.OrderUpdate, update, ts, now)

    case AccountReport.BalanceChanged(currency, amount, ts) =>
      Vector(Event.stamped(Topics.Balance, Balance(account, exchange, currency, amount, ts), ts, now))

    case AccountReport.EquityChanged(equity, ts) =>
      Vector(Event.stamped(Topics.AccountInfo, AccountInfo(account, exchange, equity), ts, now))

    case AccountReport.GreeksChanged(ccy, delta, gamma, theta, vega, ts) =>
      Vector(Event.stamped(Topics.Greeks, Greeks(account, exchange, ccy, delta, gamma, theta, vega, ts), ts, now))

    case AccountReport.PositionReported(instrument, size, _) =>
      book.observeReported(instrument, size) // 只记下来, 按节拍统一对账
      Vector.empty // 总线上的仓位只有一个来源: 本账本

  private def settle(
      orderId: OrderId,
      instrument: Instrument,
      side: Side,
      price: Price,
      cumulative: Coin,
      ts: Timestamp,
      now: Timestamp,
  ): Vector[AnyEvent] =
    book.settle(orderId, instrument, side, price, cumulative, now) match
      case PositionBook.Settled.Recorded(delta, position) =>
        logger.info(
          s"成交入账 $target $instrument $side ${delta.value} @ ${price.value} -> 仓位 ${position.size.value} (order=$orderId)"
        )
        // localTs 取本地时刻 —— 它是延迟度量的基准, 盖成交易所时间会让事件看起来零延迟
        Vector(TradingGateway.positionEvent(position, ts, now))
      case PositionBook.Settled.Unchanged => Vector.empty
      case PositionBook.Settled.Regressed(cumulative, already) =>
        // 不出声到 warn: 乱序后到的旧推送本就是常态路径, 报出来只会变成噪声。
        // 但适配层读错字段的表现也是它 —— 留一条 debug, 排查时这是唯一线索。
        logger.debug(
          s"$target $instrument order=$orderId 累计成交量倒退: 这条报 ${cumulative.value}, 已记 ${already.value} —— " +
            "不入账。多半是乱序后到的旧推送; 若持续出现, 查适配层是否读错了累计量字段"
        )
        Vector.empty

  private def report(alarm: PositionBook.Alarm): Unit = alarm match
    case PositionBook.Alarm.Disagreement(instrument, verdict) =>
      logger.error(s"!!! $target $instrument ${verdict.explain}")
    case PositionBook.Alarm.StaleSettlement(orderId, cumulative) =>
      logger.warn(
        s"$target order=$orderId 有成交 ${cumulative.value} 却长期没等到终态回报 —— " +
          "通常意味着丢了一条订单推送, 值得查。记账进度保留 (清掉会让下一条累计量从零重记, 仓位翻倍)"
      )

  /** 视作零的量级 —— **币本位**。
    *
    * `sizeStep` 是**张**，而账本比的是币 (见 [[SymbolMeta.minOrderSize]] 的说明)：
    * 直接拿它当阈值，在 contractSize=0.01 的品种上会把阈值放大一百倍，
    * 于是真实成交被静默丢弃 —— 不入账、不发回报、连告警都没有。
    *
    * 缺规格时走 [[metaOf]] 抛错而不是退回一个默认值：退回 0 的话对账容差也成了 0，
    * 两本各自累加的浮点账会以 1e-16 的差异连续几拍报出"这是我们这边的 bug"。
    * 而这个分支本就该不可达 —— [[syncSnapshot]] 在对齐入口挡过了。
    */
  private def dustOf(instrument: Instrument): Double =
    val meta = metaOf(instrument.symbol)
    meta.toCoin(Contracts(meta.sizeStep)).value / 2

  override protected def metaOf(symbol: Symbol): SymbolMeta =
    metas.getOrElse(symbol, sys.error(s"$exchange 没有 $symbol 的合约规格, 无法发单 (装配时未加载?)"))

  /** 每个 REST 调用 fork 独立虚拟线程，互不阻塞，也不阻塞柜台的事件循环。
    *
    * "不真下单"不是这里的开关，而是换一个客户端实现 ([[DryRunClient]]) ——
    * 它以 [[ExchangeError.Rejected]] 返回，正好落在拒单通道上，本类因此一个分支都不需要。
    *
    * ## 分类看语义，不看数字
    *
    * 三条通道对应三种语义：**明确拒绝**(订单确定未成立) -> 回流策略；**限频**(前提被打破)
    * -> 终止；**其余**(结果不确定) -> 终止。判据是 [[ExchangeError]] 的类型，不是 HTTP 状态码。
    *
    * 从前这里按 `Http(4xx)` 判"明确拒绝"，而那只是 **Binance** 表达业务拒单的形状：
    * OKX 拒单是 HTTP 200 + `data[0].sCode != "0"`、Bybit 是 HTTP 200 + `retCode != 0`，
    * 两家都落进了最后一条"结果不确定"通道。于是 OKX/Bybit 上一次「保证金不足」等于**进程崩溃**，
    * 而且策略侧的 pending 登记也不会被清理。归一到语义之后，各交易所在自己的边界翻译数字。
    */
  override protected def placeAligned(order: Order, now: Timestamp): Unit =
    fork {
      client.placeOrder(OrderConversion.toExchangeOrder(order, metaOf(order.symbol))) match
        case Right(orderId) =>
          // 订单确认 (Pending/Filled) 以私有流推送为准，这里只记录
          logger.info(s"下单已受理: $exchange ${order.symbol} orderId=$orderId clientOrderId=${order.clientOrderId}")
        case Left(e: ExchangeError.RateLimited) =>
          // 限频/封禁: 说明"订单生命周期自然限速"的假设已被打破，
          // 按拒单回流会形成"拒单->重挂->更多请求"的重试风暴，必须终止
          throw IllegalStateException(s"被交易所限频, 终止: $exchange ${order.symbol} ${e.message}")
        case Left(e: ExchangeError.Rejected) =>
          // 交易所明确拒绝，订单确定未成立 -> 回流策略 (与精度拒绝同一条路径)
          logger.warn(s"下单被拒: $exchange ${order.symbol} ${e.message}")
          reject(order, e.message, now)
        case Left(e) =>
          // 网络/超时/5xx/系统错误: 订单是否成立不确定，本地状态无法保证正确
          throw IllegalStateException(s"下单结果不确定, 终止: $exchange ${order.symbol} ${e.message}")
    }

  override protected def cancelOrder(instrument: Instrument, ref: OrderRef, now: Timestamp): Unit =
    fork {
      client.cancelOrder(instrument, ref) match
        case Right(())                                 => logger.info(s"撤单已受理: $instrument ${ref.raw}")
        case Left(ExchangeError.OrderNotFound(reason)) =>
          // 订单已成交/已撤销，终态同样由私有流推送，撤单失败非致命
          logger.info(s"订单已不在: $instrument ${ref.raw} ($reason)")
        case Left(e) =>
          throw IllegalStateException(s"撤单结果不确定, 终止: $instrument ${ref.raw} ${e.message}")
    }

  /** 拉一份**互相一致**的仓位/挂单快照并据此对齐账本。
    *
    * 仓位与挂单是两次独立的 REST，中间夹进一笔成交就会让两份快照对不上：成交落在
    * "拉仓位"之后、"拉挂单"之前时，账本没含它、记账进度却已含它 —— 那笔成交从此永久
    * 漏记（推送到达时增量为零），仓位低估且不会自愈。
    *
    * 读一致快照的标准手法：夹着仓位前后各拉一次挂单，两次的已成交量相同即说明这中间
    * 没有成交发生。静默标的一次就收敛。
    */
  override protected def syncSnapshot(instruments: Set[Instrument]): TradingGateway.AccountSnapshot =
    // 先确认这批标的都有合约规格。对齐是它们进入本柜台视野的**唯一入口**，
    // 在这里挡住, 后面的 dustOf / metaOf 就都落在"必然有规格"的前提上 (缺失即抛)。
    instruments.foreach(i => metaOf(i.symbol))
    val (positions, orders) = consistentSnapshot(instruments, attempt = 1)
    val mine = positions.filter(p => instruments.contains(p.instrument)).map(_.copy(account = account))
    book.align(instruments, mine, orders, nowMs)
    orders.filter(_.filledQuantity.nonZero).foreach { order =>
      logger.info(s"接管既有挂单 $target ${order.symbol} order=${order.orderId} 已成交 ${order.filledQuantity.value}")
    }
    TradingGateway.AccountSnapshot(mine, orders)

  private def consistentSnapshot(instruments: Set[Instrument], attempt: Int): (Vector[Position], Vector[OrderUpdate]) =
    val before = fetchOrders(instruments)
    val positions = client.fetchPositions() match
      case Right(ps) => ps
      case Left(e)   => throw IllegalStateException(s"$exchange 拉取初始持仓失败: ${e.message}")
    val after = fetchOrders(instruments)
    if filledByOrder(before) == filledByOrder(after) then (positions, after)
    else if attempt >= RestTradingGateway.SnapshotAttempts then
      // 拿不到一致快照就**不启动**。
      //
      // fetchPositions 的契约原文是"拉不到就返回 Left 让启动失败 —— 账户状态没对上就开始交易,
      // 比不启动危险得多"。从前这里 warn 一句然后放行, 理由是"交给三方对账兜底" —— 而对账只
      // 告警不修复, 那一笔会永久漏记, 之后每一次仓位判断都带着这个偏差。同一个模块的两处
      // 契约不能一处说"没对上就别启动"、另一处说"差一笔也先跑起来"。
      //
      // 抛出让外层拉起重试: 重启后账户很可能已经静下来, 那才是一致快照该来的时候。
      throw IllegalStateException(
        s"$target 连取 $attempt 次仍未拿到互相一致的仓位/挂单快照 (期间一直有成交), " +
          "对齐无法保证账本正确, 拒绝启动 —— 账户静下来后重试, 或先撤掉本柜台标的的全部挂单"
      )
    else
      logger.info(s"$target 取快照期间有成交进来, 重取 (第 ${attempt + 1} 次)")
      consistentSnapshot(instruments, attempt + 1)

  private def filledByOrder(orders: Vector[OrderUpdate]): Map[OrderId, Coin] =
    orders.map(o => o.orderId -> o.filledQuantity).toMap

  private def fetchOrders(instruments: Set[Instrument]): Vector[OrderUpdate] =
    instruments.toVector.sortBy(_.toString).flatMap { instrument =>
      client.fetchPendingOrders(instrument) match
        case Right(updates) => updates.map(_.copy(account = account))
        case Left(e)        => throw IllegalStateException(s"$instrument 拉取既有挂单失败: ${e.message}")
    }

  override protected def currentAccountInfo(): AccountInfo =
    client.fetchAccountInfo() match
      case Right(info) => info.copy(account = account)
      case Left(e)     => throw IllegalStateException(s"$exchange 拉取账户信息失败: ${e.message}")

  /** 完整钱包 —— 拉不到即抛: 缺它策略分不清"某币余额是 0"与"还没见过它",
    * 而 delta 对冲要靠这个区分决定是否把现货算进敞口 (见 `TradingClient.fetchWallet`)。 */
  override protected def currentWallet(): Map[String, Double] =
    client.fetchWallet() match
      case Right(balances) => balances
      case Left(e)         => throw IllegalStateException(s"$exchange 拉取钱包失败: ${e.message}")

object RestTradingGateway:
  /** 多久对一次账。时钟一秒一拍, 对账不必那么勤 —— 它抓的是持续存在的偏差 */
  val AuditIntervalMs: Long = 5_000

  /** 取一致快照最多试几次。一直失败说明账户正忙, 那时接受一份可能差一笔的快照,
    * 交给三方对账兜底 —— 比无限重试卡住启动强。 */
  val SnapshotAttempts: Int = 3

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
