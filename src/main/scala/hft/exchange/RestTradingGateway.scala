package hft.exchange

import hft.domain.*
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

  override protected def connect(): Unit = feed.connect(account, publish, body => fork(body))

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
  override protected def syncPositions(symbols: Set[Symbol]): Vector[Position] =
    client.fetchPositions() match
      case Right(positions) => positions.filter(p => symbols.contains(p.symbol)).map(_.copy(account = account))
      case Left(e)          => throw IllegalStateException(s"$exchange 拉取初始持仓失败: ${e.message}")

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
  /** 拉取本所合约规格并装好柜台。
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
    val metas = client.fetchAllSymbolMetas() match
      case Right(ms) => ms.map(m => m.symbol -> m).toMap
      case Left(e)   => throw IllegalStateException(s"${client.exchange} 预加载合约规格失败: ${e.message}")
    RestTradingGateway(client, feed, account, metas, accountRefreshMs)
