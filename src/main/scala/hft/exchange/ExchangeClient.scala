package hft.exchange

import hft.domain.*
import hft.event.{Interest, Subscription, Topic, Topics}

/** 订阅类型 (仅 public 行情需要订阅)。
  *
  * Private 数据 (Position/Balance/OrderUpdate/AccountInfo) 在 connector 启动时自动处理
  */
enum SubscriptionKind:
  case FundingRate(symbol: Symbol)
  case BBO(symbol: Symbol)
  case MarkPrice(symbol: Symbol)
  case IndexPrice(symbol: Symbol)
  /** 公共成交印记 (逐笔成交)，作策略信号 (如 K 线/动量)，不参与撮合 */
  case Trade(symbol: Symbol)

  def subscribedSymbol: Symbol = this match
    case FundingRate(s) => s
    case BBO(s)         => s
    case MarkPrice(s)   => s
    case IndexPrice(s)  => s
    case Trade(s)       => s

object SubscriptionKind:
  /** 公共行情 topic 与交易所流的对应关系 —— 唯一一张表。
    *
    * 私有回报与账户级读数不在其中：它们由账户流推送，不需要订阅。
    */
  private val streamOf: Vector[(Topic[Instrument, ?], Symbol => SubscriptionKind)] = Vector(
    Topics.Bbo -> BBO.apply,
    Topics.Trade -> Trade.apply,
    Topics.MarkPrice -> MarkPrice.apply,
    Topics.IndexPrice -> IndexPrice.apply,
    Topics.FundingRate -> FundingRate.apply,
  )

  // 本表必须与 Topics.market 严格对应: 前者是"哪些 topic 需要向交易所订阅", 后者是
  // "这些 topic 具体订哪条流"。分层不允许把两者合成一处 (Topics 在下层, 不能依赖本层),
  // 故用加载期断言钉住 —— 新增一个行情 topic 却忘了给它映射, 立即失败而不是静默不订阅。
  require(
    streamOf.map(_._1).toSet == Topics.market,
    s"streamOf 与 Topics.market 不一致: 表内=${streamOf.map(_._1.name).sorted}, 应为=${Topics.market.map(_.name).toVector.sorted}",
  )

  /** 从订阅范围派生出要向各交易所订阅的公共行情流。
    *
    * 这是"一处声明、两处派生"的第二处 (第一处是总线的投递索引)：策略只写一遍
    * [[Interest]]，行情订阅与事件过滤都从它来，不存在两份声明错开的可能。
    */
  def from(subscription: Subscription): Set[(Exchange, SubscriptionKind)] =
    subscription.interests.foreach {
      case Interest.All(t) if streamOf.exists(_._1 eq t) =>
        sys.error(
          s"公共行情 topic '$t' 只能用 Interest.Keyed 声明: Interest.All 没有标的集合, " +
            "框架无从知道该向交易所订阅哪些流"
        )
      case _ => ()
    }
    streamOf.flatMap { (topic, mkKind) =>
      subscription.interests
        .flatMap(_.keysOf(topic))
        .map(instrument => (instrument.exchange, mkKind(instrument.symbol)))
    }.toSet

/** 交易所客户端统一接口，仅封装 REST 交互。
  *
  * 所有方法同步阻塞 (运行在虚拟线程上)，错误以 Either 显式返回。
  */
trait ExchangeClient:
  def exchange: Exchange

  /** 获取所有交易对元数据 */
  def fetchAllSymbolMetas(): Either[ExchangeError, Vector[SymbolMeta]]

  /** 下单，返回交易所订单 ID */
  def placeOrder(order: Order): Either[ExchangeError, OrderId]

  /** 撤单 */
  def cancelOrder(symbol: Symbol, orderId: OrderId): Either[ExchangeError, Unit]

  /** 查询当前挂单 (live + partially_filled) */
  def fetchPendingOrders(symbol: Symbol): Either[ExchangeError, Vector[OrderUpdate]]

  /** 设置杠杆 */
  def setLeverage(symbol: Symbol, leverage: Int): Either[ExchangeError, Unit]

  /** 获取账户信息 (净值 + 总持仓名义价值) */
  def fetchAccountInfo(): Either[ExchangeError, AccountInfo]

  /** 启动期查询所有 symbol 的持仓。
    *
    * 用于在 executor 注册之后、市场订阅之前同步初始状态，避免策略基于陈旧/缺失的
    * position 做出决策。没有默认实现——每个交易所都必须显式表态：
    * REST 直查的实际请求持仓接口；走私有 WS snapshot 的返回 Right(Vector.empty)
    * 并注释说明数据来源，避免"沉默漏推"
    */
  def fetchPositions(): Either[ExchangeError, Vector[Position]]
