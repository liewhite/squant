package hft.exchange

import hft.domain.*

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
