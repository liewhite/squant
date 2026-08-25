package hft.exchange

import hft.domain.*


/** 交易所客户端统一接口，仅封装 REST 交互。
  *
  * 所有方法同步阻塞 (运行在虚拟线程上)，错误以 Either 显式返回。
  */
trait ExchangeClient:
  def exchange: Exchange

  /** 获取所有交易对元数据 */
  def fetchAllSymbolMetas(): Either[ExchangeError, Vector[SymbolMeta]]

/** 私有 REST —— **拿到这个类型本身就意味着凭证已经具备**。
  *
  * 从前只有一个 `ExchangeClient`，凭证有没有靠 `hasCredentials: Boolean` 问，三个
  * `AccountStream` 各写一句 `require(client.hasCredentials)`，客户端内部再各写一句
  * `if !hasCredentials then Left(Auth)` —— 同一个缺失的区分被复制成谓词 + 守卫 + 错误值三份。
  *
  * 更别扭的是"没有凭证"这个**构造时就已确定的静态事实**被塞进了错误通道：启动对齐要专门写
  * `case Left(ExchangeError.Auth(_)) => 跳过`，把一个装配问题伪装成运行时故障在调用链上传递。
  *
  * 现在它是类型：没凭证就拿不到 `TradingClient`，那些 require、守卫和 Auth 分支一起消失。
  * 各家客户端经伴生对象的 `public` / `trading` 两个工厂给出对应的类型（见
  * [[hft.exchange.binance.BinanceClient]]）。
  */
trait TradingClient extends ExchangeClient:

  /** 下单，返回交易所订单 ID */
  def placeOrder(order: ExchangeOrder): Either[ExchangeError, OrderId]

  /** 撤单。[[OrderRef]] 决定按交易所 id 还是按 clientOrderId 指名 —— 在途单只有后者 */
  def cancelOrder(symbol: Symbol, ref: OrderRef): Either[ExchangeError, Unit]

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
