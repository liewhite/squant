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

  /** 按 symbol 索引的合约规格 —— **进程内只拉一次**。
    *
    * 一个交易所的接入有三处要它：行情源把盘口数量从张换成币、汇报面把回报换回币、
    * 柜台把下单意图对齐到交易所精度。三处各拉一次就是同一事实的三份副本
    * （合约规格在进程生命周期内不变，但两次拉取之间交易所上了新合约的话它们就不一致了），
    * 而且启动时白打两次 REST。
    *
    * `final`：各家客户端不该再各写一份索引方式。
    *
    * 失败即抛：缺规格就发不出单、换不了算，与其在首笔下单时炸，不如装配期就失败。
    */
  final lazy val symbolMetas: Map[Symbol, SymbolMeta] = fetchAllSymbolMetas() match
    case Right(metas) => metas.map(m => m.symbol -> m).toMap
    case Left(e)      => throw IllegalStateException(s"$exchange 预加载合约规格失败: ${e.message}")

/** 私有 REST —— **拿到这个类型本身就意味着凭证已经具备**。
  *
  * 从前只有一个 `ExchangeClient`，凭证有没有靠 `hasCredentials: Boolean` 问，三个
  * 各家账户流各写一句 `require(client.hasCredentials)`，客户端内部再各写一句
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

  /** 获取账户信息 (净值) */
  def fetchAccountInfo(): Either[ExchangeError, AccountInfo]

  /** 账户的**完整**钱包 (币种 -> 余额)。未列出的币种余额为 0。
    *
    * ## 为什么它必须是 REST，而不只靠私有流
    *
    * 期权 delta 对冲要 `期权 delta + 该币现金余额` 才能给出总敞口，缺余额就只能暂停对冲
    * (见 `hft.state.StateManager.greeks`)。而"某币余额是 0"与"还没见过这个币"必须分得开：
    * OKX 余额为 0 时**不下发该币种行**，Bybit 的 wallet 频道更是明确"订阅成功时不给
    * snapshot、只在变动时推" —— 一个不做现货的账户可能几天等不到一条推送。
    *
    * 所以完整钱包由启动对齐经 REST 建立 (与持仓同理)，私有流之后只负责更新。
    * 两家的 REST 钱包接口本来就一次返回整份，此前只是把币种明细丢掉了。 */
  def fetchWallet(): Either[ExchangeError, Map[String, Double]]

  /** 启动对齐用的持仓快照 —— **必须 REST 直查, 不允许返回空让私有流去补**。
    *
    * 柜台拿它给账本定初值 (见 [[hft.exchange.PositionBook.align]]), 而账本是总线上仓位的
    * 唯一来源。返回空就等于告诉柜台"这个账户是平的"。
    *
    * 曾经允许"走私有 WS snapshot 的返回 `Right(Vector.empty)` 并注释说明数据来源",
    * 那条约定在**仓位归柜台算**之后失效了, 两道独立的原因:
    *
    *   1. 私有流推来的仓位走 [[AccountReport.PositionReported]], 柜台只把它当**交易所第三方
    *      读数**记进对账, 不进账本。
    *   2. 柜台的 `connect()` 在 onStart 里跑, 早于对齐指令 —— snapshot 到达时柜台还不知道
    *      自己管哪些标的, 报告在入口就被分流掉了。
    *
    * 照旧约定实现的表现是: 账户带仓重启 -> 策略收到一串零仓 -> 按空仓决策, 且只有一句
    * 措辞含糊的对账告警。**没有默认实现**是有意的: 每家都得显式回答这个问题。
    *
    * 拉不到就返回 `Left` 让启动失败 —— 账户状态没对上就开始交易, 比不启动危险得多。
    */
  def fetchPositions(): Either[ExchangeError, Vector[Position]]
