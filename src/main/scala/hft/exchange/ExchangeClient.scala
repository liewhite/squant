package hft.exchange

import hft.domain.*


/** 交易所客户端统一接口，仅封装 REST 交互。
  *
  * 所有方法同步阻塞 (运行在虚拟线程上)，错误以 Either 显式返回。
  */
trait ExchangeClient:
  def exchange: Exchange

  /** 拉取**某一品种**的全部合约规格。
    *
    * 按品种而不是"全部"：三家的规格端点本来就按品种分口
    * （Bybit 的 `category`、OKX 的 `instType`、Binance 的 USDⓈ-M 与期权是两套 API），
    * 一个不分品种的 `fetchAll` 只能定死在其中一种上 —— 从前它就定死在永续上，
    * 于是期权的规格根本无处可取。
    *
    * 本所不支持该品种时返回 `Left` 而不是空 —— "这个所没有期权"与"我没接期权"是两件事，
    * 空集把它们混成同一个读数。
    */
  def fetchMetas(kind: InstrumentKind): Either[ExchangeError, Vector[SymbolMeta]]

  /** 按标的索引的合约规格 —— 进程内**共享的一份**，可增量补充。
    *
    * 一个交易所的接入有三处要它：行情源把盘口数量从张换成币、汇报面把回报换回币、
    * 柜台把下单意图对齐到交易所精度。三处各拉一次就是同一事实的三份副本，
    * 启动时还白打两次 REST。所以它在客户端上，三处共用同一个实例。
    *
    * ## 键是标的而不是交易对
    *
    * 同一个 symbol 底下可能有 U 本位永续、币本位永续、几十个期权合约，而它们的
    * `contractSize` 完全不同（OKX 的 `ETH-USD-SWAP` 是 10 USD/张，`ETH-USDT-SWAP` 是
    * 0.1 ETH/张）。按 symbol 索引的话后写入的那条会静默覆盖前一条，之后所有张↔币换算
    * 都按错的乘数走 —— 数字看着都合理。
    *
    * ## 为什么可以增量补充
    *
    * 期权链每周滚动：新的到期日不断上市，旧的到期消失。一次性快照装不下"下周才有的合约"，
    * 而"一个合约一个策略实例"的用法要求运行中能装上新合约。[[loadMetas]] 因此是可以再调的。
    */
  private val metaCache: java.util.concurrent.atomic.AtomicReference[Map[Instrument, SymbolMeta]] =
    java.util.concurrent.atomic.AtomicReference(Map.empty)

  /** 已知的合约规格快照 (只读) */
  final def knownMetas: Map[Instrument, SymbolMeta] = metaCache.get()

  /** 这个标的的规格 —— **纯查表，缺失即抛**。
    *
    * 不在这里补拉：它在热路径上（每条订单回报都要拿它把张换回币），而补拉是一次网络往返。
    *
    * **谁需要规格，谁在自己的冷路径上先 [[loadMetas]]** —— 这条规则按"谁的事实"划分，
    * 不按"谁先跑"：把张数换回币本位是某些交易所自己的协议细节（OKX 的推送与 REST 都是
    * 张数，Binance/Bybit 则本就是币本位），所以由那个适配层的行情源与账户流在建立连接时
    * 保证；柜台则在启动对齐入口按交易标的补齐品种（见 `RestTradingGateway.syncSnapshot`）。
    *
    * 反过来写会出事：曾经让柜台代所有人加载，于是不经柜台的装配形态（实时看板、跨所价差
    * 监控都没有柜台）在第一条推送上就崩，或者把持仓静默读成零。
    */
  final def metaOf(instrument: Instrument): SymbolMeta =
    metaCache
      .get()
      .getOrElse(
        instrument,
        sys.error(s"$exchange 没有 $instrument 的合约规格 —— 该品种尚未加载 (见 ExchangeClient.loadMetas)"),
      )

  /** 拉一个品种的规格并**合并**进表；返回本次拉取到的条目数。
    *
    * **合并而不是整表替换**：规格是合约的静态事实，到期不会改变它。整表替换会在到期日
    * 当天把一个仍持有仓位的合约的规格抹掉，于是记账路径上的 `metaOf` 抛错、进程终止。
    * 表只增不减，进程重启即清空。
    *
    * 幂等：重复调用只是再合并一次同样的内容。并发调用经 CAS 合并，不会互相丢失。
    */
  final def loadMetas(kind: InstrumentKind): Either[ExchangeError, Int] =
    fetchMetas(kind).map { fetched =>
      val added = fetched.map(m => m.instrument -> m).toMap
      metaCache.updateAndGet(_ ++ added)
      added.size
    }

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

  /** 撤单。[[OrderRef]] 决定按交易所 id 还是按 clientOrderId 指名 —— 在途单只有后者。
    *
    * 收 [[Instrument]] 而不是 `Symbol`：撤哪一张单要先说清是哪个合约，而同一个 symbol
    * 底下可能有永续、币本位、几十个期权（见 [[InstrumentKind]]）。 */
  def cancelOrder(instrument: Instrument, ref: OrderRef): Either[ExchangeError, Unit]

  /** 查询当前挂单 (live + partially_filled) */
  def fetchPendingOrders(instrument: Instrument): Either[ExchangeError, Vector[OrderUpdate]]

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
