package hft.domain

import java.util.UUID

/** 交易所枚举 */
enum Exchange:
  case Binance
  case Okx
  case Bybit
  case Hyperliquid

  /** 生成交易所合法的 client_order_id */
  def newClientOrderId: String =
    val hex = UUID.randomUUID().toString.replace("-", "")
    this match
      case Binance     => s"0x$hex" // 34 字符, Binance 上限 36
      case Okx         => hex // 32 字符纯字母数字, OKX clOrdId 上限 32
      case Bybit       => hex // 32 字符, Bybit orderLinkId 上限 36
      case Hyperliquid => s"0x$hex" // cloid 是 128 位十六进制, 必须是 "0x" + 32 个 hex 字符

/** 账户身份。
  *
  * 同一份策略逻辑可以同时跑在实盘与若干模拟账户上 —— 它们看同一份行情、下同样的单，
  * 只有账户不同。因此账户**不是**策略的属性，而是装配期绑定的：策略自己不知道、
  * 也不该知道它跑在哪个账户上，否则同一份逻辑就没法既做实盘又做影子盘。
  *
  * 账户归属是**必填**的结构字段，不靠"来源即实盘"这类推断。参考实现在这里栽过：
  * 用一张手工维护的分类表判断私有事件归属，新增一个变体漏改一行就会把实盘私有事件
  * 广播给模拟策略 —— 危险侧、且编译器不报。
  */
enum AccountId:
  /** 真实交易所账户 */
  case Live
  /** 本地模拟账户 (影子盘)。同一进程可以有多个 */
  case Paper(id: Int)

  override def toString: String = this match
    case Live     => "live"
    case Paper(n) => s"paper$n"

/** 标的 = (交易所, 交易对) —— 公共行情与持仓/订单/成交的路由键。
  *
  * 独立类型而非 `(Exchange, Symbol)` 元组：它是事件路由的键，会进哈希表、进日志、
  * 进订阅声明，具名类型让这些地方读起来是"标的"而不是"某个二元组"。
  */
final case class Instrument(exchange: Exchange, symbol: Symbol):
  override def toString: String = s"$exchange:$symbol"

/** 某账户在某标的上的口子 —— **私有回报**的路由键。
  *
  * 行情按 [[Instrument]] 路由 (一份服务所有账户)，私有回报按本类型路由。于是实盘策略与
  * 影子策略即便交易同一标的，也从投递层就收不到对方的成交与订单回报 —— 订单归属由路由
  * 保证，不需要在消费侧再判一次"这笔是不是我的"。
  */
final case class AccountInstrument(account: AccountId, instrument: Instrument):
  override def toString: String = s"$account@$instrument"

/** 某账户在某交易所的口子 —— **账户级读数** (余额/净值/希腊值) 的路由键 */
final case class AccountExchange(account: AccountId, exchange: Exchange):
  override def toString: String = s"$account@$exchange"

/** 撤单时如何指名一张订单。
  *
  * 两种指名方式不是等价的备选，而对应订单生命周期的两个阶段：交易所确认之前，本地只有
  * 自己生成的 clientOrderId；确认之后才有交易所 id。撤下一个策略时它可能正好有单在途，
  * 只认交易所 id 就撤不掉那张单 —— 而策略已经停了，没有"下一次收尾"来兜底，
  * 那张 GTC 单会留在交易所无人跟踪。
  */
enum OrderRef:
  /** 交易所分配的 id */
  case ByExchangeId(id: OrderId)
  /** 我们下单时自己生成的 id (Binance origClientOrderId / OKX clOrdId / Bybit orderLinkId) */
  case ByClientId(id: String)

  def raw: String = this match
    case ByExchangeId(id) => id
    case ByClientId(id)   => id

/** 交易方向 */
enum Side:
  case Long, Short

  def opposite: Side = this match
    case Long  => Short
    case Short => Long

/** 限价单有效方式 */
enum TimeInForce:
  case GTC, IOC, FOK, PostOnly

/** 订单类型 */
enum OrderType:
  case Market
  case Limit(price: Price, tif: TimeInForce)

/** 订单状态 */
enum OrderStatus:
  /** 本地已创建，等待交易所响应 */
  case Created
  /** 交易所已收到，等待成交 */
  case Pending
  case PartiallyFilled(filled: Coin)
  case Filled
  case Cancelled
  case Rejected(reason: String)
  /** 下单请求发送失败 (REST API 错误) */
  case Error(reason: String)

  /** 是否为终态 (订单生命周期结束) */
  def isTerminal: Boolean = this match
    case Filled | Cancelled | Rejected(_) | Error(_) => true
    case _                                           => false

  /** 是否已被交易所确认 (非 Created 状态) */
  def isConfirmed: Boolean = this != Created

/** 订单。quantity 统一为币本位数量，发送交易所前由 SymbolMeta 转换 */
final case class Order(
    id: OrderId,
    exchange: Exchange,
    symbol: Symbol,
    side: Side,
    orderType: OrderType,
    quantity: Coin,
    reduceOnly: Boolean,
    clientOrderId: String,
    /** **策略给这张单的标注** —— "这是我的哪一张单"。空串 = 未标注。
      *
      * 存在的理由是策略认不出自己的回报：`clientOrderId` 由框架在处理器**返回之后**才生成
      * (见 [[hft.engine.StrategyRunner]])，策略手里那个是空串，拿它去匹配回报一条都匹配不上。
      * 于是每个策略只能自己发明一套按属性认单的办法 —— 按 `reduceOnly` 分槽、按
      * (交易所, 标的) 认领、或者干脆立一条"同一时刻只有一张在途单"的不变量。三种写法都能用，
      * 但都把"同一标的上同时挂多张可区分的单"排除在了能写出来的东西之外。
      *
      * 标注由策略自己定义，框架只负责**原样带住**：进挂单登记 ([[hft.state.PendingOrder]])，
      * 并在该单的回报到达时经 [[hft.strategy.StrategyContext.orderTag]] 交还。
      *
      * **不出本地**：它不进 `ExchangeOrder`、不发给交易所 (见 [[OrderConversion]])。因此
      * 重启后从交易所接管的既有挂单没有标注 —— 那是上一个进程的内存里的事实。策略若按标注
      * 分派，必须能处理"没有标注"这一支 (它本来就要处理，接管单一直是这样)。
      *
      * **不保证唯一**：一批单标同一个 tag 是合法用法 (例如整组网格单标 `grid`)。要求
      * "一个标注对应一张单"的策略自己保证，框架不校验 —— 校验会把上面那种用法一并禁掉。
      */
    tag: String = "",
)

/** 订单更新事件 */
final case class OrderUpdate(
    account: AccountId,
    orderId: OrderId,
    clientOrderId: Option[String],
    exchange: Exchange,
    symbol: Symbol,
    side: Side,
    status: OrderStatus,
    /** 订单价格 (限价单) */
    price: Price,
    /** 订单总数量 (币本位) */
    quantity: Coin,
    /** 累计成交量 */
    filledQuantity: Coin,
    /** 是否只减仓。
      *
      * **没有默认值是有意的**：策略拿它给 resting 单分槽 (止盈槽 vs 加仓槽)。从前对齐时接管的
      * 挂单在本地被伪造成 `reduceOnly = false` —— 重启后真正的止盈单被归进加仓槽，策略以为
      * 没有止盈单、于是再挂一张，而那张真的止盈单还在簿上。
      * 三家交易所的挂单查询与订单推送都返回这个字段，它不是"填不出来"的事实，是没去读。 */
    reduceOnly: Boolean,
    timestamp: Timestamp,
)

/** 公共成交印记 (市场上的匿名成交，非本账户成交)。
  * isBuyerMaker=true 表示买方是挂单方 -> 本笔为主动卖出 (taker 卖)。仅作策略可见的市场信号，不参与撮合。
  */
final case class MarketTrade(
    exchange: Exchange,
    symbol: Symbol,
    price: Price,
    qty: Coin,
    isBuyerMaker: Boolean,
    timestamp: Timestamp,
)

/** 一笔成交的明细。仓位不由它维护 —— 那是柜台账本的事 */
final case class Fill(
    account: AccountId,
    exchange: Exchange,
    symbol: Symbol,
    side: Side,
    price: Price,
    size: Coin,
    timestamp: Timestamp,
)

/** 仓位 —— **只有数量**，size 为正表示多头，为负表示空头。
  *
  * ## 为什么没有均价与未实现盈亏
  *
  * 这是总线上的**仓位快照**，要在所有交易所、真假柜台、回测四种形态下含义一致。
  * 而那两个数不是每家都给得出：
  *
  *   - **未实现盈亏**要估值价，真实柜台不订阅行情、算不了 —— 从前它由
  *     `TradingGateway.positionEvent` 一律强制归零，等于一个恒为 0 的字段占着位置。
  *   - **持仓均价**各家口径不一 (有的空仓时给空串, 有的干脆不给)，读不到就填 0；
  *     而 0 均价一旦流进盈亏计算，就是一笔凭空的巨额假亏损，且没有任何报错。
  *
  * **一个"有的交易所填不出、于是填 0"的字段，比没有这个字段危险得多** —— 缺字段是
  * 编译期就要面对的事，填 0 是运行期悄悄算错的事。所以按最小可用抽象来：只留数量。
  *
  * 均价确实需要的地方是**账本内部**的已实现盈亏 —— 那是本地撮合自己算出来的
  * (见 [[Ledger.Holding]])，永远有值，与交易所给不给无关。两者分开之后，
  * "这个均价是谁算的"不再含糊。
  *
  * 要盈亏读 [[AccountInfo]] 的净值，那是柜台确实算得出的。
  */
final case class Position(
    account: AccountId,
    exchange: Exchange,
    symbol: Symbol,
    size: Coin,
):
  /** 判断是否空仓 (epsilon 比较避免浮点精度问题) */
  def isEmpty: Boolean = size.isZero

  /** 持仓方向，空仓返回 None */
  def side: Option[Side] =
    if isEmpty then None
    else if size > Coin.Zero then Some(Side.Long)
    else Some(Side.Short)

object Position:
  val Epsilon: Double = 1e-10

  def empty(account: AccountId, exchange: Exchange, symbol: Symbol): Position =
    Position(account, exchange, symbol, Coin.Zero)

/** 资产余额 */
final case class Balance(
    account: AccountId,
    exchange: Exchange,
    asset: String,
    available: Double,
    timestamp: Timestamp,
)

/** 一份**完整**的钱包快照 —— 未列出的币种余额就是 0。
  *
  * ## 为什么需要"完整"这个事实
  *
  * 单条 [[Balance]] 只说"这个币种现在有多少"，说不出"别的币种是 0"。而 OKX 在余额为 0 时
  * **不下发该币种行**，于是"从没见过 ETH 的余额"与"ETH 余额确实是 0"在下游完全无从分辨。
  *
  * 这个区分不是学术问题：`StateManager.greeks` 要 `期权 delta + 该币现金余额` 才能给出总敞口，
  * 缺余额就返回 None、对冲静默不触发 —— 一个只卖期权、不持现货的账户会因此**永远裸着敞口**。
  * 从前的做法是让一个 feed 在启动时注入一条假的 `BalanceChanged(ccy, 0.0)` 把键"占上"，
  * 那等于用一句谎话换来对冲能跑：真实余额非零的账户在快照到达前会按 0 对冲。
  *
  * ## "完整"只能来自 REST
  *
  * 三家的私有钱包 WS 通道**都给不出这个事实**，各自的原文如下：
  *   - OKX `account` 频道：只有 initial/regular snapshot 是全量，"when there is change in balance
  *     or equity of an token, **only the incremental data of that currency will be pushed**"，
  *     且快照可能按 `curPage`/`lastPage` 分页；
  *   - Bybit `wallet` 频道："**There is no snapshot event given at the time when the subscription
  *     is successful**"，且从未声明 `coin[]` 覆盖全部币种；
  *   - Binance `ACCOUNT_UPDATE.B`：只带变化项。
  *
  * 因此这份全量由**启动对齐时的一次 REST 钱包查询**建立 (`TradingClient.fetchWallet` ->
  * `TradingGateway.currentWallet`)，之后由逐币种的 [[Balance]] 维持 —— 三家的 WS 推送里
  * 每个币种的值都是**当前余额**而非变化量，只是不覆盖未变动的币种，所以增量维护是正确的。
  *
  * 反过来把 WS 推送当全量做整表替换，代价是：一次只有 USDT 变动的推送会把 ETH 现货抹成 0，
  * 而 delta 对冲正拿这个数当敞口。Bybit 侧没有周期性全量推送，这个错误不会自愈。
  */
final case class Wallet(
    account: AccountId,
    exchange: Exchange,
    /** 币种 -> 余额。**未列出即为 0** */
    balances: Map[String, Double],
    timestamp: Timestamp,
)

/** Best Bid Offer (L1 行情) */
final case class BBO(
    exchange: Exchange,
    symbol: Symbol,
    bidPrice: Price,
    bidQty: Coin,
    askPrice: Price,
    askQty: Coin,
    timestamp: Timestamp,
):
  def spread: Price = askPrice - bidPrice
  def midPrice: Price = (bidPrice + askPrice) / 2.0

/** 资金费率 */
final case class FundingRate(
    exchange: Exchange,
    symbol: Symbol,
    rate: Rate,
    nextSettleTime: Timestamp,
    /** 数据时间戳，用于计算基于剩余时间的日化费率 */
    timestamp: Timestamp,
):
  /** 基于剩余时间的日化费率: rate * 24 / hoursToSettle (最小 1 小时防止结算临近时爆炸) */
  def dailyRate: Rate = dailyRateWithBaseTime(nextSettleTime, timestamp)

  /** 基于指定时间基准的日化费率。
    *
    * 它公开的理由曾是"跨交易所用统一基准公平比较"，而那个消费者
    * (`SymbolView.bestShort/bestLongExchange`) 已随无人使用一并删除。现在只剩
    * [[dailyRate]] 一个调用方，故收成私有 —— 一个没有消费者的公开参数化入口，
    * 只会让读者以为存在"另一种基准"的用法。 */
  private def dailyRateWithBaseTime(baseSettleTime: Timestamp, currentTime: Timestamp): Rate =
    if currentTime >= baseSettleTime then 0.0
    else
      val hoursToSettle = (baseSettleTime - currentTime).toDouble / (1000.0 * 60 * 60)
      rate * (24.0 / math.max(hoursToSettle, 1.0))

/** 标记价格 */
final case class MarkPrice(
    exchange: Exchange,
    symbol: Symbol,
    price: Price,
    timestamp: Timestamp,
)

/** 指数价格 */
final case class IndexPrice(
    exchange: Exchange,
    symbol: Symbol,
    price: Price,
    timestamp: Timestamp,
)

/** 账户级期权希腊字母 (按币种 ccy 聚合)。
  *
  * 这是该币种**所有期权持仓的净希腊字母**，而非单个合约——OKX `account/greeks` 直接返回此聚合值，
  * 回测的 BS 合成源亦将单合约希腊字母按持仓聚合为同一形态，使策略对实盘/回测无感。
  *
  * delta 为**原始期权 delta**；总敞口需叠加现货/合约 delta，见 `hft.state.StateManager.greeks`
  * 用 cashBal 做的修正。delta>0 表示该币种看多敞口。
  *
  * **通道规范单位 (SSOT)**：所有来源 (OKX 轮询 / BS 合成) 必须统一为——
  *   - delta/gamma: 币本位 (dPrice/dS、d²Price/dS²)
  *   - theta: **每日** 时间衰减
  *   - vega : 对 **1% (0.01)** 波动率变动的敏感度
  * 以此保证策略数值读 theta/vega 时实盘与回测一致。(OKX deltaBS/thetaBS/vegaBS 视为已遵循此约定;
  * BS 合成源在 [[hft.backtest.BsGreeksSource]] 内把数学约定的每年 theta、对 1.0 vega 换算到此约定。)
  */
final case class Greeks(
    account: AccountId,
    exchange: Exchange,
    /** 币种, e.g. "BTC" / "ETH" (非 symbol "BTCUSDT") */
    ccy: String,
    delta: Double,
    gamma: Double,
    theta: Double,
    vega: Double,
    timestamp: Timestamp,
)

/** 账户信息 (净值 + 总持仓名义价值，原子读取) */
/** 账户读数 —— **只有净值**。
  *
  * 从前还有一个"总持仓名义价值": Binance 的 REST 给真值, OKX 与 Bybit 的 REST 都填 0,
  * 而 OKX 的私有流给真值 —— 于是那个真值会被柜台每 10 秒一次的 REST 刷回 0。三种口径,
  * 一个字段, 且主代码里**没有任何人读它**。
  *
  * 同 [[Position]] 的均价: 填不出就填 0 的字段比没有这个字段危险。要杠杆率就由读得到
  * 持仓的一方自己算 (账本有 [[Ledger.notional]], 那是本地算得出的)。
  */
final case class AccountInfo(
    account: AccountId,
    exchange: Exchange,
    /** 账户净值 (balance + unrealizedPnl) */
    equity: Double,
)

/** 交易对元数据 (精度、合约乘数) */
final case class SymbolMeta(
    exchange: Exchange,
    symbol: Symbol,
    /** 价格最小变动单位 */
    tickSize: Double,
    /** 数量最小变动单位 */
    sizeStep: Double,
    /** 最小下单数量 —— 与 [[sizeStep]] **同域**：交易所原生数量单位（张）。
      *
      * OKX 的 `minSz`/`lotSz` 本就是张数；Binance/Bybit 的 `contractSize = 1`，币与张数值相等，
      * 看不出区别。所以判定必须在张数域做（见 [[meetsMinOrderSize]]），
      * 拿币本位数量直接和它比，在 OKX 上会差整整一个 contractSize。 */
    minOrderSize: Double,
    /** 合约乘数: 每张合约对应的币本位数量 (Binance 为 1.0) */
    contractSize: Double,
):
  def isValid: Boolean = tickSize > 0 && sizeStep > 0 && contractSize > 0

  /** 币本位数量 -> 下单数量 (张) */
  /** 币本位 -> 合约张数（裸浮点）。用于**解析与展示** —— 那里的值本身就是近似的，
    * 且行情解析是热路径。要发往交易所的数量请用 [[toExchangeContracts]]，它精确。 */
  def toContracts(amount: Coin): Contracts = Contracts(amount.value / contractSize)

  /** 精确的张数（BigDecimal 域）—— 取整与发单都靠它。
    *
    * 裸浮点除法会失真到**整整一档**：`0.3 / 0.1 = 2.9999999999999996`，FLOOR 之后是 2 而不是 3。
    * Scala 的 `BigDecimal(Double)` 经 `Double.toString` 构造，故 `BigDecimal(0.3)` 是精确的 0.3。
    */
  private def exactContracts(amount: Coin): BigDecimal =
    BigDecimal(amount.value) / BigDecimal(contractSize)

  /** 下单数量 (张) -> 币本位数量 */
  /** 合约张数 -> 币本位。**回报解析时用**：进了框架的数量一律币本位 */
  def toCoin(qty: Contracts): Coin = Coin(qty.value * contractSize)

  /** 价格取整到合法精度 (四舍五入到 tickSize) */
  def roundPrice(price: Price): Price =
    Price(SymbolMeta.roundToStep(price.value, tickSize, BigDecimal.RoundingMode.HALF_UP))

  /** 数量取整到合法精度（就近）。理由同 [[roundCoin]] —— 这已是发往交易所的最后一步，
    * 再 FLOOR 一次会把 [[toExchangeContracts]] 刚对齐好的数量又削掉一档。 */
  def roundSize(size: Contracts): Contracts =
    Contracts(SymbolMeta.roundToStep(size.value, sizeStep, BigDecimal.RoundingMode.HALF_UP))

  /** 已对齐过的币本位数量 -> 发往交易所的张数。
    *
    * **不能用裸 double 除法**：`x * contractSize / contractSize ≠ x`。实测
    * `0.07 / 0.01 = 7.000000000000001`、`0.3 / 0.1 = 2.9999999999999996`，
    * 而各 client 的格式化只做 `stripTrailingZeros`、不再取整 —— 那串尾巴会被交易所按
    * lot size 判为非法数量而拒单。
    *
    * 用 `HALF_UP` 而不是 `FLOOR`：尾差可能偏大也可能偏小（见上面第二个例子），
    * FLOOR 会把 `2.9999999999999996` 砍成 2，整整少一档。
    */
  def toExchangeContracts(aligned: Coin): Contracts =
    val step = BigDecimal(sizeStep)
    Contracts(((exactContracts(aligned) / step).setScale(0, BigDecimal.RoundingMode.HALF_UP) * step).toDouble)

  /** 把币本位数量对齐到交易所能接受的精度，**结果仍是币本位**。
    *
    * 换算 -> 取整 -> 换回来。这样"按交易所精度取整"这件事不必把张数泄漏进框架：
    * 策略与撮合看到的始终是币，只是它是一个交易所收得下的币数。
    *
    * ## 为什么是就近取整而不是向下
    *
    * 交易所是**十进制记账**的：1000 笔 0.1 的成交，它那边持仓精确等于 100。而我们的账本是
    * 浮点求和，会漂成 `99.99999999999860`。这个差不是"我们比交易所少 1.4e-12"，是**我们算错了**
    * —— 对齐到 step 网格恰恰是把真值恢复回来，取整后的数字才是那个真实的决策。
    *
    * 向下取整会把这个漂移**放大成整整一档**：`99.99999999999860` FLOOR 到 `99.999`，
    * 平仓就留下 `0.000999` 的残仓 —— 它比 [[Position.Epsilon]] 大七个数量级（判不出"已归零"），
    * 又小于最小下单量（再也发不出单），于是永远平不掉、[[hft.perf.Supervisor]] 永远告警。
    * BigDecimal 只能保证除法不引入新误差，救不了本身就带累加误差的输入；
    * 取整方向才是这里的决定因素。
    *
    * 多取一档的代价则小得多：reduceOnly 由撮合层与交易所双重截断（见
    * [[hft.sim.SimState.onOrderArrived]]），开仓方向也只是多一个 step 的敞口。
    */
  def roundCoin(amount: Coin): Coin =
    val step = BigDecimal(sizeStep)
    val aligned = (exactContracts(amount) / step).setScale(0, BigDecimal.RoundingMode.HALF_UP) * step
    Coin((aligned * BigDecimal(contractSize)).toDouble)

  /** 这个币本位数量对齐后交易所收不收 —— 在**张数域**判定，与 [[minOrderSize]] 同域。
    *
    * 数量为 0 也归入此类：从前低于一档的量被向下取整成 0 之后照样发出去，
    * 换来一个交易所的拒单和一次白跑的往返。
    */
  def meetsMinOrderSize(amount: Coin): Boolean =
    val contracts = toExchangeContracts(amount).value
    contracts > 0.0 && contracts >= minOrderSize

  /** 格式化价格为 API 请求字符串 */
  def formatPrice(price: Price): String =
    BigDecimal(roundPrice(price).value).underlying.stripTrailingZeros.toPlainString

  /** 格式化数量为 API 请求字符串 —— 参数是**张数**，这是发往交易所的最后一步 */
  def formatSize(size: Contracts): String =
    BigDecimal(roundSize(size).value).underlying.stripTrailingZeros.toPlainString

object SymbolMeta:
  /** 用 BigDecimal 精确计算，按 step 取整 */
  private def roundToStep(value: Double, step: Double, mode: BigDecimal.RoundingMode.Value): Double =
    if step <= 0 then value
    else
      val ticks = BigDecimal(value) / BigDecimal(step)
      (ticks.setScale(0, mode) * BigDecimal(step)).toDouble
