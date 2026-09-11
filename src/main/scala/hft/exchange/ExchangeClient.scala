package hft.exchange

import hft.domain.*
import org.slf4j.LoggerFactory

/** 交易所客户端统一接口，仅封装 REST 交互。
  *
  * 所有方法同步阻塞 (运行在虚拟线程上)，错误以 Either 显式返回。
  */
trait ExchangeClient:
  def exchange: Exchange

  /** 本适配层**接了哪些品种，以及各自的规格从哪拉** —— 一个所支持什么，是这一处说了算。
    *
    * ## 为什么是一个 Map，而不是"一个集合 + 一个按品种取的方法"
    *
    * 后者能写出"集合里加了期权、而取法忘了加那一支"的状态。那时 `ensureMetas(Option)`
    * 会拿一批**永续**规格去顶期权、还被 [[MetaTable]] 记成"已加载"，随后 `metaOf(期权)`
    * 报"尚未加载" —— 失败可见，但归因指向错误的地方。一个 Map 让这两件事不可能不一致。
    *
    * ## 为什么要有这个声明
    *
    * "本适配层只接 U 本位永续"从前以 `require(instrument.kind == LinearPerp)` 的形态散在
    * 六处（三家客户端的下单/撤单/挂单查询入口各一句、三家的 `fetchMetas` 各一句），四个
    * 行情源里还有三句同样的话 —— 同一个事实九份副本，措辞各不相同，而且**漏过一处**：
    * OKX 的行情源没有这道守卫，订一个期权盘口会订阅成功、然后在第一条推送上抛
    * "Unknown OKX instId"，错误指向报文而不是"这个品种没接"。
    *
    * ## 请求侧与响应侧必须同时通了才算"接了"
    *
    * 这不是"能不能拼出那个 instId"。OKX 的 `OkxCodec.toOkx` 早就能为期权和币本位拼出正确的
    * instId，但响应侧还没跟上（`fetchPositions` 固定 `instType=SWAP`、`fetchPendingOrders`
    * 与私有流经 `OkxCodec.instrumentOf` 只认 `base-quote-SWAP`）。只放开请求侧的话，一张
    * 期权单能成功发出去，然后**全程没有回报、没有挂单、没有仓位** —— 策略以为单没成立，
    * pending 清不掉，十几秒后以"结果不确定"终止，而真实原因是我们根本没在听那条频道。
    *
    * 所以一个品种进这个 Map 的条件是：规格端点、下单端点、以及**回报解析**三处都接通了。
    *
    * 按品种分口而不是一个"拉全部"：三家的规格端点本来就按品种分（Bybit 的 `category`、
    * OKX 的 `instType`、Binance 的 USDⓈ-M 与期权是两套 API），一个 `fetchAll` 只能定死在
    * 其中一种上 —— 从前它就定死在永续上，于是期权的规格根本无处可取。
    */
  protected def metaFetchers: Map[InstrumentKind, () => Either[ExchangeError, Vector[SymbolMeta]]]

  /** 本适配层接了哪些品种。由 [[metaFetchers]] 派生 —— 不是第二处声明。 */
  final def supportedKinds: Set[InstrumentKind] = metaFetchers.keySet

  /** 这个标的本适配层接不接 —— 接不了就**立即失败**，返回它自己好让调用点串起来。
    *
    * 不静默当永续处理：那会把一张期权单发到永续端点上，要么被交易所拒（白跑一趟），
    * 要么撞上一个同名的永续合约。品种是调用方明确写下的事实，对不上就是装配错了。
    */
  final def requireSupported(instrument: Instrument): Instrument =
    require(
      supportedKinds.contains(instrument.kind),
      s"$exchange 适配层未接入 ${instrument.kind} (已接: ${supportedKinds.mkString("/")}): $instrument",
    )
    instrument

  /** 拉取**某一品种**的全部合约规格。
    *
    * 没接的品种返回 `Left` 而不是空 —— "这个所没有期权"与"我没接期权"是两件事，
    * 空集把它们混成同一个读数。
    */
  final def fetchMetas(kind: InstrumentKind): Either[ExchangeError, Vector[SymbolMeta]] =
    metaFetchers.get(kind) match
      case Some(fetch) => fetch()
      case None =>
        Left(ExchangeError.Rejected("unsupported", s"$exchange 适配层未接入 $kind (已接: ${supportedKinds.mkString("/")})"))


  /** 本所的合约规格表 —— 按标的索引，可增量补充。见 [[MetaTable]]。
    *
    * 一个交易所的接入有四处要它：行情源把盘口数量从张换成币、汇报面把回报换回币、
    * 柜台把下单意图对齐到交易所精度、**以及客户端自己把 REST 响应里的张数换回币**。
    * 前三处各拉一次就是同一事实的三份副本，启动时还白打两次 REST。
    *
    * **状态在具体客户端上，不在这个 trait 上** —— 它从前是这里的一个 `private val`，
    * 那让装饰器 ([[DryRunClient]]) 必然自带一张接不上去的空表。理由见 [[MetaTable]]。
    *
    * 公开而不是 `protected`：装饰器要把它转发给 delegate（跨实例访问，`protected` 做不到），
    * 而实现方不限于本包 —— 接入一个新交易所不该要求把客户端写进 `hft.exchange` 里。
    * 调用方仍然走 [[metaOf]] / [[ensureMetas]]，那两个是 `final` 的。
    *
    * ## 键是标的而不是交易对
    *
    * 同一个 symbol 底下可能有 U 本位永续、币本位永续、几十个期权合约，而它们的
    * `contractSize` 完全不同（OKX 的 `ETH-USD-SWAP` 是 10 USD/张，`ETH-USDT-SWAP` 是
    * 0.1 ETH/张）。按 symbol 索引的话后写入的那条会静默覆盖前一条，之后所有张↔币换算
    * 都按错的乘数走 —— 数字看着都合理。
    */
  def metaTable: MetaTable

  /** 这个标的的规格 —— **纯查表，缺失即抛**。
    *
    * 不在这里补拉：它在热路径上（每条订单回报都要拿它把张换回币），而补拉是一次网络往返。
    *
    * **谁需要规格，谁在自己的冷路径上先 [[ensureMetas]]** —— 这条规则按"谁的事实"划分，
    * 不按"谁先跑"：把张数换回币本位是某些交易所自己的协议细节（OKX 的推送与 REST 都是
    * 张数，Binance/Bybit 则本就是币本位），所以由那个适配层的行情源、账户流**与客户端
    * 自己的 REST 响应侧**在各自的冷路径上保证；柜台则在启动对齐入口按交易标的补齐品种
    * （见 `RestTradingGateway.syncSnapshot`）。
    *
    * 两种写法都出过事：让柜台代所有人加载，于是不经柜台的装配形态（实时看板、跨所价差
    * 监控都没有柜台）在第一条推送上就崩；只让两条流负责、漏掉客户端自己的 REST 响应侧，
    * 于是同一个交易所建了两个客户端实例的看板形态下，账户轮询在第一轮就抛"尚未加载"。
    */
  final def metaOf(instrument: Instrument): SymbolMeta =
    metaTable
      .get(instrument)
      .getOrElse(
        sys.error(s"$exchange 没有 $instrument 的合约规格 —— 该品种尚未加载 (见 ExchangeClient.ensureMetas)"),
      )

  /** 这个品种的规格**必须在表里** —— 没拉过就拉一次，拉过就什么都不做。
    *
    * 需要规格的每一处在自己的冷路径上调它，幂等且不白打 REST。`requester` 只进日志：
    * 一次 REST 是谁触发的，出问题时是要查的东西。
    *
    * **返回 `Either` 而不是抛**：规格端点的超时、限频、错误码都是本接口契约里明说会发生的
    * 失败 (见类文档"错误以 Either 显式返回")，它们必须和调用方自己那次请求的失败走同一条
    * 通道。客户端的 REST 响应侧就在 `Either` 契约里 (`fetchPositions` 每秒被账户轮询调一次)，
    * 让它半路抛出去，等于同一个方法对同一类失败有两个出口。
    *
    * 建立连接那种"失败即不该继续"的调用点用 [[ensureMetasOrThrow]]。
    */
  final def ensureMetas(kind: InstrumentKind, requester: String): Either[ExchangeError, Unit] =
    metaTable.ensure(kind)(fetchMetas(kind)).map {
      case Some(n) => ExchangeClient.logger.info(s"$requester 加载 $exchange $kind 合约规格 $n 条")
      case None    => ()
    }

  /** [[ensureMetas]] 的抛出形态 —— 给建立连接、启动对齐这类**带伤继续毫无意义**的调用点。
    *
    * 收在这里而不是让三个调用点各自 `fold`：错误文案是同一句话，各写一遍迟早各不相同。
    */
  final def ensureMetasOrThrow(kind: InstrumentKind, requester: String): Unit =
    ensureMetas(kind, requester) match
      case Right(())=> ()
      case Left(e)  => throw IllegalStateException(s"$requester 加载 $exchange $kind 合约规格失败: ${e.message}")

object ExchangeClient:
  /** 规格加载的日志出口。放伴生对象而不是 trait 字段: 后者每个客户端实例一份。 */
  private val logger = LoggerFactory.getLogger(classOf[ExchangeClient])

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
