package hft.dashboard

import hft.domain.*
import hft.event.{AnyEvent, Topic, Topics}

/** 一条读数 + 它是**什么时候**到本地的。
  *
  * 看板上每个数字都必须带着它的年龄一起出现。一个 4 分钟前的盘口和一个刚到的盘口在页面上
  * 长得一模一样, 就是这轮审查反复在修的那类问题的展示层版本: **"不知道"被显示成一个正常值**。
  * 陈旧不是异常路径, 是常态 (行情断线、某个标的当天没有成交), 所以它是这个类型的一部分,
  * 而不是某个分支里的告警。
  *
  * `at` 取**本地接收时刻** (`Event.localTs`) 而不是交易所时间戳: 年龄要拿本地墙钟去减,
  * 两边必须同一个钟, 否则减出来的是"陈旧度 + 两地时钟偏斜"。同一条判据见
  * [[hft.state.StateManager.greeksAt]]。
  */
final case class Stamped[+A](value: A, at: Timestamp):
  def ageMs(now: Timestamp): Long = now - at

/** 某个账户在某个标的上的情况 —— 仓位、在场挂单、最近一笔成交。 */
final case class AccountBoard(
    position: Option[Stamped[Coin]],
    /** 尚未终态的挂单, 按 orderId 索引。 */
    pendingOrders: Map[OrderId, Stamped[OrderUpdate]],
    lastFill: Option[Stamped[Fill]],
)

object AccountBoard:
  val empty: AccountBoard = AccountBoard(None, Map.empty, None)

/** 一个标的 (交易所 + 交易对) 的全部情况：公共行情 + 各账户的私有情况。 */
final case class SymbolBoard(
    instrument: Instrument,
    bbo: Option[Stamped[BBO]],
    mark: Option[Stamped[MarkPrice]],
    funding: Option[Stamped[FundingRate]],
    lastTrade: Option[Stamped[MarketTrade]],
    accounts: Map[AccountId, AccountBoard],
)

object SymbolBoard:
  def empty(instrument: Instrument): SymbolBoard =
    SymbolBoard(instrument, None, None, None, None, Map.empty)

/** 账户级读数 (不按标的分)：净值与钱包。 */
final case class AccountSummary(
    equity: Option[Stamped[Double]],
    /** 余额本身的规则 (全量整表替换 / 逐币种增量 / 全量之前不能把缺失读作 0) 归
      * [[WalletState]] —— **判据只写一遍**, `hft.state.StateManager` 用的是同一个。 */
    wallet: WalletState,
    /** 各币种余额**各自**的到达时刻。[[WalletState]] 只管值, 时刻是看板自己要的东西 ——
      * 让它进领域类型, 就等于让"页面上要显示年龄"这件事污染一个与显示无关的规则。 */
    balanceAt: Map[String, Timestamp],
):
  def withWallet(snapshot: Map[String, Double], at: Timestamp): AccountSummary =
    // 整表替换: 时刻表也一并换掉, 否则会留下已经不存在的币种的时刻。
    copy(wallet = wallet.withWallet(snapshot), balanceAt = snapshot.keys.map(_ -> at).toMap)

  def withBalance(currency: String, amount: Double, at: Timestamp): AccountSummary =
    copy(wallet = wallet.withBalance(currency, amount), balanceAt = balanceAt.updated(currency, at))

  /** 币种 -> (余额, 到达时刻)。 */
  def stampedBalances: Map[String, Stamped[Double]] =
    wallet.balances.map((ccy, amount) => ccy -> Stamped(amount, balanceAt.getOrElse(ccy, 0L)))

object AccountSummary:
  val empty: AccountSummary = AccountSummary(None, WalletState.empty, Map.empty)

/** 看板的**全部状态**, 不可变。
  *
  * ## 它是一份观察者的视图, 不是任何人的决策依据
  *
  * 框架里已经有一份权威状态 ([[hft.state.StateManager]]), 那份归策略执行器所有、只在它的
  * actor 线程上读写。看板**不去读它** —— 那是跨线程读一份可变状态, 是数据竞争; 而且
  * StateManager 是按策略实例建的, 一个进程里有几个策略就有几份, 它们各自只看得见自己订阅的
  * 那些标的。
  *
  * 所以看板走总线: 自己订阅、自己折叠出一份不可变快照。代价是这里有第二份"从事件重建状态"
  * 的代码, 收益是**看板看得见全进程的全部标的与全部账户**, 而且与交易路径完全隔离 ——
  * 它多算错算, 不会有一分钱因此下错单。
  *
  * 为了让这份代价可控, 凡是**判断**都复用框架那一份, 不在这里重写:
  *   - 挂单在不在场: [[OrderStatus.isTerminal]];
  *   - 钱包全量 / 逐币种增量的分工: 见 [[hft.domain.Wallet]]。
  *
  * ## 折叠是纯函数
  *
  * [[apply]] 是 `(快照, 事件) => 快照`, 不读墙钟 (时刻取自事件的 `localTs`)、不做 IO。
  * 于是它能脱离线程、HTTP、引擎单测, 而线程安全由调用方用一次引用替换保证
  * (见 [[DashboardActor]])。
  */
final case class BoardSnapshot(
    symbols: Map[Instrument, SymbolBoard],
    accounts: Map[AccountExchange, AccountSummary],
    /** 已折叠的事件条数 —— 用来分辨"没有数据"与"有数据但都是空的"。 */
    eventsApplied: Long,
    /** 最后一条事件的本地时刻。看板整体的"心跳"。 */
    lastEventAt: Option[Timestamp],
    /** 每家交易所最后一条事件的本地时刻 —— **那家的心跳**。
      *
      * 这是"这家的数据流还活着吗"唯一靠得住的读数, 而单个标的的报价年龄不是:
      * 一个标的在某家所几分钟不报价, 可能只是**市场安静** (股票永续在美股闭市时段几乎不动),
      * 也可能是那家的流卡住了 —— 单看它自己分不出来。跨所有别的标的还在报, 就说明流是活的。
      *
      * (连接彻底断掉是另一回事: WsLoop 有 5 分钟空闲看门狗, 那种情况进程直接死, 看板也就没了。) */
    lastEventByExchange: Map[Exchange, Timestamp],
):
  /** 折叠一条事件。**必须是已声明的 topic** —— 判据见 [[BoardSnapshot.folds]]。 */
  def apply(event: AnyEvent): BoardSnapshot =
    val at = event.localTs
    val fold = BoardSnapshot.folds.getOrElse(
      event.topic,
      // 走到这里只可能是"订阅了却没有折叠规则", 而 folds 是这两件事的唯一来源 ——
      // 除非有人绕过 DashboardActor.interests 直接投递。不静默放行: 那样的失效形态是
      // 事件被计数、心跳在跳、页面上那一列永远是空的。
      throw IllegalStateException(s"看板收到未声明的 topic ${event.topic.name} (key=${event.key})"),
    )
    val next = fold(this, event, at)
    next.copy(
      eventsApplied = eventsApplied + 1,
      lastEventAt = Some(at),
      lastEventByExchange = BoardSnapshot
        .exchangeOf(event)
        .fold(next.lastEventByExchange)(ex => next.lastEventByExchange.updated(ex, at)),
    )

  private def summary(account: AccountId, exchange: Exchange): AccountSummary =
    accounts.getOrElse(AccountExchange(account, exchange), AccountSummary.empty)

  private def onSymbol(key: Instrument)(f: SymbolBoard => SymbolBoard): BoardSnapshot =
    copy(symbols = symbols.updated(key, f(symbols.getOrElse(key, SymbolBoard.empty(key)))))

  private def onAccount(key: Instrument, account: AccountId)(
      f: AccountBoard => AccountBoard
  ): BoardSnapshot =
    onSymbol(key) { sb =>
      sb.copy(accounts = sb.accounts.updated(account, f(sb.accounts.getOrElse(account, AccountBoard.empty))))
    }

  private def onSummary(account: AccountId, exchange: Exchange)(f: AccountSummary => AccountSummary): BoardSnapshot =
    val key = AccountExchange(account, exchange)
    copy(accounts = accounts.updated(key, f(accounts.getOrElse(key, AccountSummary.empty))))

object BoardSnapshot:
  val empty: BoardSnapshot = BoardSnapshot(Map.empty, Map.empty, 0L, None, Map.empty)

  /** 这条事件来自哪家交易所。看板订阅的每个 topic 的载荷都带交易所, 所以这里不该有 None ——
    * 留 Option 只是因为签名上给不出保证; 真出现就说明订阅表里混进了不带交易所的 topic。 */
  private def exchangeOf(event: AnyEvent): Option[Exchange] =
    event.key match
      case i: Instrument       => Some(i.exchange)
      case a: AccountInstrument => Some(a.instrument.exchange)
      case a: AccountExchange  => Some(a.exchange)
      case _                   => None

  /** 一个 topic 的折叠规则。 */
  private final class Fold[P](val topic: Topic[?, P], f: (BoardSnapshot, P, Timestamp) => BoardSnapshot):
    def apply(s: BoardSnapshot, event: AnyEvent, at: Timestamp): BoardSnapshot =
      // 能取到 payload 是因为调用方按 event.topic 选中的就是这条规则。
      f(s, event.as(topic).getOrElse(sys.error(s"topic ${topic.name} 的载荷类型与折叠规则不符")), at)

  /** **看板订阅什么、以及每条怎么折叠 —— 只有这一张表。**
    *
    * 从前这是两份列表 (`DashboardActor.interests` 一份、`fold` 的分支一份), 而它们会错开:
    * 往订阅里加一个 topic 却忘了加折叠分支, 事件被计数、心跳在跳、页面上那一列永远是空的,
    * 没有任何症状。同一事实两处写, 两处必然会错开 —— 所以订阅从这张表派生 (见 [[topics]]),
    * 加一个 topic 就必须同时给出它的折叠规则, 否则编译不过。
    */
  private val folds: Map[Topic[?, ?], Fold[?]] =
    Vector[Fold[?]](
      Fold(Topics.Bbo, (s, p, at) => s.onSymbol(p.instrument)(_.copy(bbo = Some(Stamped(p, at))))),
      Fold(Topics.MarkPrice, (s, p, at) => s.onSymbol(p.instrument)(_.copy(mark = Some(Stamped(p, at))))),
      Fold(Topics.FundingRate, (s, p, at) => s.onSymbol(p.instrument)(_.copy(funding = Some(Stamped(p, at))))),
      Fold(Topics.Trade, (s, p, at) => s.onSymbol(p.instrument)(_.copy(lastTrade = Some(Stamped(p, at))))),
      Fold(Topics.Position, (s, p, at) => s.onAccount(p.instrument, p.account)(_.copy(position = Some(Stamped(p.size, at))))),
      Fold(Topics.Fill, (s, p, at) => s.onAccount(p.instrument, p.account)(_.copy(lastFill = Some(Stamped(p, at))))),
      Fold(
        Topics.OrderUpdate,
        (s, p, at) =>
          s.onAccount(p.instrument, p.account) { acc =>
            // 在不在场只有一条判据 —— 框架的 OrderStatus.isTerminal。这里若自己列一遍终态,
            // 迟早会与它错开一个状态, 而失效形态是看板上挂着一张早就没了的单。
            if p.status.isTerminal then acc.copy(pendingOrders = acc.pendingOrders - p.orderId)
            else acc.copy(pendingOrders = acc.pendingOrders.updated(p.orderId, Stamped(p, at)))
          },
      ),
      Fold(Topics.AccountInfo, (s, p, at) => s.onSummary(p.account, p.exchange)(_.copy(equity = Some(Stamped(p.equity, at))))),
      Fold(
        Topics.Wallet,
        (s, p, at) => s.onSummary(p.account, p.exchange)(_.withWallet(p.balances, at)),
      ),
      Fold(
        Topics.Balance,
        (s, p, at) => s.onSummary(p.account, p.exchange)(_.withBalance(p.asset, p.available, at)),
      ),
    ).map(f => f.topic -> f).toMap

  /** 看板要订阅的 topic —— 由 [[folds]] 派生, 见那里的说明。 */
  val topics: Set[Topic[?, ?]] = folds.keySet
