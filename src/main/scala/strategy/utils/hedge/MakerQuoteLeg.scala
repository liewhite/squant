package strategy.utils.hedge

import hft.domain.*

/** **被动挂单腿** —— "任一时刻账上最多一张单"的状态机 + 被动价计算。
  *
  * 抽出来是因为这段机制在两个对冲策略里一模一样（价格轴触发的
  * [[strategy.strategies.makerhedge.logic.MakerHedgeStrategy]] 与敞口轴触发的
  * [[strategy.strategies.ivsellhedge.logic.DeltaKamaHedgeStrategy]]）：两者只在**何时该对冲**
  * 上不同，**怎么挂、怎么撤、怎么防重**完全相同。各写一份的话，改了其中一份不会有任何编译
  * 错误 —— 只是从此两个策略的挂单行为悄悄分叉。
  *
  * ## 四个态，因为"撤单在途"是一个真实的状态
  *
  * {{{
  *   Idle ──place──> Placing ──Pending──> Resting ──超时──> Cancelling ──终态──> Idle
  *     ^                │                    │                              │
  *     └────────────────┴────────────────────┴──────────────────────────────┘  (终态回报)
  * }}}
  *
  * **[[Cancelling]] 不能省。** 撤单是异步的：发出撤单请求到终态回报到达之间有个窗口，而这段
  * 时间里 BBO 会 tick 好几次。把这个窗口当成"已经空闲"就会出孤儿挂单 ——
  * 挂出新单 B，紧接着旧单 A 的 `Cancelled` 到达并把状态清成空闲，于是又挂出 C；
  * B 的回报后到、随即被 C 覆盖，从此无人管、无人撤。框架的超时清理只管 `Created` 态
  * （见 `SymbolState.failOnTimedOutOrders`），对已经 Pending 的 B 没有兜底，
  * 它最终会在一个陈旧价位上成交，只能靠下一轮对账反向擦掉 —— 白付两笔手续费。
  *
  * ## 回报按 orderId 匹配
  *
  * 因为任一时刻账上最多一张单，[[Resting]] / [[Cancelling]] 期间收到**别的** orderId 的回报
  * 只可能是一张已经结束的旧单的迟到消息，一律忽略。不核对 id 的话，那条迟到消息会把当前
  * 这张单的状态一起清掉 —— 这正是上面那个孤儿挂单的成因。
  *
  * [[Placing]] 期间还没有 id（clientOrderId 由框架在处理器返回之后才生成），所以第一条
  * 非 `Created` 回报的 orderId 就是这张单的 id。这一步安全，正因为不变量保证此刻账上只有它。
  *
  * ## 撤单不回应时不静默等下去
  *
  * 撤单的终态确认以私有流为准，框架不合成。若那条确认始终不来，干等就是"从此再不对冲"
  * 且没有任何症状。所以 [[Cancelling]] 超过 [[requoteMs]] 会**重发同一张单的撤单**
  * （同一个 id，不可能造出新的孤儿），并由调用方打日志。
  *
  * ## 时钟域
  *
  * [[step]] 的 `now` 与回报里的时间戳必须同一个时钟。注意各交易所适配层给的
  * `OrderUpdate.timestamp` 并不都是交易所时钟（OKX 私有流盖的是本地收到时刻），
  * 所以这里只要求"调用方两边用同一个域"，不承诺它是哪一个。
  */
final class MakerQuoteLeg(val offsetPct: Double, val requoteMs: Long):
  import MakerQuoteLeg.*

  private var state: State = State.Idle

  /** 当前状态（诊断/测试用） */
  def phase: State = state

  /** 本腿的订单回报驱动状态推进。**Filled 时返回成交价**，供调用方重置自己的判据基准
    * （价格轴策略把对冲中心移到成交价；敞口轴策略不需要，忽略即可）。 */
  def onOrderUpdate(u: OrderUpdate): Option[Price] =
    state match
      case State.Idle => None // 已结束的旧单的迟到消息
      case State.Placing =>
        u.status match
          case OrderStatus.Created => None // 本地态，还不带交易所 id
          case OrderStatus.Pending | OrderStatus.PartiallyFilled(_) =>
            state = State.Resting(u.orderId, u.timestamp); None
          case OrderStatus.Filled => state = State.Idle; Some(u.price)
          case OrderStatus.Cancelled | OrderStatus.Rejected(_) | OrderStatus.Error(_) =>
            state = State.Idle; None
      case State.Resting(id, _) if u.orderId == id =>
        u.status match
          case OrderStatus.Pending | OrderStatus.PartiallyFilled(_) =>
            state = State.Resting(id, u.timestamp); None
          case OrderStatus.Filled => state = State.Idle; Some(u.price)
          case OrderStatus.Cancelled | OrderStatus.Rejected(_) | OrderStatus.Error(_) =>
            state = State.Idle; None
          case OrderStatus.Created => None
      case State.Cancelling(id, _) if u.orderId == id =>
        u.status match
          // 撤单还没生效，继续等它的终态 —— 这期间不能挂新单
          case OrderStatus.Pending | OrderStatus.PartiallyFilled(_) | OrderStatus.Created => None
          case OrderStatus.Filled => state = State.Idle; Some(u.price)
          case OrderStatus.Cancelled | OrderStatus.Rejected(_) | OrderStatus.Error(_) =>
            state = State.Idle; None
      case _ => None // orderId 不匹配：别的（已结束的）单的迟到消息

  /** 本 tick 这条腿能做什么 */
  def step(now: Timestamp): Step =
    state match
      case State.Idle    => Step.Ready
      case State.Placing => Step.Blocked
      case State.Resting(id, at) =>
        if now - at > requoteMs then
          state = State.Cancelling(id, now)
          Step.Requote(OrderRef.ByExchangeId(id), retry = false)
        else Step.Blocked
      case State.Cancelling(id, sentAt) =>
        // 撤单确认迟迟不来：重发同一张单的撤单，而不是从此静默不动
        if now - sentAt > requoteMs then
          state = State.Cancelling(id, now)
          Step.Requote(OrderRef.ByExchangeId(id), retry = true)
        else Step.Blocked

  /** 记下"已发出下单请求、等确认"，返回该挂的**被动价**。
    *
    * 卖挂 `bestAsk·(1+offset)`、买挂 `bestBid·(1−offset)` —— 即对手盘**外** offset，
    * 保证 PostOnly 不会吃单。盘口缺失时回退 `fallbackPx`（降级，调用方应告警）。
    *
    * 必须在**真正要返回下单意图时**调用：提前调用会让这条腿进入等确认态，而那张单可能因为
    * 后续分支根本没发出去 —— 从此永远在等一个不会来的确认。
    *
    * 只在 [[Step.Ready]] 之后调用；其它态下调用会抛 —— 那意味着调用方绕过了 [[step]]，
    * 而后果正是本类要消灭的孤儿挂单。
    */
  def place(side: Side, bbo: Option[BBO], fallbackPx: Price): Price =
    require(state == State.Idle, s"只能在空闲态下单，当前 $state —— 绕过 step 会挂出无人管的孤儿单")
    state = State.Placing
    val ref = side match
      case Side.Short => bbo.fold(fallbackPx)(_.askPrice)
      case Side.Long  => bbo.fold(fallbackPx)(_.bidPrice)
    if side == Side.Short then ref.scaled(1.0 + offsetPct) else ref.scaled(1.0 - offsetPct)

object MakerQuoteLeg:
  /** 挂单腿的状态 */
  enum State:
    /** 账上无单，可以挂 */
    case Idle
    /** 已发出下单请求，等交易所确认（此刻还没有 orderId） */
    case Placing
    /** 在簿上。`at` = 最近一条回报的时刻，重挂年龄从它算 */
    case Resting(id: OrderId, at: Timestamp)
    /** 已发出撤单请求，等终态。`sentAt` = 撤单发出时刻，用于超时重发 */
    case Cancelling(id: OrderId, sentAt: Timestamp)

  /** 本 tick 该做的事 */
  enum Step:
    /** 什么都别做（等确认 / 挂单还没到重挂年龄 / 撤单在途） */
    case Blocked
    /** 撤掉这一张。`retry=true` 表示上一次撤单请求迟迟没有终态回报，这是重发（调用方应告警） */
    case Requote(ref: OrderRef, retry: Boolean)
    /** 空闲，可以下新单 */
    case Ready
