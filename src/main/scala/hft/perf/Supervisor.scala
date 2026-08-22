package hft.perf

import hft.actor.{Actor, ActorContext, ActorHandle}
import hft.domain.*
import hft.event.{AnyEvent, Event, Interest, Topics}
import hft.strategy.{AccountOutcome, OrderIntent, OutcomeEvent, Strategy}
import org.slf4j.LoggerFactory

import scala.collection.mutable

/** 按影子盘表现调度实盘实例：判据说可以就拉起，判据说该收就撤下并平掉敞口。
  *
  * {{{
  *          共享行情 ──> 影子实例 ──> 虚拟柜台 ──> 影子成交
  *                  └──> 实盘实例 ──> 交易所   ──> 实盘成交
  *                                                   │
  *                       两边成交都汇到 PerformanceTracker
  *                                   │
  *                          PromotionPolicy 决定开/关实盘
  * }}}
  *
  * 本类只做三件事：**记录战绩、按节拍问判据、执行决定**。它不含任何阈值 —— 那是
  * [[PromotionPolicy]] 的事。
  *
  * ## 降级为什么要平仓
  *
  * [[hft.engine.Executor]] 的停机收尾只撤单不平仓，因为"平不平"是策略之外的决定，
  * 框架不该替它做。但监督者恰恰**就是**做这个决定的那一层：它说"这个策略不该继续持有
  * 敞口"，所以由它显式发平仓意图。职责落在有权决定的人身上。
  *
  * ## 同一个工厂
  *
  * 影子与实盘用**同一个** [[Strategy]] 工厂：两边必须是同一份逻辑、同一组参数，
  * 否则影子盘的结论无法外推到实盘。工厂只按标的生成，不接收账户 —— 账户由装配绑定，
  * 策略自己不该知道。
  */
final class Supervisor(
    instruments: Seq[Instrument],
    paperAccount: AccountId,
    strategyFactory: Instrument => Strategy,
    policy: PromotionPolicy,
    /** 拉起 / 撤下实盘实例。由装配期注入，避免本类依赖具体的引擎类型 */
    promoteLive: (Instrument, Strategy) => ActorHandle,
    demoteLive: ActorHandle => Unit,
    /** 平仓单也要换算成交易所格式 —— 与策略发单走同一份 [[OrderConversion]] */
    symbolMetas: Map[(Exchange, Symbol), SymbolMeta],
    decideIntervalMs: Long = 5000,
) extends Actor:
  private val logger = LoggerFactory.getLogger(classOf[Supervisor])
  private val paperPerf = mutable.Map.empty[Instrument, Performance]

  /** 某标的的实盘实例。
    *
    * **"实盘在不在跑"只有这一个载体** —— 句柄的存在就是那个事实本身。此前它被
    * `liveHandles` / `livePerf` / `liveSince` 三处分别编码，判定却用了最不可靠的那份
    * (战绩的存在性)，于是有两个洞：
    *   - 晋升后到首笔成交前没有战绩，防重失效 → 每个节拍再拉起一个实例，旧句柄被覆盖、
    *     无人能停，继续真金白银交易；
    *   - 降级后战绩仍被周期发布、又灌回来，`isLive` 恒真而句柄已无 → 该标的永远无法
    *     再晋升，且每个节拍空跑一遍降级。
    */
  private final case class LiveState(handle: ActorHandle, since: Timestamp, baseline: Option[Performance], latest: Option[Performance])
  private val live = mutable.Map.empty[Instrument, LiveState]
  private var ctx: ActorContext = scala.compiletime.uninitialized
  private var lastDecision: Timestamp = 0L
  /** 各标的实盘的**累计**战绩 (跨轮存续)，仅用于晋升时取基线 */
  private val liveCumulative = mutable.Map.empty[Instrument, Performance]
  /** 已发出的平仓单：clientOrderId -> (标的, 发出时刻)。用于确认它们真的成交了 */
  private val flattening = mutable.Map.empty[String, (Instrument, Timestamp)]
  /** 降级后仍需盯着敞口是否归零的标的 */
  private val residual = mutable.Set.empty[Instrument]
  /** 上次残留告警的时刻 —— 未平掉的敞口要**反复**报，单次日志在无人盯屏时等于没有 */
  private val lastResidualWarn = mutable.Map.empty[Instrument, Timestamp]

  override def name: String = "supervisor"

  override def interests: Set[Interest] = Set(
    Interest.All(Performances),
    Interest.All(Topics.Clock),
    // 平仓单的终态：它失败就意味着敞口还在，必须有人看见
    Interest.All(Topics.OrderUpdate),
  )

  override def onStart(context: ActorContext): Unit = ctx = context

  override def onEvent(event: AnyEvent, now: Timestamp): Vector[AnyEvent] =
    event.as(Performances).foreach { perf =>
      if perf.account == paperAccount then paperPerf(perf.instrument) = perf
      else if perf.account == AccountId.Live then
        liveCumulative(perf.instrument) = perf
        // 只有正在跑实盘的标的才把战绩喂给判据。降级之后跟踪器仍会周期发布该键的历史
        // 战绩，收下它就会让"实盘在跑"这个事实起死回生。
        live.updateWith(perf.instrument)(_.map(_.copy(latest = Some(perf))))
        checkResidual(perf)
    }
    event.as(Topics.OrderUpdate).foreach(checkFlatten)
    event.as(Topics.Clock).foreach { _ =>
      checkFlattenTimeouts(now)
      maybeDecide(now)
    }
    Vector.empty

  /** 平仓单没成交就是敞口还在 —— 监督者管不了它，但必须让人看见 */
  private def checkFlatten(update: OrderUpdate): Unit =
    update.clientOrderId.foreach { cid =>
      flattening.get(cid).foreach { case (instrument, _) =>
        if update.status.isTerminal then
          flattening -= cid
          update.status match
            case OrderStatus.Filled => logger.warn(s"降级平仓已成交: $instrument")
            case other =>
              logger.error(s"!!! 降级平仓未成交 ($other): $instrument 的实盘敞口仍在, 且已无策略管理它, 请人工介入")
      }
    }

  /** 降级后仍带着敞口 —— 平仓单可能少平了 (读数落后于事实)、没成交、或回报根本没回来。
    *
    * 这条监控是平仓路径的兜底：`flatten` 的数量取自最迟落后一个发布周期的快照，
    * 而撤单是异步的，降级瞬间在途的挂单仍可能成交。reduceOnly 只防多平、不防少平。
    */
  private def checkResidual(perf: Performance): Unit =
    if residual.contains(perf.instrument) then
      if math.abs(perf.position) <= Position.Epsilon then
        residual -= perf.instrument
        lastResidualWarn -= perf.instrument
        logger.warn(s"降级后敞口已归零: ${perf.instrument}")
      else if perf.updatedAt - lastResidualWarn.getOrElse(perf.instrument, 0L) >= Supervisor.ResidualWarnIntervalMs then
        lastResidualWarn(perf.instrument) = perf.updatedAt
        logger.error(
          s"!!! ${perf.instrument} 降级后仍有实盘敞口 ${perf.position}, 已无策略管理它。" +
            "平仓单可能少平/未成交, 请人工介入"
        )

  /** 平仓单迟迟不见终态 —— 私有流掉线或漏推时，"敞口无人管"这个最该告警的状态反而零告警 */
  private def checkFlattenTimeouts(now: Timestamp): Unit =
    flattening.foreach { case (cid, (instrument, sentAt)) =>
      if now - sentAt >= Supervisor.FlattenTimeoutMs then
        logger.error(
          s"!!! $instrument 的降级平仓单 $cid 已 ${now - sentAt}ms 未见终态, 敞口可能仍在, 请人工介入"
        )
    }

  private def maybeDecide(now: Timestamp): Unit =
    if now - lastDecision < decideIntervalMs then return
    lastDecision = now
    instruments.foreach { instrument =>
      // 没有影子战绩就没有判断依据 —— 静默跳过, 而不是拿空数据去问判据
      paperPerf.get(instrument).foreach { paper =>
        val current = live.get(instrument)
        // 判据看到的是**本轮**实盘的战绩 (扣掉晋升时的基线)：跟踪器的账本跨轮存续，
        // 不分段的话第二轮实盘的数字里会混着第一轮的盈亏与降级平仓的成交。
        val view = SymbolPerformance(instrument, paper, current.flatMap(thisRound), current.map(_.since), now)
        policy.decide(view) match
          // 防重的判据是句柄在不在，不是有没有战绩 —— 后者在空窗期为空
          case Decision.Promote if current.isEmpty  => promote(instrument, now)
          case Decision.Demote if current.isDefined => demote(instrument, now)
          case _                                    => ()
      }
    }

  /** 本轮实盘的战绩 = 最新累计 - 晋升时的基线 */
  private def thisRound(st: LiveState): Option[Performance] =
    st.latest.map { latest =>
      st.baseline.fold(latest) { base =>
        latest.copy(
          realizedPnl = latest.realizedPnl - base.realizedPnl,
          fees = latest.fees - base.fees,
          fills = latest.fills - base.fills,
          roundTrips = latest.roundTrips - base.roundTrips,
          since = st.since,
        )
      }
    }

  private def promote(instrument: Instrument, now: Timestamp): Unit =
    logger.warn(s"promoting to LIVE: $instrument (影子战绩 ${paperPerf.get(instrument)})")
    val handle = promoteLive(instrument, strategyFactory(instrument))
    // 记下此刻的累计战绩作基线：跟踪器的账本跨轮存续，本轮表现要从这里往后算
    live(instrument) = LiveState(handle, now, liveCumulative.get(instrument), None)

  private def demote(instrument: Instrument, now: Timestamp): Unit =
    live.remove(instrument).foreach { st =>
      logger.warn(s"demoting from live: $instrument (本轮战绩 ${thisRound(st)})")
      // 先撤下 (它的收尾会撤掉自己的挂单)，再平掉残留敞口。
      // 顺序不能反：还活着的策略会看见平仓成交并可能立刻反手补回去。
      demoteLive(st.handle)
      flatten(instrument, st.latest.map(_.position).getOrElse(0.0), now)
      // 平仓单是异步的，且这里的仓位读数最迟落后一个发布周期 ——
      // 把该标的挂进残留监控，后续战绩若还带着敞口就告警 (见 checkResidual)
      residual += instrument
    }

  /** 平掉实盘在该标的上的残留敞口 —— reduce-only 市价，撮合层保证只减不增。
    *
    * `size` 来自最迟落后一个发布周期的快照，所以这一发不保证平干净；真正的兜底是
    * [[checkResidual]] 的持续监控。
    */
  private def flatten(instrument: Instrument, size: Quantity, now: Timestamp): Unit =
    if math.abs(size) > Position.Epsilon then
      val side = if size > 0 then Side.Short else Side.Long
      val clientOrderId = instrument.exchange.newClientOrderId
      val order = OrderConversion.toExchangeFormat(
        Order(
          id = "",
          exchange = instrument.exchange,
          symbol = instrument.symbol,
          side = side,
          orderType = OrderType.Market,
          quantity = math.abs(size),
          reduceOnly = true,
          clientOrderId = clientOrderId,
        ),
        symbolMetas,
      )
      flattening(clientOrderId) = (instrument, now)
      logger.warn(s"flattening live position: $instrument size=$size -> $side ${math.abs(size)}")
      ctx.publish(Event.local(
        OrderIntent,
        AccountOutcome(AccountId.Live, OutcomeEvent.PlaceOrders(Vector(order), s"demote flatten $instrument")),
      ))

object Supervisor:
  /** 平仓单多久没见终态就开始告警 —— 之后每个时钟节拍重复报 */
  val FlattenTimeoutMs: Long = 30_000
  /** 残留敞口的告警间隔：反复报，因为单次日志在无人盯屏时等于没有 */
  val ResidualWarnIntervalMs: Long = 30_000
