package hft.exchange

import hft.domain.*

/** 一个账户在一个交易所的仓位账本 —— **纯逻辑**：不碰总线、不碰 REST、不读时钟。
  *
  * 三件事收在这里，它们本来就是同一件事的三面：
  *
  *   1. **记账**：按订单回报的累计成交量算增量入账（增量 = 累计 − 已记）
  *   2. **记账进度**：每张单已记到哪了 —— 差分的前值，加上终态之后的墓碑
  *   3. **三方对账**：两本独立记出来的账 + 交易所报的读数，两两比对
  *
  * 从柜台里分出来是因为它们**没有一处依赖外部世界**：给定同一串输入必得同一个结果。
  * 留在柜台里的话，要验证"分批成交怎么记""墓碑清早了会怎样""三方谁跟谁对不上"，
  * 就得先装一台柜台、连一条总线、发一串事件 —— 而那些设施与要验证的东西毫无关系。
  *
  * 时间由调用方传入（`now`），所以连"过了保留期没有"这种判断也测得动。
  *
  * @param dustOf 视作零的量级（币本位）。`sizeStep` 是**张**，拿它直接当阈值会在
  *               contractSize ≠ 1 的品种上差一个数量级，故由调用方换算好再传进来
  */
final class PositionBook(account: AccountId, exchange: Exchange, dustOf: Symbol => Double):
  import PositionBook.*

  /** 主账本 —— 由订单回报的累计成交量驱动。**发到总线的仓位就是它**。
    *
    * 只有数量, 不是 [[Ledger]]: 柜台的账本从不读均价, 也没有现金可言 (那是撮合的事)。
    * 用全套账本等于给这里凭空引进两个它填不出、也用不上的量 —— 而"填不出就填 0"的字段
    * 迟早会被谁读走。 */
  private var sizes: Map[Symbol, Coin] = Map.empty

  /** 成交明细独立累出来的账 —— **第二意见**。
    *
    * 它与 [[ledger]] 走的是两条完全不同的渠道（累计成交量 / 逐笔明细）。两者对不上
    * 说明**我们这边**有问题（少解析了一条推送、字段读错、去重去多了），
    * 而不是账户被外部改了 —— 后者要看 [[reported]]。三方比对的价值全在这个区分上。
    */
  private var fillSizes: Map[Symbol, Coin] = Map.empty

  /** 交易所报的最新仓位 —— **第三方读数**，不是账本 */
  private val reported = scala.collection.mutable.Map.empty[Symbol, Coin]

  /** 每张订单**已记进账本**的累计成交量 —— 差分的前值，也是去重的依据 */
  private val settled = scala.collection.mutable.Map.empty[OrderId, Settlement]

  /** 本账本负责的标的 —— 由对齐指令划定 */
  private var managed: Set[Symbol] = Set.empty

  /** 连续对不上的次数：(标的, 比对种类) -> 次数 */
  private val disagreements = scala.collection.mutable.Map.empty[(Symbol, Kind), Int]

  /** 这个标的归本账本管吗。交易所推的是整个账户的动静，别人的标的不归它记也不归它对 */
  def manages(symbol: Symbol): Boolean = managed.contains(symbol)

  def managedSymbols: Set[Symbol] = managed

  def positionOf(symbol: Symbol): Position =
    Position(account, exchange, symbol, sizes.getOrElse(symbol, Coin.Zero))

  /** 按标的**增量**重置 —— 只动这批，别把上一批的账抹掉。
    *
    * 引擎为每批新加的策略都发一次对齐指令，`symbols` 只含那一批。整本替换会让先装的
    * 策略的仓位与记账进度一起归零，那些策略从此按零仓决策而没有任何症状。
    *
    * 既有挂单的已成交量要填进记账进度：少了这一步，对齐后第一条回报会把对齐之前就已
    * 成交的部分再记一遍 —— 拉到的仓位里本已含着它，而记账进度是空的。
    */
  def align(symbols: Set[Symbol], positions: Vector[Position], pendingOrders: Vector[OrderUpdate], now: Timestamp): Unit =
    val refreshed = positions.filter(p => symbols.contains(p.symbol)).map(p => p.symbol -> p.size).toMap
    sizes = (sizes -- symbols) ++ refreshed
    fillSizes = (fillSizes -- symbols) ++ refreshed
    reported --= symbols
    disagreements.filterInPlace((key, _) => !symbols.contains(key._1))
    // 记账进度不清空：这批标的还活着的订单会被下面的挂单快照覆盖，已终态的靠墓碑自然过期。
    // 清掉反而会让晚到的回报从零重记。
    //
    // 挂单同样按 `symbols` 过滤：账本重置与进度重置**必须同批**。放进一张别批标的的挂单，
    // 它的记账进度会被刷成快照值而对应账本没跟着重置 —— 增量差额从此永久漏记, 仓位错且无症状。
    // 这条不变量收在这里, 不能指望每个调用方都只传本批的挂单。
    pendingOrders.filter(o => symbols.contains(o.symbol) && o.filledQuantity.nonZero).foreach { order =>
      settled(order.orderId) = Settlement(order.filledQuantity, firstSeenAt = now, terminalAt = None)
    }
    managed ++= symbols

  /** 把某订单的账记到 `cumulative` 为止。
    *
    * 增量为零（重复推送、或这条回报没带来新成交）时什么都不发生 —— 幂等是这套记账的
    * 立身之本：重复推送记不进第二次、跨频道乱序谁先到谁记账、丢一条推送由下一条的
    * 累计量补回来。
    */
  def settle(orderId: OrderId, symbol: Symbol, side: Side, price: Price, cumulative: Coin, now: Timestamp): Settled =
    val already = settled.get(orderId).map(_.cumulative).getOrElse(Coin.Zero)
    val delta = cumulative - already
    val dust = dustOf(symbol)
    // 半个最小变动单位以下视作零 —— 两个精确值相减仍会留下浮点尾巴
    // (0.8 - 0.3 = 0.5000000000000001)，严格比较会让它产出一笔量级 1e-16 的幻影成交
    if delta.value <= dust then
      // 明显的倒退与浮点尾巴区分开：两者都不入账 (记账只认单调增的累计量)，
      // 但倒退值得留一条线索 —— 它多半只是乱序后到的旧推送, 也可能是适配层读错了字段,
      // 而在本地这两者无从分辨。判定留在这里, 要不要出声由柜台决定。
      if delta.value < -dust then Settled.Regressed(cumulative, already) else Settled.Unchanged
    else if price.value <= 0.0 then
      // 非正的成交均价不是任何市场状态, 只能是**适配层填错了价格字段** (市价单的委托价是空的)。
      //
      // 从前这里返回 Settled.Rejected, 柜台打一条 error 然后继续 —— 也就是**明知有一笔成交却
      // 不入账并接着交易**。此后账本确定性地少一笔, 策略看到旧仓位再下一单, 正是柜台花大段
      // 文字要消灭的那个"危险侧中间状态", 只不过成因从交易所换成了我们自己。
      // 已经确认是 bug 的输入不该有"继续"这条路: 在第一现场抛, 带上足以定位的上下文。
      throw IllegalStateException(
        s"$symbol order=$orderId 成交均价为 ${price.value} (非正), 拒绝入账并终止 —— " +
          s"适配层多半填了委托价而非成交均价 (side=$side cumulative=${cumulative.value} 已记=${already.value})"
      )
    else
      settled(orderId) = settled.get(orderId) match
        case Some(prev) => prev.copy(cumulative = cumulative)
        case None       => Settlement(cumulative, firstSeenAt = now, terminalAt = None)
      sizes = sizes.updated(symbol, sizes.getOrElse(symbol, Coin.Zero) + PositionBook.signed(side, delta))
      Settled.Recorded(delta, positionOf(symbol))

  /** 订单进终态：立墓碑，不删。
    *
    * 状态与成交常走两条频道，"已成交"先到、那笔成交的推送随后才到是常态。
    * 删了记账进度，晚到的那条会被当成新成交记第二遍 —— 仓位凭空翻倍。
    */
  def markTerminal(orderId: OrderId, now: Timestamp): Unit =
    settled.updateWith(orderId)(_.map(_.copy(terminalAt = Some(now))))

  /** 成交明细进第二本账 —— 只为对账，不影响发到总线的仓位 */
  def recordFill(symbol: Symbol, side: Side, qty: Coin): Unit =
    fillSizes = fillSizes.updated(symbol, fillSizes.getOrElse(symbol, Coin.Zero) + PositionBook.signed(side, qty))

  /** 记下交易所报的仓位，按节拍统一对账 */
  def observeReported(symbol: Symbol, size: Coin): Unit = reported(symbol) = size

  /** 走一遍对账与清理，返回需要喊出来的事。
    *
    * 调用方只管把它们写进日志 —— 判定在这里，节流也在这里。
    */
  def audit(now: Timestamp): Vector[Alarm] =
    val stale = evictSettled(now)
    val mismatches = managed.toVector.sorted.flatMap { symbol =>
      val byOrders = positionOf(symbol).size
      val byFills = fillSizes.getOrElse(symbol, Coin.Zero)
      compare(byOrders, byFills, reported.get(symbol), dustOf(symbol) * 2)
        .flatMap(verdict => record(symbol, verdict))
    }
    stale ++ mismatches

  /** 记一次比对结果：一致就清零，不一致就累加，连续够多次才喊。
    *
    * 中间那几次咽下去 —— 它们多半是时序窗口，报出来会把真问题淹掉。
    */
  private def record(symbol: Symbol, verdict: Verdict): Option[Alarm] =
    val key = (symbol, verdict.kind)
    verdict match
      case _: Verdict.Agreed =>
        disagreements.remove(key)
        None
      case mismatch =>
        val times = disagreements.getOrElse(key, 0) + 1
        disagreements(key) = times
        // 恰好到阈值时喊一次；之后不再重复刷屏，首次告警已经说清楚了
        Option.when(times == DisagreementsBeforeAlarm)(Alarm.Disagreement(symbol, mismatch))

  /** 清掉已终态且过了保留期的记账进度；顺便报出长期等不到终态的那些。
    *
    * **非终态的永远不清**：在累计量记账的体系里，清掉一张还活着的订单的进度，
    * 下一条带真累计量的回报就会以"已记 0"重新记一遍，此前入账的全部数量再记一次。
    * 而"部分成交之后继续挂着"是完全正常的形态（GTC、冰山单），按"存在多久"去判它
    * 是不是僵尸，判据本身就不成立。
    */
  private def evictSettled(now: Timestamp): Vector[Alarm] =
    val stale = settled.iterator.collect {
      case (orderId, s) if s.terminalAt.isEmpty && !s.staleWarned && now - s.firstSeenAt > StaleSettlementMs =>
        orderId -> s
    }.toVector
    stale.foreach((orderId, s) => settled(orderId) = s.copy(staleWarned = true)) // 只报一次
    settled.filterInPlace((_, s) => retains(s, now))
    stale.map((orderId, s) => Alarm.StaleSettlement(orderId, s.cumulative))

object PositionBook:
  /** 方向变成符号：多为正、空为负 */
  private def signed(side: Side, qty: Coin): Coin = side match
    case Side.Long  => qty
    case Side.Short => -qty

  /** 订单终态后，记账进度还要留多久 —— 留着是为了让晚到的成交推送认得出"已经记过了" */
  val SettledRetentionMs: Long = 60_000

  /** 一张单停在"有成交、无终态"多久之后值得报一句。**只报，不清** */
  val StaleSettlementMs: Long = 60 * 60 * 1000

  /** 连续对不上多少次才升级为告警。偶尔一次是时序窗口，连续三次跨越十几秒解释不了 */
  val DisagreementsBeforeAlarm: Int = 3

  /** 一张订单的记账进度。`terminalAt` 有值表示已终态，只等过保留期被清掉 */
  final case class Settlement(
      cumulative: Coin,
      firstSeenAt: Timestamp,
      terminalAt: Option[Timestamp],
      staleWarned: Boolean = false,
  )

  /** 这条记账进度此刻还该不该留着 */
  def retains(settlement: Settlement, now: Timestamp): Boolean =
    settlement.terminalAt.forall(at => now - at <= SettledRetentionMs)

  /** 记一笔账的几种结局 */
  enum Settled:
    case Recorded(delta: Coin, position: Position)
    /** 增量在尘埃量级以内 —— 重复推送, 或这条回报没带来新成交 */
    case Unchanged
    /** 累计量比已记的还少。**不入账**, 因为记账只认单调增的累计量。
      *
      * 绝大多数情况是乱序后到的旧推送 (完全正常), 少数情况是适配层读错了字段。
      * 两者在本地分辨不了, 所以这不是告警, 只是一条排查时用得上的线索。
      */
    case Regressed(cumulative: Coin, already: Coin)

  /** 比对的种类 —— 决定了不一致该怎么处理 */
  enum Kind:
    /** 两条内部渠道 —— 对不上是**我们的 bug** */
    case Internal
    /** 账本与交易所 —— 对不上是**外部事实** */
    case External

  /** 一次比对的结论 */
  enum Verdict(val kind: Kind):
    case Agreed(k: Kind) extends Verdict(k)
    case Internal(byOrders: Coin, byFills: Coin) extends Verdict(Kind.Internal)
    case External(byOrders: Coin, reported: Coin) extends Verdict(Kind.External)

    def explain: String = this match
      case Agreed(_) => "一致"
      case Internal(byOrders, byFills) =>
        s"两条内部渠道对不上: 订单回报账=${byOrders.value} 成交明细账=${byFills.value}。" +
          "**这是我们这边的 bug** —— 少解析了一条推送 / 字段读错 / 去重去多了, 与账户被外部改动无关, 请查适配层"
      case External(byOrders, reported) =>
        s"账本与交易所对不上: 本地=${byOrders.value} 交易所=${reported.value}。" +
          "常见成因: 强平 / 手动干预 / 资金费结算 / 漏收订单回报; " +
          "**若刚启动不久, 也可能是对齐竞态** —— 一张在对齐瞬间恰好完全成交的单, " +
          "其回报晚于 REST 快照到达, 会被当成新成交重记一遍。" +
          "策略正按本地账本决策, 需人工确认; 确认后**先撤掉在途单**再发账户对齐指令让柜台按 REST 快照重置 —— " +
          "有单在途时重置会把一笔既在快照里、回报又还在路上的成交记两遍"

  /** 要喊出来的事 */
  enum Alarm:
    case Disagreement(symbol: Symbol, verdict: Verdict)
    /** 有成交却长期等不到终态回报 —— 通常意味着丢了一条订单推送 */
    case StaleSettlement(orderId: OrderId, cumulative: Coin)

  /** 三方比对 —— 纯函数。交易所读数缺席时（还没推过）只做内部比对：
    * 那不是"一致"，是"无从比较"。 */
  def compare(byOrders: Coin, byFills: Coin, reported: Option[Coin], tolerance: Double): Vector[Verdict] =
    val internal =
      if (byOrders - byFills).abs.value <= tolerance then Verdict.Agreed(Kind.Internal)
      else Verdict.Internal(byOrders, byFills)
    val external = reported.map { r =>
      if (byOrders - r).abs.value <= tolerance then Verdict.Agreed(Kind.External) else Verdict.External(byOrders, r)
    }
    internal +: external.toVector
