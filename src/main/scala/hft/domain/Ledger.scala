package hft.domain

/** 纯账本：持仓 + 现金，不可变、无副作用、无锁，可脱离线程/延迟同步单测。
  *
  * 持仓 size 带符号 (正多负空)，[[Ledger.Holding.entryPrice]] 为持仓均价，现金累加已实现盈亏。
  *
  * ## 账本条目与总线仓位是两个类型
  *
  * 账本条目 ([[Ledger.Holding]]) 带均价，因为已实现盈亏要用它；总线上的 [[Position]] 只有
  * 数量。分开的理由是**均价的出处不同**：账本里的均价是本地撮合逐笔加权算出来的，永远有值；
  * 而交易所报的均价各家口径不一、拿不到就得填 0，那种 0 流进盈亏计算就是一笔凭空的假亏损。
  *
  * 从前两者是同一个类型，于是"这个均价是谁算的"含糊不清，`unrealizedPnl` 还被发布路径
  * 一律强制归零 —— 一个恒为 0 的字段占着位置。
  */
final case class Ledger(account: AccountId, positions: Map[Instrument, Ledger.Holding], cash: Double):
  // 按**标的**记账而不是按交易对: 同一个 symbol 底下可能有 U 本位永续、币本位永续、
  // 几十个期权合约, 它们是各自独立的仓位。按 symbol 合并的话两条仓位会互相覆盖,
  // 而它们的敞口含义完全不同 —— 一条期权空头与一条永续多头合并出来的净额没有意义。


  /** 账户读数快照 —— 净值与名义价值的**唯一构造处**。
    *
    * 回测周期发布、影子盘周期发布、实盘替身的 REST 查询，三处此前各拼一遍
    * `AccountInfo(account, exchange, equity(...))`。同一个读数三处构造，
    * 迟早有一处漏跟上口径变化 (比如将来净值要扣未结算资金费)。
    */
  def accountInfo(exchange: Exchange, markOf: Instrument => Option[Price]): AccountInfo =
    AccountInfo(account, exchange, equity = equity(markOf))


  /** 应用一笔成交，返回新账本：
    *   - 新开 / 同向加仓：加权平均成本
    *   - 反向平仓：平掉 min(本次, 持仓) 的已实现盈亏入现金
    *   - 反手：平掉原仓后，剩余在成交价重新开仓
    *
    * @param fee 本笔手续费 (>=0, 直接从现金扣除)。类型是名义额而非裸 double —— 它是一笔钱。maker/taker 区分与费率换算由调用方 (SimState) 决定。
    */
  def applyFill(instrument: Instrument, side: Side, price: Price, qty: Coin, fee: Notional = Notional.Zero): Ledger =
    require(
      Ledger.LinearSettled.contains(instrument.kind),
      s"账本只算线性结算的品种 (${Ledger.LinearSettled.mkString("/")}), 收到 ${instrument.kind}: $instrument",
    )
    val signed = side match
      case Side.Long  => qty
      case Side.Short => -qty
    val pos = positions.getOrElse(instrument, Ledger.Holding.empty)
    val oldSize = pos.size
    val newSize = oldSize + signed
    if oldSize.isZero || (oldSize > Coin.Zero) == (signed > Coin.Zero) then
      // 新开 / 同向加仓: 加权平均成本 (按名义额加权, 故用 notional 跨到金额域)
      val newEntry =
        if oldSize.isZero then price
        else (oldSize.abs.notional(pos.entryPrice) + qty.notional(price)).pricePer(oldSize.abs + qty)
      copy(positions = positions.updated(instrument, pos.copy(size = newSize, entryPrice = newEntry)), cash = cash - fee.value)
    else
      val closeQty = qty.min(oldSize.abs)
      val dir = if oldSize > Coin.Zero then 1.0 else -1.0
      val realized = closeQty.pnl(pos.entryPrice, price).value * dir
      val newEntry =
        if signed.abs <= oldSize.abs then
          if newSize.isZero then Price.Zero else pos.entryPrice
        else price // 反手: 剩余在成交价重开
      Ledger(account, positions.updated(instrument, pos.copy(size = newSize, entryPrice = newEntry)), cash + realized - fee.value)

  /** 账户净值 = 现金 + 未实现盈亏。
    *
    * `markOf` 对**持有仓位**的标的必须给得出估值价：给不出就意味着这部分仓位的盈亏算不出来，
    * 而净值是策略的杠杆闸门读的数。从前无估值价时把那一段未实现盈亏记作 0 —— 净值静默少算
    * 一块且没有任何症状。 */
  def equity(markOf: Instrument => Option[Price]): Double =
    cash + positions.map((instrument, h) => Ledger.unrealizedPnl(instrument, h, markOf(instrument))).sum

  /** 非空持仓的**总线形态** —— 只有数量 (见 [[Position]] 关于均价与盈亏的说明)。
    *
    * **不按交易所过滤**。从前它收一个 `exchange` 参数、只返回那个所的仓位, 而 [[equity]]
    * 对全账本求和 —— 同一个账本, 两个读数两个口径。更要紧的是那道过滤在替调用方防一件
    * 由路由保证不会发生的事 (柜台只会收到自己那个 (账户, 所) 的成交), 而防的方式是**静默
    * 丢弃**: 真出现异所仓位时, 它不报错, 只是不报出来。
    *
    * 账本记的就是喂给它的那些成交。谁喂进来的谁负责 —— 单所柜台的账本因此天然只有本所的
    * 仓位, 这份就是它要报的全部。
    *
    * `markOf` 不再需要: 从前它用来回填 `unrealizedPnl`, 而那个字段已经不在 Position 上了。
    */
  def openPositions: Vector[Position] =
    positions.iterator
      .filterNot((_, h) => h.isEmpty)
      .map((instrument, h) => Position.of(account, instrument, h.size))
      .toVector

object Ledger:
  /** 本账本建模的是**线性结算**: 盈亏 = 数量 x 价差, 计价币即现金币种。
    *
    * 不在这个集合里的品种不是"还没测过", 是**公式不成立**:
    *   - [[InstrumentKind.InversePerp]] 币本位的盈亏是 `面值_usd x (1/开仓价 - 1/平仓价)`,
    *     记在基础币上 —— 与这里的 `qty x 价差` 连量纲都不同, 而现金是 USDT
    *     (`SimConfig.initialBalanceUsdt`)。算出来会是一个看着正常的数。
    *   - [[InstrumentKind.Spot]] 现货买入当场扣现金、卖出当场收现金, 没有"平仓才实现"
    *     这回事, 也不能做空。本账本的现金只在平仓时动。
    *
    * 期权在里面: USDT 结算的期权, 其权利金盈亏就是 `张数 x 权利金价差`。
    *
    * 按第 2 条在入口挡住而不是算出一个数来 —— 今天没有适配层产得出这两个品种的成交
    * (见 `ExchangeClient.supportedKinds`), 所以这道守卫现在不可能触发; 它是给接入那天的。
    */
  val LinearSettled: Set[InstrumentKind] = Set(InstrumentKind.LinearPerp, InstrumentKind.Option)

  def empty(account: AccountId, cash: Double): Ledger = Ledger(account, Map.empty, cash)

  /** 账本里的一条持仓：数量 + **本地算出来的**均价。
    *
    * 均价在这里永远有值 —— 它由 [[Ledger.applyFill]] 逐笔加权得出，不来自交易所。
    * 这正是它可以安全参与盈亏计算、而总线上的 [[Position]] 不带它的原因。
    */
  final case class Holding(size: Coin, entryPrice: Price):
    def isEmpty: Boolean = size.isZero

  object Holding:
    val empty: Holding = Holding(Coin.Zero, Price.Zero)

  /** 未实现盈亏：(标记价 - 均价) × 带符号仓位。空仓不需要估值价。 */
  private def unrealizedPnl(instrument: Instrument, h: Holding, mark: Option[Price]): Double =
    if h.isEmpty then 0.0 else h.size.pnl(h.entryPrice, requireMark(instrument, h, mark)).value

  /** 持有仓位却拿不到估值价 = 净值算不出来。只由 [[unrealizedPnl]] 在**非空仓**时调用。 */
  private def requireMark(instrument: Instrument, h: Holding, mark: Option[Price]): Price =
    mark.filter(_ > Price.Zero) match
      case Some(px) => px
      case None =>
        sys.error(
          s"$instrument 持仓 ${h.size.value} 却没有可用的估值价 (mark=$mark) —— " +
            "净值与名义额都算不出来, 记 0 会让杠杆闸门读到一个偏小的净值"
        )
