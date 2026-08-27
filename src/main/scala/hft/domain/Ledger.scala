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
final case class Ledger(account: AccountId, positions: Map[Symbol, Ledger.Holding], cash: Double):

  /** 账户读数快照 —— 净值与名义价值的**唯一构造处**。
    *
    * 回测周期发布、影子盘周期发布、实盘替身的 REST 查询，三处此前各拼一遍
    * `AccountInfo(account, exchange, equity(...), notional(...))`。同一个读数三处构造，
    * 迟早有一处漏跟上口径变化 (比如将来净值要扣未结算资金费)。
    */
  def accountInfo(exchange: Exchange, markOf: Symbol => Price): AccountInfo =
    AccountInfo(account, exchange, equity = equity(markOf))

  /** 应用一笔成交，返回新账本：
    *   - 新开 / 同向加仓：加权平均成本
    *   - 反向平仓：平掉 min(本次, 持仓) 的已实现盈亏入现金
    *   - 反手：平掉原仓后，剩余在成交价重新开仓
    *
    * @param fee 本笔手续费 (>=0, 直接从现金扣除)。类型是名义额而非裸 double —— 它是一笔钱。maker/taker 区分与费率换算由调用方 (SimState) 决定。
    */
  def applyFill(exchange: Exchange, symbol: Symbol, side: Side, price: Price, qty: Coin, fee: Notional = Notional.Zero): Ledger =
    val signed = side match
      case Side.Long  => qty
      case Side.Short => -qty
    val pos = positions.getOrElse(symbol, Ledger.Holding.empty)
    val oldSize = pos.size
    val newSize = oldSize + signed
    if oldSize.isZero || (oldSize > Coin.Zero) == (signed > Coin.Zero) then
      // 新开 / 同向加仓: 加权平均成本 (按名义额加权, 故用 notional 跨到金额域)
      val newEntry =
        if oldSize.isZero then price
        else (oldSize.abs.notional(pos.entryPrice) + qty.notional(price)).pricePer(oldSize.abs + qty)
      copy(positions = positions.updated(symbol, pos.copy(size = newSize, entryPrice = newEntry)), cash = cash - fee.value)
    else
      val closeQty = qty.min(oldSize.abs)
      val dir = if oldSize > Coin.Zero then 1.0 else -1.0
      val realized = closeQty.pnl(pos.entryPrice, price).value * dir
      val newEntry =
        if signed.abs <= oldSize.abs then
          if newSize.isZero then Price.Zero else pos.entryPrice
        else price // 反手: 剩余在成交价重开
      Ledger(account, positions.updated(symbol, pos.copy(size = newSize, entryPrice = newEntry)), cash + realized - fee.value)

  /** 账户净值 = 现金 + 未实现盈亏 (markOf 提供各 symbol 的估值价格) */
  def equity(markOf: Symbol => Price): Double =
    cash + positions.map((sym, h) => Ledger.unrealizedPnl(h, markOf(sym))).sum

  /** 总持仓名义价值 (用于杠杆率) */
  def notional(markOf: Symbol => Price): Double =
    positions.map((sym, h) => h.size.abs.notional(markOf(sym)).value).sum

  /** 非空持仓的**总线形态** —— 只有数量 (见 [[Position]] 关于均价与盈亏的说明)。
    *
    * `markOf` 不再需要: 从前它用来回填 `unrealizedPnl`, 而那个字段已经不在 Position 上了。
    */
  def openPositions(exchange: Exchange): Vector[Position] =
    positions.iterator
      .filterNot((_, h) => h.isEmpty)
      .map((sym, h) => Position(account, exchange, sym, h.size))
      .toVector

object Ledger:
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

  /** 未实现盈亏：(标记价 - 均价) * 带符号仓位；无估值价格时记 0 */
  private def unrealizedPnl(h: Holding, mark: Price): Double =
    if mark <= Price.Zero then 0.0 else h.size.pnl(h.entryPrice, mark).value
