package hft.domain

/** 纯账本：仓位 + 现金，不可变、无副作用、无锁，可脱离线程/延迟同步单测。
  *
  * 仓位 size 带符号 (正多负空)，entryPrice 为持仓均价，现金累加已实现盈亏。
  */
final case class Ledger(account: AccountId, positions: Map[Symbol, Position], cash: Double):

  /** 账户读数快照 —— 净值与名义价值的**唯一构造处**。
    *
    * 回测周期发布、影子盘周期发布、实盘替身的 REST 查询，三处此前各拼一遍
    * `AccountInfo(account, exchange, equity(...), notional(...))`。同一个读数三处构造，
    * 迟早有一处漏跟上口径变化 (比如将来净值要扣未结算资金费)。
    */
  def accountInfo(exchange: Exchange, markOf: Symbol => Price): AccountInfo =
    AccountInfo(account, exchange, equity = equity(markOf), notional = notional(markOf))

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
    val pos = positions.getOrElse(symbol, Position.empty(account, exchange, symbol))
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
    cash + positions.values.map(p => Ledger.unrealizedPnl(p, markOf(p.symbol))).sum

  /** 总持仓名义价值 (用于杠杆率) */
  def notional(markOf: Symbol => Price): Double =
    positions.values.map(p => p.size.abs.notional(markOf(p.symbol)).value).sum

  /** 非空仓位 (回填最新未实现盈亏) */
  def openPositions(markOf: Symbol => Price): Vector[Position] =
    positions.values.filterNot(_.isEmpty).map(p => p.copy(unrealizedPnl = Ledger.unrealizedPnl(p, markOf(p.symbol)))).toVector

object Ledger:
  def empty(account: AccountId, cash: Double): Ledger = Ledger(account, Map.empty, cash)

  /** 未实现盈亏：(标记价 - 均价) * 带符号仓位；无估值价格时记 0 */
  private def unrealizedPnl(pos: Position, mark: Price): Double =
    if mark <= Price.Zero then 0.0 else pos.size.pnl(pos.entryPrice, mark).value
