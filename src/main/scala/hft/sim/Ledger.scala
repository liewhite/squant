package hft.sim

import hft.domain.*

/** 撮合判定 (纯函数)。挂单成交判定即「BBO 越过挂单价」。 */
object Matcher:
  /** resting 单是否被 BBO 越过：买单在最优卖价跌破挂单价时成交，卖单在最优买价升破挂单价时成交。
    * 到达撮合时的「可成交性 (marketable)」判定与此完全一致 —— 保证「不可成交即 resting、
    * 下个 BBO 再撮合」自洽。
    */
  def crosses(side: Side, limitPrice: Price, bbo: BBO): Boolean = side match
    case Side.Long  => bbo.askPrice <= limitPrice
    case Side.Short => bbo.bidPrice >= limitPrice

  /** resting 单是否被一笔**真实成交**越过 (trade-print 撮合，严格不含相等)：
    * 买单在成交价**跌破**挂单价时成交、卖单在成交价**升破**挂单价时成交。
    * 相等不算 (价格只触及挂单价时通常排在队尾，未真正穿过)，是更保守的下界模型。
    */
  def tradeCrosses(side: Side, limitPrice: Price, tradePrice: Price): Boolean = side match
    case Side.Long  => tradePrice < limitPrice
    case Side.Short => tradePrice > limitPrice

  /** 主动成交 (taker) 的对手价：买单吃最优卖价，卖单吃最优买价 */
  def touchPrice(side: Side, bbo: BBO): Price = side match
    case Side.Long  => bbo.askPrice
    case Side.Short => bbo.bidPrice

/** 纯账本：仓位 + 现金，不可变、无副作用、无锁，可脱离线程/延迟同步单测。
  *
  * 仓位 size 带符号 (正多负空)，entryPrice 为持仓均价，现金累加已实现盈亏。
  */
final case class Ledger(account: AccountId, positions: Map[Symbol, Position], cash: Double):

  /** 应用一笔成交，返回新账本：
    *   - 新开 / 同向加仓：加权平均成本
    *   - 反向平仓：平掉 min(本次, 持仓) 的已实现盈亏入现金
    *   - 反手：平掉原仓后，剩余在成交价重新开仓
    *
    * @param fee 本笔手续费 (>=0, 直接从现金扣除)。maker/taker 区分与费率换算由调用方 (SimState) 决定。
    */
  def applyFill(exchange: Exchange, symbol: Symbol, side: Side, price: Price, qty: Coin, fee: Double = 0.0): Ledger =
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
        else (oldSize.abs.notional(pos.entryPrice) + qty.notional(price)) / (oldSize.abs + qty).value
      copy(positions = positions.updated(symbol, pos.copy(size = newSize, entryPrice = newEntry)), cash = cash - fee)
    else
      val closeQty = qty.min(oldSize.abs)
      val dir = if oldSize > Coin.Zero then 1.0 else -1.0
      val realized = closeQty.notional(price - pos.entryPrice) * dir
      val newEntry =
        if signed.abs <= oldSize.abs then
          if newSize.isZero then 0.0 else pos.entryPrice
        else price // 反手: 剩余在成交价重开
      Ledger(account, positions.updated(symbol, pos.copy(size = newSize, entryPrice = newEntry)), cash + realized - fee)

  /** 账户净值 = 现金 + 未实现盈亏 (markOf 提供各 symbol 的估值价格) */
  def equity(markOf: Symbol => Double): Double =
    cash + positions.values.map(p => Ledger.unrealizedPnl(p, markOf(p.symbol))).sum

  /** 总持仓名义价值 (用于杠杆率) */
  def notional(markOf: Symbol => Double): Double =
    positions.values.map(p => p.size.abs.notional(markOf(p.symbol))).sum

  /** 非空仓位 (回填最新未实现盈亏) */
  def openPositions(markOf: Symbol => Double): Vector[Position] =
    positions.values.filterNot(_.isEmpty).map(p => p.copy(unrealizedPnl = Ledger.unrealizedPnl(p, markOf(p.symbol)))).toVector

object Ledger:
  def empty(account: AccountId, cash: Double): Ledger = Ledger(account, Map.empty, cash)

  /** 未实现盈亏：(标记价 - 均价) * 带符号仓位；无估值价格时记 0 */
  private def unrealizedPnl(pos: Position, mark: Double): Double =
    if mark <= 0.0 then 0.0 else pos.size.notional(mark - pos.entryPrice)
