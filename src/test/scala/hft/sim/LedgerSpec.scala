package hft.sim

import hft.domain.*

/** 账本与撮合判定的纯单测：无线程、无延迟、无 sleep。 */
class LedgerSpec extends munit.FunSuite:
  private val ex = Exchange.Binance
  private val sym = "BTCUSDT"
  private def empty = Ledger.empty(AccountId.Live, 10_000.0)
  private def sizeOf(l: Ledger): Double = l.positions.get(sym).map(_.size).getOrElse(0.0)
  private def entryOf(l: Ledger): Double = l.positions.get(sym).map(_.entryPrice).getOrElse(0.0)

  test("新开多头: 记录均价, 现金不变"):
    val l = empty.applyFill(ex, sym, Side.Long, price = 100.0, qty = 2.0)
    assertEquals(sizeOf(l), 2.0)
    assertEquals(entryOf(l), 100.0)
    assertEquals(l.cash, 10_000.0)

  test("同向加仓: 加权平均成本"):
    val l = empty
      .applyFill(ex, sym, Side.Long, 100.0, 2.0)
      .applyFill(ex, sym, Side.Long, 110.0, 2.0)
    assertEquals(sizeOf(l), 4.0)
    assertEquals(entryOf(l), 105.0) // (2*100 + 2*110)/4
    assertEquals(l.cash, 10_000.0)

  test("部分平仓: 实现盈亏入现金, 均价不变"):
    val l = empty
      .applyFill(ex, sym, Side.Long, 100.0, 4.0)
      .applyFill(ex, sym, Side.Short, 120.0, 1.0) // 平 1 个, 盈利 20
    assertEquals(sizeOf(l), 3.0)
    assertEquals(entryOf(l), 100.0)
    assertEquals(l.cash, 10_020.0)

  test("全平多头: 仓位归零, 均价清零, 盈亏入账"):
    val l = empty
      .applyFill(ex, sym, Side.Long, 100.0, 2.0)
      .applyFill(ex, sym, Side.Short, 90.0, 2.0) // 平 2 个, 亏损 20
    assertEquals(sizeOf(l), 0.0)
    assertEquals(entryOf(l), 0.0)
    assertEquals(l.cash, 9_980.0)

  test("反手: 平掉原仓 + 剩余在成交价反向重开"):
    val l = empty
      .applyFill(ex, sym, Side.Long, 100.0, 2.0)
      .applyFill(ex, sym, Side.Short, 120.0, 5.0) // 平多 2 (盈利 40), 反手开空 3 @120
    assertEquals(sizeOf(l), -3.0)
    assertEquals(entryOf(l), 120.0)
    assertEquals(l.cash, 10_040.0)

  test("空头平仓盈亏方向正确 (跌价获利)"):
    val l = empty
      .applyFill(ex, sym, Side.Short, 100.0, 2.0)
      .applyFill(ex, sym, Side.Long, 90.0, 2.0) // 空头平于低价, 盈利 20
    assertEquals(sizeOf(l), 0.0)
    assertEquals(l.cash, 10_020.0)

  test("equity / notional 用未实现盈亏估值"):
    val l = empty.applyFill(ex, sym, Side.Long, 100.0, 2.0)
    val markOf = (_: Symbol) => 150.0
    assertEquals(l.equity(markOf), 10_000.0 + (150.0 - 100.0) * 2.0) // 10100
    assertEquals(l.notional(markOf), 2.0 * 150.0) // 300
    assertEquals(l.openPositions(markOf).head.unrealizedPnl, 100.0)

  test("无估值价格 (mark<=0) 时未实现盈亏记 0"):
    val l = empty.applyFill(ex, sym, Side.Long, 100.0, 2.0)
    assertEquals(l.equity((_: Symbol) => 0.0), 10_000.0)

  test("Matcher.crosses: 买单卖价跌破成交, 卖单买价升破成交"):
    val bbo = BBO(ex, sym, bidPrice = 100.0, bidQty = 1, askPrice = 101.0, askQty = 1, timestamp = 0)
    assert(!Matcher.crosses(Side.Long, 99.0, bbo))  // ask 101 > 99, 不成交
    assert(Matcher.crosses(Side.Long, 101.0, bbo))  // ask 101 <= 101, 成交
    assert(!Matcher.crosses(Side.Short, 102.0, bbo)) // bid 100 < 102, 不成交
    assert(Matcher.crosses(Side.Short, 100.0, bbo))  // bid 100 >= 100, 成交

  test("Matcher.touchPrice: 买单吃卖价, 卖单吃买价"):
    val bbo = BBO(ex, sym, 100.0, 1, 101.0, 1, 0)
    assertEquals(Matcher.touchPrice(Side.Long, bbo), 101.0)
    assertEquals(Matcher.touchPrice(Side.Short, bbo), 100.0)
