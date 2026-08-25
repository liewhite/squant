package hft.sim

import hft.domain.*
import hft.TestUnits.given

/** 账本与撮合判定的纯单测：无线程、无延迟、无 sleep。 */
class LedgerSpec extends munit.FunSuite:
  private val ex = Exchange.Binance
  private val sym = "BTCUSDT"
  private def empty = Ledger.empty(AccountId.Live, 10_000.0)
  private def sizeOf(l: Ledger): Double = l.positions.get(sym).map(_.size.value).getOrElse(0.0)
  private def entryOf(l: Ledger): Double = l.positions.get(sym).map(_.entryPrice).getOrElse(0.0)

  test("新开多头: 记录均价, 现金不变"):
    val l = empty.applyFill(ex, sym, Side.Long, price = 100.0, qty = 2.0)
    assertEquals(sizeOf(l), 2.0)
    assertEquals(entryOf(l), 100.0)
    assertEquals(l.cash, 10_000.0)

  test("同向加仓: 加权平均成本"):
    val l = empty
      .applyFill(ex, sym, Side.Long, 100.0, Coin(2.0))
      .applyFill(ex, sym, Side.Long, 110.0, Coin(2.0))
    assertEquals(sizeOf(l), 4.0)
    assertEquals(entryOf(l), 105.0) // (2*100 + 2*110)/4
    assertEquals(l.cash, 10_000.0)

  test("部分平仓: 实现盈亏入现金, 均价不变"):
    val l = empty
      .applyFill(ex, sym, Side.Long, 100.0, Coin(4.0))
      .applyFill(ex, sym, Side.Short, 120.0, Coin(1.0)) // 平 1 个, 盈利 20
    assertEquals(sizeOf(l), 3.0)
    assertEquals(entryOf(l), 100.0)
    assertEquals(l.cash, 10_020.0)

  test("全平多头: 仓位归零, 均价清零, 盈亏入账"):
    val l = empty
      .applyFill(ex, sym, Side.Long, 100.0, Coin(2.0))
      .applyFill(ex, sym, Side.Short, 90.0, Coin(2.0)) // 平 2 个, 亏损 20
    assertEquals(sizeOf(l), 0.0)
    assertEquals(entryOf(l), 0.0)
    assertEquals(l.cash, 9_980.0)

  test("反手: 平掉原仓 + 剩余在成交价反向重开"):
    val l = empty
      .applyFill(ex, sym, Side.Long, 100.0, Coin(2.0))
      .applyFill(ex, sym, Side.Short, 120.0, Coin(5.0)) // 平多 2 (盈利 40), 反手开空 3 @120
    assertEquals(sizeOf(l), -3.0)
    assertEquals(entryOf(l), 120.0)
    assertEquals(l.cash, 10_040.0)

  test("空头平仓盈亏方向正确 (跌价获利)"):
    val l = empty
      .applyFill(ex, sym, Side.Short, 100.0, Coin(2.0))
      .applyFill(ex, sym, Side.Long, 90.0, Coin(2.0)) // 空头平于低价, 盈利 20
    assertEquals(sizeOf(l), 0.0)
    assertEquals(l.cash, 10_020.0)

  test("equity / notional 用未实现盈亏估值"):
    val l = empty.applyFill(ex, sym, Side.Long, 100.0, Coin(2.0))
    val markOf = (_: Symbol) => 150.0
    assertEquals(l.equity(markOf), 10_000.0 + (150.0 - 100.0) * 2.0) // 10100
    assertEquals(l.notional(markOf), 2.0 * 150.0) // 300
    assertEquals(l.openPositions(markOf).head.unrealizedPnl, 100.0)

  test("无估值价格 (mark<=0) 时未实现盈亏记 0"):
    val l = empty.applyFill(ex, sym, Side.Long, 100.0, Coin(2.0))
    assertEquals(l.equity((_: Symbol) => 0.0), 10_000.0)

  test("Matcher.marketable (taker, 乐观): 价格重合即可成交"):
    val bbo = BBO(ex, sym, bidPrice = 100.0, Coin(1), askPrice = 101.0, Coin(1), timestamp = 0)
    assert(!Matcher.marketable(Side.Long, 99.0, bbo))  // 出价 99 < ask 101, 够不着
    assert(Matcher.marketable(Side.Long, 101.0, bbo))  // 出价 == ask, 重合即成交
    assert(Matcher.marketable(Side.Long, 102.0, bbo))  // 出价高过 ask, 成交
    assert(!Matcher.marketable(Side.Short, 102.0, bbo)) // 要价 102 > bid 100, 够不着
    assert(Matcher.marketable(Side.Short, 100.0, bbo))  // 要价 == bid, 重合即成交
    assert(Matcher.marketable(Side.Short, 99.0, bbo))   // 要价低于 bid, 成交

  test("Matcher.crossedByBbo (maker, 悲观): 必须严格穿越, 仅触及不成交"):
    val bbo = BBO(ex, sym, bidPrice = 100.0, Coin(1), askPrice = 101.0, Coin(1), timestamp = 0)
    assert(!Matcher.crossedByBbo(Side.Long, 101.0, bbo)) // ask 恰好触及挂单价 -> 排队未消化, 不成交
    assert(Matcher.crossedByBbo(Side.Long, 101.5, bbo))  // ask 101 跌破挂单价 101.5 -> 成交
    assert(!Matcher.crossedByBbo(Side.Short, 100.0, bbo)) // bid 恰好触及挂单价 -> 不成交
    assert(Matcher.crossedByBbo(Side.Short, 99.5, bbo))   // bid 100 升破挂单价 99.5 -> 成交

  test("Matcher.crossedByTrade (maker, 悲观): 逐笔与 BBO 同一穿越口径"):
    assert(!Matcher.crossedByTrade(Side.Long, 100.0, 100.0)) // 成交价仅触及挂单价 -> 不成交
    assert(Matcher.crossedByTrade(Side.Long, 100.0, 99.9))   // 成交价跌破 -> 成交
    assert(!Matcher.crossedByTrade(Side.Short, 100.0, 100.0))
    assert(Matcher.crossedByTrade(Side.Short, 100.0, 100.1)) // 成交价升破 -> 成交

  test("maker 悲观 / taker 乐观的有意间隙: ask 恰等于挂单价时, 到达单吃单成交、簿上单不成交"):
    val bbo = BBO(ex, sym, bidPrice = 100.0, Coin(1), askPrice = 101.0, Coin(1), timestamp = 0)
    assert(Matcher.marketable(Side.Long, 101.0, bbo))     // 此刻到达 -> 主动吃单
    assert(!Matcher.crossedByBbo(Side.Long, 101.0, bbo))  // 早已在簿 -> 被动排队, 不成交

  test("Matcher.touchPrice: 买单吃卖价, 卖单吃买价"):
    val bbo = BBO(ex, sym, 100.0, Coin(1), 101.0, Coin(1), 0)
    assertEquals(Matcher.touchPrice(Side.Long, bbo), 101.0)
    assertEquals(Matcher.touchPrice(Side.Short, bbo), 100.0)
