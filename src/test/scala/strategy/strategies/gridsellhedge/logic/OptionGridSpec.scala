package strategy.strategies.gridsellhedge.logic

import hft.option.OptionRight
import strategy.strategies.gridsellhedge.logic.OptionGrid.Sell

/** OptionGrid 纯网格逻辑单测: 相邻网格线 + 当前价上下档 bracket。 */
class OptionGridSpec extends munit.FunSuite:
  private val sp = 100.0

  test("nextAbove/nextBelow: 取严格相邻网格线, 线上则跳一格"):
    assertEquals(OptionGrid.nextAbove(2350, sp), 2400.0)
    assertEquals(OptionGrid.nextBelow(2350, sp), 2300.0)
    assertEquals(OptionGrid.nextAbove(2400, sp), 2500.0) // 恰在线上 -> 上一格
    assertEquals(OptionGrid.nextBelow(2400, sp), 2300.0)

  test("bracket: 当前价上档 call + 下档 put"):
    assertEquals(OptionGrid.bracket(2350, sp), Seq(Sell(OptionRight.Call, 2400), Sell(OptionRight.Put, 2300)))
    // 价格漂移到新区间 -> 上下档随之变化
    assertEquals(OptionGrid.bracket(2470, sp), Seq(Sell(OptionRight.Call, 2500), Sell(OptionRight.Put, 2400)))
