package strategy.strategies.macdgrid.logic

import hft.domain.Side
import strategy.strategies.macdgrid.logic.MacdGridLogic.{Decision, OrderSpec, Params, decide}

/** MacdGridLogic 纯决策单测：方向(DEA)/加仓减仓(柱)/超买超卖(MA20) 三闸门 + 滚动单份括号, 多空对称。 */
class MacdGridLogicSpec extends munit.FunSuite:
  private val p = Params() // add1/close2/tight1/stop5/tight8/max5
  // 默认: ma20=100, atr=1, 现价=锚价=100 (不拉伸), unit=2币
  private def d(
      dea: Double, bar: Double, posUnits: Int,
      mark: Double = 100.0, level: Double = 100.0, ma20: Double = 100.0, atr: Double = 1.0, unit: Double = 2.0,
  ): Decision = decide(dea, bar, mark, level, ma20, atr, posUnits, unit, p)

  test("未就绪: atr<=0 或 unit<=0 -> 不动作"):
    assertEquals(d(1.0, 1.0, 0, atr = 0.0), Decision(false, None, None))
    assertEquals(d(1.0, 1.0, 0, unit = 0.0), Decision(false, None, None))

  test("DEA 转向: 持多遇DEA水下 / 持空遇DEA水上 -> 立刻市价全平"):
    assertEquals(d(-0.1, 1.0, posUnits = 3), Decision(true, None, None))  // 持多, DEA水下
    assertEquals(d(0.1, -1.0, posUnits = -3), Decision(true, None, None)) // 持空, DEA水上

  test("做多开仓: DEA水上+柱水上+平仓态 -> 下方1ATR挂一份买, 无止盈单"):
    val r = d(0.5, 0.5, posUnits = 0)
    assertEquals(r.flatten, false)
    assertEquals(r.add, Some(OrderSpec(Side.Long, 99.0, 2.0, reduceOnly = false))) // 100 - 1*1
    assertEquals(r.close, None) // 平仓态无持仓 -> 无止盈

  test("做多持仓: 加仓单(下1ATR买) + 止盈单(上2ATR卖, reduceOnly)"):
    val r = d(0.5, 0.5, posUnits = 2)
    assertEquals(r.add, Some(OrderSpec(Side.Long, 99.0, 2.0, false)))
    assertEquals(r.close, Some(OrderSpec(Side.Short, 102.0, 2.0, true))) // 100 + 2*1

  test("做多加仓闸门: 柱水下 -> 不加仓, 止盈收紧到1ATR 且转追价(trail, 用现价)"):
    val r = d(0.5, -0.5, posUnits = 2) // DEA水上但柱水下 -> 单侧挂单, 被动平仓
    assertEquals(r.add, None)
    assertEquals(r.close, Some(OrderSpec(Side.Short, 101.0, 2.0, true, trail = true))) // 现价100 + 1*1, 追价

  test("做多加仓闸门: 已满5份 -> 不再加仓, 止盈照挂(单侧->追价)"):
    val r = d(0.5, 0.5, posUnits = 5)
    assertEquals(r.add, None)
    assertEquals(r.close, Some(OrderSpec(Side.Short, 102.0, 2.0, true, trail = true)))

  test("MA20 超买: 现价高于MA20超过5ATR -> 停止加多, 止盈2ATR 追价(用现价)"):
    val r = d(0.5, 0.5, posUnits = 2, mark = 106.0, ma20 = 100.0, atr = 1.0) // stretch=6 (>5,<=8)
    assertEquals(r.add, None)
    assertEquals(r.close, Some(OrderSpec(Side.Short, 108.0, 2.0, true, trail = true))) // 追价: 现价106 + 2

  test("MA20 极度超买: 超过8ATR -> 停止加多 且 止盈收紧1ATR 追价"):
    val r = d(0.5, 0.5, posUnits = 2, mark = 109.0, ma20 = 100.0, atr = 1.0) // stretch=9 (>8)
    assertEquals(r.add, None)
    assertEquals(r.close, Some(OrderSpec(Side.Short, 110.0, 2.0, true, trail = true))) // 追价: 现价109 + 1

  test("做空镜像: DEA水下+柱水下 -> 上方1ATR挂卖加空 + 下方2ATR买止盈(静态)"):
    val r = d(-0.5, -0.5, posUnits = -2)
    assertEquals(r.add, Some(OrderSpec(Side.Short, 101.0, 2.0, false)))           // 100 + 1
    assertEquals(r.close, Some(OrderSpec(Side.Long, 98.0, 2.0, true, trail = false))) // 加仓中 -> 静态锚价 100-2

  test("做空闸门: 柱水上 -> 不加空 + 止盈收紧 追价; 超卖>5ATR停止加空"):
    val r1 = d(-0.5, 0.5, posUnits = -2) // 柱水上(against short) -> 单侧, 追价
    assertEquals(r1.add, None)
    assertEquals(r1.close, Some(OrderSpec(Side.Long, 99.0, 2.0, true, trail = true))) // 收紧 现价100-1
    val r2 = d(-0.5, -0.5, posUnits = -2, mark = 94.0, ma20 = 100.0) // 超卖=6 (>5)
    assertEquals(r2.add, None)

  test("DEA 恰在零轴 + 持多: 不平不加, 仅保留止盈 追价 (无明确方向)"):
    val r = d(0.0, 0.5, posUnits = 2)
    assertEquals(r.flatten, false)
    assertEquals(r.add, None)                                          // dea 非>0, 不加
    assertEquals(r.close, Some(OrderSpec(Side.Short, 102.0, 2.0, true, trail = true))) // 单侧 -> 追价(现价100+2)

  test("被动平仓追价: 单侧挂单(无加仓单)时止盈单 trail=true 且价用现价; 加仓态则静态用锚价"):
    // 加仓态 (柱水上/未超买): close 静态, 用锚价 level=98 -> 98+2=100, trail=false
    val active = d(0.5, 0.5, posUnits = 2, mark = 102.0, level = 98.0)
    assertEquals(active.add, Some(OrderSpec(Side.Long, 97.0, 2.0, false)))            // 锚价 98-1
    assertEquals(active.close, Some(OrderSpec(Side.Short, 100.0, 2.0, true, trail = false))) // 锚价 98+2
    // 被动态 (柱水下): close 追价, 用现价 mark=102 -> 102+1(收紧)=103, trail=true, 不再受锚价影响
    val passive = d(0.5, -0.5, posUnits = 2, mark = 102.0, level = 98.0)
    assertEquals(passive.add, None)
    assertEquals(passive.close, Some(OrderSpec(Side.Short, 103.0, 2.0, true, trail = true)))

  test("锚价≠现价: 挂单价用锚价算, 超买度用现价算"):
    // 锚价98 (上次成交), 现价102 (已涨), ma20=100, atr=1 -> stretchAbove=2 (不停加)
    val r = d(0.5, 0.5, posUnits = 2, mark = 102.0, level = 98.0)
    assertEquals(r.add, Some(OrderSpec(Side.Long, 97.0, 2.0, false)))   // 98 - 1
    assertEquals(r.close, Some(OrderSpec(Side.Short, 100.0, 2.0, true))) // 98 + 2
