package voltrade

import hft.domain.Side

class PerpHedgerSpec extends munit.FunSuite:
  private val p = PerpHedger.Params(offsetPct = 0.01, requoteMs = 5000, minHedge = 0.01, tightAtr = 1.0, looseAtr = 2.0)

  test("首轮: center=NaN -> 初始化为现价, Hold"):
    val (a, st) = PerpHedger.decide(mid = 100, atr = 1, maBias = 1, netDelta = 0.5, nowMs = 0, st = PerpHedger.State(), p)
    assertEquals(a, PerpHedger.Action.Hold)
    assertEquals(st.center, 100.0)

  test("均线上 上行越紧带(1ATR) 净多 -> 挂被动卖, 价=mid*(1+offset)"):
    val st0 = PerpHedger.State(center = 100)
    val (a, _) = PerpHedger.decide(mid = 102, atr = 1, maBias = 1, netDelta = 0.5, nowMs = 0, st = st0, p) // dev+2 > 1ATR
    a match
      case PerpHedger.Action.Place(side, price, qty) =>
        assertEquals(side, Side.Short); assert(math.abs(price - 102 * 1.01) < 1e-6); assert(math.abs(qty - 0.5) < 1e-9)
      case other => fail(s"expected Place, got $other")

  test("均线上 下行松带(2ATR): 回落1.5ATR 未越带 -> Hold"):
    val st0 = PerpHedger.State(center = 100)
    assertEquals(PerpHedger.decide(98.5, 1, 1, -0.5, 0, st0, p)._1, PerpHedger.Action.Hold) // 回落1.5 < 2ATR

  test("净 delta 小于 minHedge -> Hold"):
    val st0 = PerpHedger.State(center = 100)
    assertEquals(PerpHedger.decide(104, 1, 1, 0.005, 0, st0, p)._1, PerpHedger.Action.Hold)

  test("已有挂单: 超 requote 撤单, 未超 Hold"):
    val st0 = PerpHedger.State(center = 100, restingId = Some("o1"), restingAt = 1000)
    assertEquals(PerpHedger.decide(110, 1, 1, 1.0, 1000 + 6000, st0, p)._1, PerpHedger.Action.Cancel("o1")) // 6s>5s
    assertEquals(PerpHedger.decide(110, 1, 1, 1.0, 1000 + 2000, st0, p)._1, PerpHedger.Action.Hold)          // 2s<5s

  test("ATR 未就绪 (<=0) -> Hold"):
    assertEquals(PerpHedger.decide(110, 0.0, 1, 1.0, 0, PerpHedger.State(center = 100), p)._1, PerpHedger.Action.Hold)

  test("均线下: 下行紧带(1ATR) 净空 -> 挂被动买"):
    val st0 = PerpHedger.State(center = 100)
    PerpHedger.decide(98, 1, -1, -0.5, 0, st0, p)._1 match // 均线下 down=tight=1ATR, 回落2>1 -> 触发
      case PerpHedger.Action.Place(side, price, _) =>
        assertEquals(side, Side.Long); assert(math.abs(price - 98 * 0.99) < 1e-6)
      case other => fail(s"expected Place Long, got $other")
