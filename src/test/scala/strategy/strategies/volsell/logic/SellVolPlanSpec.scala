package strategy.strategies.volsell.logic
import strategy.utils.option.*

import java.time.{DayOfWeek, Instant, ZoneId, ZonedDateTime}

class SellVolPlanSpec extends munit.FunSuite:
  private def near(a: Double, b: Double, eps: Double = 1e-9): Unit = assert(math.abs(a - b) < eps, s"$a vs $b")

  test("annualizedRv: 恒定对数收益 r -> |r|·sqrt(barsPerYear5m)"):
    val r = 0.001
    val closes = (0 until 50).map(i => 1000.0 * math.exp(r * i))
    near(SellVolPlan.annualizedRv(closes), r * math.sqrt(SellVolPlan.BarsPerYear5m), 1e-6)

  test("decideMultiplier: 本周(后半)波动升 -> gridHigh; 降 -> gridLow"):
    val flat = Vector.fill(10)(100.0)
    val choppy = (0 until 10).map(i => if i % 2 == 0 then 100.0 else 103.0).toVector
    assertEquals(SellVolPlan.decideMultiplier(flat ++ choppy, 2.0, 1.0)._1, 2.0) // 后半更波动 -> 2x
    assertEquals(SellVolPlan.decideMultiplier(choppy ++ flat, 2.0, 1.0)._1, 1.0) // 后半更平 -> 1x
    assertEquals(SellVolPlan.decideMultiplier(Vector(100.0), 2.0, 1.0)._1, 1.0)  // 样本不足 -> 1x

  test("selectStrangle: 离目标到期最近的到期 + 贴近现价两侧的价外 call/put"):
    val d = 86_400_000L
    val chain = Vector(
      OptionInstrument("ETH-A-3000-C", 10 * d, 3000, OptionRight.Call), // 错误到期
      OptionInstrument("ETH-B-2900-P", 21 * d, 2900, OptionRight.Put),  // <spot 较远
      OptionInstrument("ETH-B-3000-P", 21 * d, 3000, OptionRight.Put),  // <spot 最近 -> 选
      OptionInstrument("ETH-B-3100-C", 21 * d, 3100, OptionRight.Call), // >spot 最近 -> 选
      OptionInstrument("ETH-B-3200-C", 21 * d, 3200, OptionRight.Call), // >spot 较远
    )
    val res = SellVolPlan.selectStrangle(chain, nowMs = 0, spot = 3060, targetExpiryMs = 21 * d)
    assertEquals(res.map((c, p) => (c.symbol, p.symbol)), Some(("ETH-B-3100-C", "ETH-B-3000-P")))
    // spot 恰在行权上 -> 严格价外 (跳过等于 spot 的行权)
    val onStrike = SellVolPlan.selectStrangle(chain, 0, 3000, 21 * d) // put 须严格 <3000 -> 2900
    assertEquals(onStrike.map((c, p) => (c.strike, p.strike)), Some((3100.0, 2900.0)))
    // 某侧无价外行权 -> 无宽跨
    assertEquals(SellVolPlan.selectStrangle(chain.filterNot(_.right == OptionRight.Put), 0, 3060, 21 * d), None)

  test("sellQuote: 价差≤0.3 -> 对手价(买一) taker; >0.3 -> 中价-0.2 maker"):
    assertEquals(SellVolPlan.sellQuote(Quote(bid = 49.9, ask = 50.0)), (49.9, false)) // 价差0.1
    assertEquals(SellVolPlan.sellQuote(Quote(bid = 10.0, ask = 10.2)), (10.0, false)) // 价差0.2 -> taker
    near(SellVolPlan.sellQuote(Quote(bid = 39.0, ask = 40.0))._1, 39.3)               // 价差1.0 -> 中价39.5-0.2
    assertEquals(SellVolPlan.sellQuote(Quote(bid = 39.0, ask = 40.0))._2, true)        // maker

  test("quantizeQty: 向下取整到 step 并校验 minQty"):
    assertEquals(SellVolPlan.quantizeQty(1.0, 0.1, 0.1), Some(1.0))
    assertEquals(SellVolPlan.quantizeQty(2.0, 0.1, 0.1), Some(2.0))
    assertEquals(SellVolPlan.quantizeQty(0.05, 0.1, 0.1), None)        // < min
    assertEquals(SellVolPlan.quantizeQty(0.25, 0.1, 0.1).map(r => math.round(r * 100) / 100.0), Some(0.2)) // floor 到 step
    assertEquals(SellVolPlan.quantizeQty(1.0, 0.0, 0.1), Some(1.0))    // 无 step
    assertEquals(SellVolPlan.quantizeQty(0.05, 0.0, 0.1), None)

  test("currentDecisionTime: 当下或之前最近的北京周五17:00, 且不晚于 now、距今<7天"):
    val zone = ZoneId.of("Asia/Shanghai")
    Seq("2025-06-21T00:00:00Z", "2025-06-20T10:00:00Z", "2025-06-23T00:00:00Z").foreach { s =>
      val now = Instant.parse(s).toEpochMilli
      val anchor = SellVolPlan.currentDecisionTime(now)
      val z = ZonedDateTime.ofInstant(Instant.ofEpochMilli(anchor), zone)
      assertEquals(z.getDayOfWeek, DayOfWeek.FRIDAY, s); assertEquals(z.getHour, 17, s)
      assert(anchor <= now && now - anchor < 7L * 86_400_000L, s)
    }

  test("nextDecisionTime: 永远是北京周五17:00 且在 from 之后"):
    val zone = ZoneId.of("Asia/Shanghai")
    Seq("2025-06-16T00:00:00Z", "2025-06-20T08:59:00Z", "2025-06-20T10:00:00Z", "2025-06-21T00:00:00Z").foreach { s =>
      val from = Instant.parse(s).toEpochMilli
      val next = SellVolPlan.nextDecisionTime(from)
      val z = ZonedDateTime.ofInstant(Instant.ofEpochMilli(next), zone)
      assertEquals(z.getDayOfWeek, DayOfWeek.FRIDAY, s)
      assertEquals(z.getHour, 17, s); assertEquals(z.getMinute, 0, s)
      assert(next > from, s"next $next not after $from")
    }

  test("targetExpiryMs: = 锚点 + targetDays, 锚点在周五 -> 仍落周五"):
    val zone = ZoneId.of("Asia/Shanghai")
    val anchor = SellVolPlan.currentDecisionTime(Instant.parse("2025-06-20T10:00:00Z").toEpochMilli) // 周五17:00
    val target = SellVolPlan.targetExpiryMs(anchor, 21)
    assertEquals(target - anchor, 21L * SellVolPlan.DayMs)
    assertEquals(ZonedDateTime.ofInstant(Instant.ofEpochMilli(target), zone).getDayOfWeek, DayOfWeek.FRIDAY) // 21=3×7

  test("decisionAnchor: runNow=上周五(lastDecisionTime), 常规=本周五; 周五当天二者差 7 天"):
    val zone = ZoneId.of("Asia/Shanghai")
    val fri = Instant.parse("2025-06-20T10:00:00Z").toEpochMilli // 北京周五 18:00 (>17:00)
    val regular = SellVolPlan.decisionAnchor(fri, runNow = false)
    val last = SellVolPlan.decisionAnchor(fri, runNow = true)
    assertEquals(regular, SellVolPlan.currentDecisionTime(fri))
    assertEquals(last, SellVolPlan.lastDecisionTime(fri))
    assertEquals(regular - last, 7L * SellVolPlan.DayMs) // 周五当天: 本周五 vs 上周五
    val z = ZonedDateTime.ofInstant(Instant.ofEpochMilli(last), zone)
    assertEquals(z.getDayOfWeek, DayOfWeek.FRIDAY); assertEquals(z.getHour, 17)

  test("decisionAnchor: 周中 runNow 与常规一致 (都=上周五)"):
    val wed = Instant.parse("2025-06-18T00:00:00Z").toEpochMilli // 周三
    assertEquals(SellVolPlan.decisionAnchor(wed, runNow = true), SellVolPlan.decisionAnchor(wed, runNow = false))
