package voltrade

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

  test("selectStraddle: 选 ~21天到期 + ATM 行权的 call/put"):
    val d = 86_400_000L
    val chain = Vector(
      OptionInstrument("ETH-A-2900-C", 10 * d, 2900, OptionRight.Call),
      OptionInstrument("ETH-A-3000-C", 10 * d, 3000, OptionRight.Call),
      OptionInstrument("ETH-B-2900-C", 21 * d, 2900, OptionRight.Call),
      OptionInstrument("ETH-B-3000-C", 21 * d, 3000, OptionRight.Call),
      OptionInstrument("ETH-B-3000-P", 21 * d, 3000, OptionRight.Put),
      OptionInstrument("ETH-B-3100-C", 21 * d, 3100, OptionRight.Call),
    )
    val res = SellVolPlan.selectStraddle(chain, nowMs = 0, spot = 2990, targetDays = 21)
    assertEquals(res.map((c, p) => (c.symbol, p.symbol)), Some(("ETH-B-3000-C", "ETH-B-3000-P"))) // 21天到期, ATM=3000
    // 缺 put 则无跨式
    assertEquals(SellVolPlan.selectStraddle(chain.filterNot(_.right == OptionRight.Put), 0, 2990, 21), None)

  test("quantizeQty: 向下取整到 step 并校验 minQty"):
    assertEquals(SellVolPlan.quantizeQty(1.0, 0.1, 0.1), Some(1.0))
    assertEquals(SellVolPlan.quantizeQty(2.0, 0.1, 0.1), Some(2.0))
    assertEquals(SellVolPlan.quantizeQty(0.05, 0.1, 0.1), None)        // < min
    assertEquals(SellVolPlan.quantizeQty(0.25, 0.1, 0.1).map(r => math.round(r * 100) / 100.0), Some(0.2)) // floor 到 step
    assertEquals(SellVolPlan.quantizeQty(1.0, 0.0, 0.1), Some(1.0))    // 无 step
    assertEquals(SellVolPlan.quantizeQty(0.05, 0.0, 0.1), None)

  test("currentDecisionTime: 当下或之前最近的北京周五15:00, 且不晚于 now、距今<7天"):
    val zone = ZoneId.of("Asia/Shanghai")
    Seq("2025-06-21T00:00:00Z", "2025-06-20T08:00:00Z", "2025-06-23T00:00:00Z").foreach { s =>
      val now = Instant.parse(s).toEpochMilli
      val anchor = SellVolPlan.currentDecisionTime(now)
      val z = ZonedDateTime.ofInstant(Instant.ofEpochMilli(anchor), zone)
      assertEquals(z.getDayOfWeek, DayOfWeek.FRIDAY, s); assertEquals(z.getHour, 15, s)
      assert(anchor <= now && now - anchor < 7L * 86_400_000L, s)
    }

  test("nextDecisionTime: 永远是北京周五15:00 且在 from 之后"):
    val zone = ZoneId.of("Asia/Shanghai")
    Seq("2025-06-16T00:00:00Z", "2025-06-20T06:59:00Z", "2025-06-20T08:00:00Z", "2025-06-21T00:00:00Z").foreach { s =>
      val from = Instant.parse(s).toEpochMilli
      val next = SellVolPlan.nextDecisionTime(from)
      val z = ZonedDateTime.ofInstant(Instant.ofEpochMilli(next), zone)
      assertEquals(z.getDayOfWeek, DayOfWeek.FRIDAY, s)
      assertEquals(z.getHour, 15, s); assertEquals(z.getMinute, 0, s)
      assert(next > from, s"next $next not after $from")
    }
