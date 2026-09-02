package strategy.strategies.volsell.logic
import strategy.utils.option.*

import java.time.{DayOfWeek, Instant, ZoneId, ZonedDateTime}

class SellVolPlanSpec extends munit.FunSuite:
  private def near(a: Double, b: Double, eps: Double = 1e-9): Unit = assert(math.abs(a - b) < eps, s"$a vs $b")

  test("annualizedRv: 恒定对数收益 r -> |r|·sqrt(barsPerYear5m)"):
    val r = 0.001
    val closes = (0 until 50).map(i => 1000.0 * math.exp(r * i))
    near(SellVolPlan.annualizedRv(closes).get, r * math.sqrt(SellVolPlan.BarsPerYear5m), 1e-6)

  test("decideMultiplier: 本周(后半)波动升 -> gridHigh; 降 -> gridLow"):
    val flat = Vector.fill(10)(100.0)
    val choppy = (0 until 10).map(i => if i % 2 == 0 then 100.0 else 103.0).toVector
    assertEquals(SellVolPlan.decideMultiplier(flat ++ choppy, 2.0, 1.0).get.mult, 2.0) // 后半更波动 -> 2x
    assertEquals(SellVolPlan.decideMultiplier(choppy ++ flat, 2.0, 1.0).get.mult, 1.0) // 后半更平 -> 1x

  test("decideMultiplier: 样本不足 -> None, 不凭空给出方向判断"):
    // 从前返回 gridLow, 等于把"不知道波动是升是降"说成"在降", 而它决定卖出多少份
    assertEquals(SellVolPlan.decideMultiplier(Vector(100.0), 2.0, 1.0), None)
    assertEquals(SellVolPlan.decideMultiplier(Vector(100.0, 101.0, 102.0), 2.0, 1.0), None)

  test("selectStrangle: 离目标到期最近的到期 + 贴近现价两侧的价外 call/put"):
    val d = 86_400_000L
    val chain = Vector(
      OptionInstrument("ETH-A-3000-C", 10 * d, 3000, OptionRight.Call, ctVal = 1.0, minQty = 1.0, qtyStep = 1.0, tickSize = 0.1), // 错误到期
      OptionInstrument("ETH-B-2900-P", 21 * d, 2900, OptionRight.Put, ctVal = 1.0, minQty = 1.0, qtyStep = 1.0, tickSize = 0.1), // <spot 较远
      OptionInstrument("ETH-B-3000-P", 21 * d, 3000, OptionRight.Put, ctVal = 1.0, minQty = 1.0, qtyStep = 1.0, tickSize = 0.1), // <spot 最近 -> 选
      OptionInstrument("ETH-B-3100-C", 21 * d, 3100, OptionRight.Call, ctVal = 1.0, minQty = 1.0, qtyStep = 1.0, tickSize = 0.1), // >spot 最近 -> 选
      OptionInstrument("ETH-B-3200-C", 21 * d, 3200, OptionRight.Call, ctVal = 1.0, minQty = 1.0, qtyStep = 1.0, tickSize = 0.1), // >spot 较远
    )
    val res = SellVolPlan.selectStrangle(chain, nowMs = 0, spot = 3060, targetExpiryMs = 21 * d)
    assertEquals(res.map((c, p) => (c.symbol, p.symbol)), Some(("ETH-B-3100-C", "ETH-B-3000-P")))
    // spot 恰在行权上 -> 严格价外 (跳过等于 spot 的行权)
    val onStrike = SellVolPlan.selectStrangle(chain, 0, 3000, 21 * d) // put 须严格 <3000 -> 2900
    assertEquals(onStrike.map((c, p) => (c.strike, p.strike)), Some((3100.0, 2900.0)))
    // 某侧无价外行权 -> 无宽跨
    assertEquals(SellVolPlan.selectStrangle(chain.filterNot(_.right == OptionRight.Put), 0, 3060, 21 * d), None)

  test("sellQuote: 相对价差 ≤ cutoffRatio -> 对手价 taker; 否则中价让价 maker 并对齐 tick"):
    val tick = 0.1
    // 相对价差 0.1/49.95 ≈ 0.2% < 0.75% -> taker
    assertEquals(SellVolPlan.sellQuote(Quote(bid = 49.9, ask = 50.0), tick), (49.9, false))
    // 相对价差 1.0/39.5 ≈ 2.5% > 0.75% -> maker: 39.5×(1-0.00375)=39.3519, 向上对齐到 0.1 -> 39.4
    near(SellVolPlan.sellQuote(Quote(bid = 39.0, ask = 40.0), tick)._1, 39.4)

  test("sellQuote: maker 价必须严格高于买一 —— 否则 postOnly 单被交易所拒"):
    // 从前 cutoff=0.75%、offset=0.5% 破了 cutoff >= 2×offset: 相对价差落在 0.75%~1.0% 时
    // maker 分支报出低于买一的价, 这一档价差下永远挂不上单, 而唯一症状是"没有挂单"。
    val tick = 0.01
    // 相对价差恰好 0.8% (在原来的破窗里): mid=100.4/2... 取 bid=100.0 ask=100.8 -> mid=100.4, spread=0.8
    val inOldGap = Quote(bid = 100.0, ask = 100.8)
    val (px, postOnly) = SellVolPlan.sellQuote(inOldGap, tick)
    assert(postOnly, "0.8% > 0.75% -> maker")
    assert(px > inOldGap.bid, s"maker 卖价 $px 必须高于买一 ${inOldGap.bid}")

  test("sellQuote: cutoff < 2×offset 即拒 —— 那组参数注定造出必被拒的价"):
    intercept[IllegalArgumentException] {
      SellVolPlan.sellQuote(Quote(bid = 39.0, ask = 40.0), 0.1, cutoffRatio = 0.0075, offsetRatio = 0.005)
    }

  test("sellQuote: 判据是**相对**价差 —— 换报价货币不改变行为"):
    // 从前 cutoff 是绝对值 0.3: OKX 币本位期权 px 以 ETH 计 (0.005~0.1), 价差恒 <= 0.3
    // -> 永远走 taker; 而 maker 分支的 mid-0.2 还会是负数。比例形式在两家都成立。
    val usdtLike = Quote(bid = 39.0, ask = 40.0)   // 相对价差 2.5%
    val coinLike = Quote(bid = 0.039, ask = 0.040) // 同样是 2.5%
    assertEquals(SellVolPlan.sellQuote(usdtLike, 0.1)._2, true, "宽价差 -> maker")
    assertEquals(SellVolPlan.sellQuote(coinLike, 0.0001)._2, true, "同样的相对价差, 同样的结论")
    assert(SellVolPlan.sellQuote(coinLike, 0.0001)._1 > 0.0, "让价后仍须为正 —— 绝对偏移会算成负数")

  test("sellQuote: tick 非正即拒 (maker 单落在网格外会被交易所拒)"):
    intercept[IllegalArgumentException](SellVolPlan.sellQuote(Quote(bid = 39.0, ask = 40.0), 0.0))

  test("quantizeQty: 向下取整到 step 并校验 minQty"):
    assertEquals(SellVolPlan.quantizeQty(1.0, 0.1, 0.1), Some(1.0))
    assertEquals(SellVolPlan.quantizeQty(2.0, 0.1, 0.1), Some(2.0))
    assertEquals(SellVolPlan.quantizeQty(0.05, 0.1, 0.1), None)        // < min
    assertEquals(SellVolPlan.quantizeQty(0.25, 0.1, 0.1).map(r => math.round(r * 100) / 100.0), Some(0.2)) // floor 到 step

  test("quantizeQty: 精度非正即拒 —— 校验不了下单量的合约不该可交易"):
    // 从前 step<=0 时**跳过对齐**、minQty=0 放过任何量, 于是交易所报文里一个坏掉的精度字段
    // 就能让下单量校验整体失效。两家期权客户端现在都拒绝精度非正的合约, 所以这里是前置条件。
    intercept[IllegalArgumentException](SellVolPlan.quantizeQty(1.0, 0.0, 0.1))
    intercept[IllegalArgumentException](SellVolPlan.quantizeQty(1.0, 0.1, 0.0))

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
