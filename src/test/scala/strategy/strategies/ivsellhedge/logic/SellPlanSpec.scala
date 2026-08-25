package strategy.strategies.ivsellhedge.logic

import strategy.utils.option.{OptionHolding, OptionInstrument, OptionRight, Quote}

/** SellPlan 单测：IV 定量的正相关与三个边界、选到期、选行权距离、三道闸门、杠杆率。 */
class SellPlanSpec extends munit.FunSuite:
  private val d = SellPlan.DayMs
  private val cfg = SellPlan.IvQty(ivStart = 0.2, qtyStart = 50, qtySlope = 1, qtyMax = 130).validated

  private def inst(sym: String, exp: Long, strike: Double, right: OptionRight, step: Double = 1.0, min: Double = 1.0) =
    OptionInstrument(sym, exp, strike, right, ctVal = 0.1, minQty = min, qtyStep = step, tickSize = 0.0001)

  // ---------- IV 定量 ----------

  test("IV 低于起卖点 -> 目标 0 (硬门槛)"):
    assertEquals(SellPlan.targetContracts(0.199, cfg), 0.0)
    assertEquals(SellPlan.targetContracts(0.0, cfg), 0.0)

  test("IV 与目标张数正相关: 起卖点=起量, 每 1 个点 +slope"):
    assertEquals(SellPlan.targetContracts(0.20, cfg), 50.0)
    assertEquals(SellPlan.targetContracts(0.30, cfg), 60.0) // +10 个点 -> +10 张
    assertEquals(SellPlan.targetContracts(0.50, cfg), 80.0)

  test("目标张数受上限兜住 (防异常 markVol 一轮天量卖单)"):
    assertEquals(SellPlan.targetContracts(1.0, cfg), 130.0)
    assertEquals(SellPlan.targetContracts(9.99, cfg), 130.0)

  test("恰好落在整点的目标不被 floor 吞掉 (浮点尾差补偿)"):
    val c = SellPlan.IvQty(0.2, 1.0, 1.0, 10.0).validated
    assertEquals(SellPlan.targetContracts(0.2, c), 1.0)
    assertEquals(SellPlan.targetContracts(0.21, c), 2.0)

  test("IV 为 NaN -> 目标 0 (不拿脏数据卖单)"):
    assertEquals(SellPlan.targetContracts(Double.NaN, cfg), 0.0)

  test("参数越界即抛: qtyStart > qtyMax 会让起点被静默改写"):
    intercept[IllegalArgumentException](SellPlan.IvQty(0.2, 200, 1, 130).validated)
    intercept[IllegalArgumentException](SellPlan.IvQty(0.2, 50, 0, 130).validated)  // slope=0 -> 缩放无意义
    intercept[IllegalArgumentException](SellPlan.IvQty(0.2, 50, 1, 0.5).validated)  // 上限 < 1 张

  // ---------- 选到期 ----------

  private val now = 1_700_000_000_000L
  private def chainAt(days: Long*): Seq[OptionInstrument] =
    days.flatMap(dd => Seq(
      inst(s"C$dd", now + dd * d, 3100, OptionRight.Call),
      inst(s"P$dd", now + dd * d, 2900, OptionRight.Put),
    ))

  test("选离 now+targetDays 最近的到期"):
    assertEquals(SellPlan.selectExpiry(chainAt(1, 2, 5, 9), now, 3, d), Some(now + 2 * d))

  test("剩余期限低于 minTtl 的一律排除 (当日到期 gamma 极大)"):
    // 目标 1 天; 0.5 天那档虽然最近, 但低于 minTtl=1天 -> 选 2 天
    assertEquals(SellPlan.selectExpiry(chainAt(2, 4), now + d / 2, 1, d), Some(now + 2 * d))
    assertEquals(SellPlan.selectExpiry(chainAt(0), now, 3, d), None, "全部低于 minTtl -> 无可选")

  test("距离相等取更晚一档 (结果只由日期决定, 不受集合迭代序影响)"):
    // 目标 3 天; 候选 2 天与 4 天等距 -> 取 4 天
    assertEquals(SellPlan.selectExpiry(chainAt(2, 4), now, 3, d), Some(now + 4 * d))
    assertEquals(SellPlan.selectExpiry(chainAt(4, 2), now, 3, d), Some(now + 4 * d))

  // ---------- 选行权 ----------

  private val expiry = now + 3 * d
  private val strikes = Seq(2800.0, 2900.0, 2950.0, 3050.0, 3100.0, 3200.0)
  private val chain = strikes.flatMap(k => Seq(
    inst(s"C$k", expiry, k, OptionRight.Call), inst(s"P$k", expiry, k, OptionRight.Put),
  ))

  test("宽跨两腿恒离现价至少 minDistance, 取满足条件里最近的一档"):
    // spot=3000, 距离 2% -> call >= 3060 最小 = 3100; put <= 2940 最大 = 2900
    val (call, put) = SellPlan.strangleLegs(chain, expiry, 3000.0, 0.02)
    assertEquals(call.map(_.strike), Some(3100.0))
    assertEquals(put.map(_.strike), Some(2900.0))

  test("最近一档不够远则自动往外挪 (不会贴着现价卖)"):
    // 距离 0% 时可以取 3050/2950; 距离 2% 后必须挪到 3100/2900
    assertEquals(SellPlan.strangleLegs(chain, expiry, 3000.0, 0.0)._1.map(_.strike), Some(3050.0))
    assertEquals(SellPlan.strangleLegs(chain, expiry, 3000.0, 0.02)._1.map(_.strike), Some(3100.0))

  test("某侧无满足距离的行权价 -> 该侧 None (单腿裸卖, 调用方须告警)"):
    val (call, put) = SellPlan.strangleLegs(chain, expiry, 3190.0, 0.02) // call 需 >= 3253.8, 无
    assertEquals(call, None)
    assert(put.nonEmpty)

  test("只在指定到期内选腿 (不跨到期拼宽跨)"):
    val mixed = chain ++ Seq(inst("Cother", expiry + d, 3060, OptionRight.Call))
    assertEquals(SellPlan.strangleLegs(mixed, expiry, 3000.0, 0.02)._1.map(_.strike), Some(3100.0))

  // ---------- 单腿决策与闸门 ----------

  private val leg = inst("ETH-USD-C", expiry, 3100, OptionRight.Call)
  private def plan(iv: Double, held: Seq[OptionHolding], q: Quote, minP: Double = 0.0, maxR: Double = 1.1) =
    SellPlan.planLeg(leg, iv, held, cfg, q, minP, maxR)

  test("声明式对账: 目标 - 已持空头 = 补卖量"):
    val r = plan(0.20, Seq(OptionHolding("ETH-USD-C", -20)), Quote(0.05, 0.052))
    assertEquals(r.map(_.contracts), Right(30.0)) // 目标 50 - 已持空 20
    assertEquals(r.map(_.price), Right(0.05))     // 以 bid 卖 (IOC 实收价)

  test("已达目标 -> 不动 (IV 回落只是不再加卖, 不主动平仓)"):
    plan(0.20, Seq(OptionHolding("ETH-USD-C", -60)), Quote(0.05, 0.052)) match
      case Left(SellPlan.Skip.AtTarget(t, held)) => assertEquals((t, held), (50.0, 60.0))
      case other                                  => fail(s"expected AtTarget, got $other")

  test("只看空头张数: 多头持仓不抵扣卖出目标"):
    assertEquals(plan(0.20, Seq(OptionHolding("ETH-USD-C", 30)), Quote(0.05, 0.052)).map(_.contracts), Right(50.0))

  test("别的合约的持仓不算在本腿头上"):
    assertEquals(plan(0.20, Seq(OptionHolding("OTHER", -40)), Quote(0.05, 0.052)).map(_.contracts), Right(50.0))

  test("权利金闸门排在点差之前 (权利金太薄与盘口质量无关)"):
    // 同时违反两道闸门, 报的是权利金
    plan(0.20, Nil, Quote(0.001, 0.01), minP = 0.005, maxR = 1.1) match
      case Left(SellPlan.Skip.PremiumTooLow(bid, min)) => assertEquals((bid, min), (0.001, 0.005))
      case other                                       => fail(s"expected PremiumTooLow, got $other")

  test("点差闸门: ask/bid 超上限不卖 (流动性差不贱卖)"):
    plan(0.20, Nil, Quote(0.05, 0.08)) match
      case Left(SellPlan.Skip.SpreadTooWide(r, max)) => assertEquals(max, 1.1); assert(r > 1.1)
      case other                                     => fail(s"expected SpreadTooWide, got $other")

  test("补差不足最小下单量 -> 不卖 (下轮继续攒)"):
    val big = inst("ETH-USD-C", expiry, 3100, OptionRight.Call, step = 10.0, min = 10.0)
    SellPlan.planLeg(big, 0.20, Seq(OptionHolding("ETH-USD-C", -45)), cfg, Quote(0.05, 0.052), 0.0, 1.1) match
      case Left(SellPlan.Skip.BelowMinQty(q, m)) => assertEquals((q, m), (5.0, 10.0))
      case other                                 => fail(s"expected BelowMinQty, got $other")

  test("补差按 step 向下对齐 (宁少勿多)"):
    val stepped = inst("ETH-USD-C", expiry, 3100, OptionRight.Call, step = 10.0, min = 10.0)
    assertEquals(
      SellPlan.planLeg(stepped, 0.20, Seq(OptionHolding("ETH-USD-C", -23)), cfg, Quote(0.05, 0.052), 0.0, 1.1).map(_.contracts),
      Right(20.0), // 缺口 27 -> 对齐到 20
    )

  test("IV 低于起卖点 -> AtTarget(0) 而非下单"):
    plan(0.10, Nil, Quote(0.05, 0.052)) match
      case Left(SellPlan.Skip.AtTarget(t, _)) => assertEquals(t, 0.0)
      case other                              => fail(s"expected AtTarget(0), got $other")

  // ---------- 杠杆率 ----------

  test("杠杆率按绝对张数计名义 (多空都贡献, 比净敞口更严)"):
    val holdings = Seq(OptionHolding("C3100.0", -10), OptionHolding("P2900.0", 10))
    // Σ|张| = 20, ctVal=0.1, spot=3000 -> 名义 6000; 净值 12000 -> 0.5
    assertEquals(SellPlan.optionLeverage(holdings, chain, 3000.0, 12000.0), 0.5)

  test("净值 <= 0 -> 杠杆率 +∞ (禁止卖出, 而不是给个荒谬的负数)"):
    val h = Seq(OptionHolding("C3100.0", -10))
    assertEquals(SellPlan.optionLeverage(h, chain, 3000.0, 0.0), Double.PositiveInfinity)
    assertEquals(SellPlan.optionLeverage(h, chain, 3000.0, -100.0), Double.PositiveInfinity)

  test("配不上期权链的持仓不计入名义 (算不出 ctVal 就不猜)"):
    assertEquals(SellPlan.optionLeverage(Seq(OptionHolding("UNKNOWN", -10)), chain, 3000.0, 1000.0), 0.0)
