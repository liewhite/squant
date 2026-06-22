package strategy.research

import hft.domain.Side
import hft.indicator.TrendConviction
import strategy.research.TrendRideLogic.{Exec, Params}

class TrendRideLogicSpec extends munit.FunSuite:

  private val p = Params(
    mMax = 10.0, rMax = 3.0, stepQty = 2.0, band = 0.5,
    adverseCap = 0.5, convDead = 0.05, passiveOffset = 0.001, priceTol = 0.002,
  )

  // ===== 测量原语 (TrendConviction) =====

  test("driftScore: σ≤0 或周期≤0 返回 0 (不臆造方向)"):
    assertEquals(TrendConviction.driftScore(0.05, 0.0, 24, 1.5), 0.0)
    assertEquals(TrendConviction.driftScore(0.05, 0.01, 0, 1.5), 0.0)

  test("driftScore: 关于 logRet 单调递增, 且严格落在 (-1,1)"):
    val s1 = TrendConviction.driftScore(0.01, 0.01, 24, 1.5)
    val s2 = TrendConviction.driftScore(0.05, 0.01, 24, 1.5)
    val s3 = TrendConviction.driftScore(0.20, 0.01, 24, 1.5)
    assert(s1 < s2 && s2 < s3)
    assert(s3 < 1.0 && s1 > 0.0)
    assert(TrendConviction.driftScore(-0.20, 0.01, 24, 1.5) > -1.0)

  test("convictionOf: 加权平均并钳到 [-1,1]; 权重全 0 → 0"):
    assertEqualsDouble(TrendConviction.convictionOf(Seq((1.0, 1.0), (1.0, -1.0))), 0.0, 1e-12)
    assertEqualsDouble(TrendConviction.convictionOf(Seq((1.0, 0.4), (3.0, 0.8))), (0.4 + 2.4) / 4.0, 1e-12)
    assertEquals(TrendConviction.convictionOf(Seq((0.0, 0.9))), 0.0)
    assert(TrendConviction.convictionOf(Seq((1.0, 5.0))) == 1.0) // 钳顶

  // ===== 目标仓位：信念安全带 (硬约束 #7) =====

  test("#7 看多 (C>死区) 时目标永不为负, 即使极度超买"):
    for z <- Seq(-5.0, 0.0, 2.0, 50.0) do
      assert(TrendRideLogic.target(0.8, z, p) >= 0.0, s"z=$z")
    // 极度超买把 raw 压到负 → 被 lo=0 钳住
    assertEquals(TrendRideLogic.target(1.0, 100.0, p), 0.0)

  test("#7 看空 (C<−死区) 时目标永不为正"):
    for z <- Seq(-50.0, 0.0, 3.0) do
      assert(TrendRideLogic.target(-0.8, z, p) <= 0.0, s"z=$z")

  test("混乱 (|C|≤死区) 时目标对称且限于 ±rMax: 超买做空、超卖做多"):
    assert(TrendRideLogic.target(0.0, 1.0, p) < 0.0)  // 超买 → 轻空
    assert(TrendRideLogic.target(0.0, -1.0, p) > 0.0) // 超卖 → 轻多
    assert(math.abs(TrendRideLogic.target(0.0, 10.0, p)) <= p.rMax + 1e-9)

  test("满信念无拉伸 → 目标 = ±mMax (重仓)"):
    assertEqualsDouble(TrendRideLogic.target(1.0, 0.0, p), p.mMax, 1e-9)
    assertEqualsDouble(TrendRideLogic.target(-1.0, 0.0, p), -p.mMax, 1e-9)

  // ===== 期望挂单：执行激进度由成因决定 =====

  test("不交易带内 → None"):
    assertEquals(TrendRideLogic.desired(pos = 5.0, target = 5.3, c = 0.8, price = 100.0, p), None)

  test("逆势超限纯强平 (target 同号/为零) → taker 且 reduceOnly=true (只减不增)"):
    val d = TrendRideLogic.desired(pos = 2.0, target = 0.0, c = -0.6, price = 100.0, p).get
    assertEquals(d.exec, Exec.Taker)
    assertEquals(d.side, Side.Short)
    assertEquals(d.reduceOnly, true)

  test("趋势翻向 (target 与 pos 反号) → taker 反手, reduceOnly=false 可穿越"):
    val d = TrendRideLogic.desired(pos = 3.0, target = -2.0, c = -0.7, price = 100.0, p).get
    assertEquals(d.exec, Exec.Taker)
    assertEquals(d.side, Side.Short)
    assertEquals(d.reduceOnly, false)
    assertEqualsDouble(d.qty, 5.0, 1e-9) // 一次吃掉 |target-pos|

  test("顺势加仓 → maker, reduceOnly=false, 单笔截到 stepQty, 买挂现价下方"):
    val d = TrendRideLogic.desired(pos = 0.0, target = 8.0, c = 0.9, price = 100.0, p).get
    assertEquals(d.exec, Exec.Maker)
    assertEquals(d.side, Side.Long)
    assertEquals(d.reduceOnly, false)
    assertEqualsDouble(d.qty, p.stepQty, 1e-9)
    assert(d.price < 100.0)

  test("超买部分止盈 (顺势减仓) → maker, reduceOnly=true, 卖挂现价上方"):
    val d = TrendRideLogic.desired(pos = 8.0, target = 5.0, c = 0.7, price = 100.0, p).get
    assertEquals(d.exec, Exec.Maker)
    assertEquals(d.side, Side.Short)
    assertEquals(d.reduceOnly, true)
    assert(d.price > 100.0)

  test("stretchZ: 超买为正、超卖为负、退化输入为 0"):
    assert(TrendRideLogic.stretchZ(101.0, 100.0, 0.01, 6) > 0.0)
    assert(TrendRideLogic.stretchZ(99.0, 100.0, 0.01, 6) < 0.0)
    assertEquals(TrendRideLogic.stretchZ(101.0, 0.0, 0.01, 6), 0.0)

  // ===== 对账计划 plan：#7 执行层 —— 强平必须与撤单同拍并发, 无哑火窗口 =====

  import strategy.research.TrendRideLogic.{Desired, Resting, plan}
  private val noCancel: hft.domain.OrderId => Boolean = _ => false

  test("#7 逆势 taker + resting maker 在簿 → 同拍既撤 maker 又下市价 (不等撤单确认)"):
    val want = Some(Desired(Side.Short, 100.0, 2.0, reduceOnly = true, Exec.Taker))
    val maker = Resting("m1", Side.Long, reduceOnly = false, isLimit = true, price = 99.9, confirmed = true)
    val pl = plan(want, Vector(maker), noCancel, 0.002)
    assertEquals(pl.cancelIds, Vector("m1"))        // 撤掉在簿 maker
    assertEquals(pl.place.map(_.exec), Some(Exec.Taker)) // 且并发下 taker —— 无哑火窗口

  test("已有市价单在飞 → 抑制重复 taker (但仍撤残留 maker)"):
    val want = Some(Desired(Side.Short, 100.0, 2.0, reduceOnly = true, Exec.Taker))
    val takerInFlight = Resting("t1", Side.Short, reduceOnly = true, isLimit = false, price = 0.0, confirmed = false)
    val maker = Resting("m1", Side.Long, reduceOnly = false, isLimit = true, price = 99.9, confirmed = true)
    val pl = plan(want, Vector(takerInFlight, maker), noCancel, 0.002)
    assertEquals(pl.cancelIds, Vector("m1"))
    assertEquals(pl.place, None) // 不重复下市价单

  test("maker 期望已有匹配挂单 → 不重复挂; 不匹配 → 撤旧补新"):
    val want = Some(Desired(Side.Long, 99.92, 1.5, reduceOnly = false, Exec.Maker))
    val matched = Resting("a", Side.Long, reduceOnly = false, isLimit = true, price = 99.90, confirmed = true)
    assertEquals(plan(want, Vector(matched), noCancel, 0.002).place, None)
    val stale = Resting("b", Side.Long, reduceOnly = false, isLimit = true, price = 95.0, confirmed = true)
    val pl = plan(want, Vector(stale), noCancel, 0.002)
    assertEquals(pl.cancelIds, Vector("b"))
    assertEquals(pl.place.map(_.exec), Some(Exec.Maker))

  test("未确认 (Created) 挂单不撤 (终态以私有流为准); 已在撤的不重复撤"):
    val want: Option[Desired] = None
    val created = Resting("c", Side.Long, reduceOnly = false, isLimit = true, price = 99.0, confirmed = false)
    assertEquals(plan(want, Vector(created), noCancel, 0.002).cancelIds, Vector.empty)
    val confirmed = Resting("d", Side.Long, reduceOnly = false, isLimit = true, price = 99.0, confirmed = true)
    assertEquals(plan(want, Vector(confirmed), id => id == "d", 0.002).cancelIds, Vector.empty)
