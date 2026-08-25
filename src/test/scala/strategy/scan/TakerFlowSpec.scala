package strategy.scan

/** 单标的 taker 流向窗口与稳健基线的纯单测：无引擎、无时钟、无 IO，时间全部手喂。 */
class TakerFlowSpec extends munit.FunSuite:

  private def flowOf(bucketMs: Long = 1000, window: Int = 3, baseline: Int = 10) =
    SymbolTakerFlow(bucketMs, window, baseline)

  test("taker 买卖分别累计, 净流向 = 买 - 卖"):
    val f = flowOf()
    f.onTrade(0, 100.0, takerBuy = true)
    f.onTrade(0, 30.0, takerBuy = false)
    assertEquals(f.windowFlow, 70.0)
    assertEquals(f.windowNotional, 130.0)

  test("滚出窗口的桶被扣掉 (窗口 3 桶, 第 4 桶时首桶应出局)"):
    val f = flowOf(window = 3)
    f.onTrade(0, 100.0, takerBuy = true)     // 桶 0
    f.onTrade(1000, 10.0, takerBuy = true)   // 桶 1
    f.onTrade(2000, 1.0, takerBuy = true)    // 桶 2
    assertEquals(f.windowFlow, 111.0)
    f.onTrade(3000, 0.5, takerBuy = true)    // 桶 3 -> 桶 0 出局
    assertEquals(f.windowFlow, 11.5)

  test("空桶清零: 静默一段时间后窗口归零, 不留上一轮的量"):
    val f = flowOf(window = 3)
    f.onTrade(0, 100.0, takerBuy = true)
    f.advanceTo(10_000) // 跨度远超窗口
    assertEquals(f.windowFlow, 0.0)
    assertEquals(f.windowNotional, 0.0)

  test("跨度超过一个窗口不逐桶空转, 结果与逐桶推进一致"):
    val a = flowOf(window = 3)
    val b = flowOf(window = 3)
    a.onTrade(0, 100.0, takerBuy = true)
    b.onTrade(0, 100.0, takerBuy = true)
    a.advanceTo(1_000_000)                                   // 一步跨过去
    (1 to 1000).foreach(i => b.advanceTo(i.toLong * 1000))   // 逐桶走
    assertEquals(a.windowFlow, b.windowFlow)
    assertEquals(a.windowNotional, b.windowNotional)

  test("基线样本不足 -> 无 z 分 (冷启动宁可不报)"):
    val f = flowOf(baseline = 10)
    f.onTrade(0, 100.0, takerBuy = true)
    f.advanceTo(3000)
    assertEquals(f.baselineSize, 3)
    assertEquals(f.zScore, None)

  test("平稳流量 -> z 分接近 0; 突发单向爆量 -> z 分显著为正"):
    val f = flowOf(window = 3, baseline = 20)
    // 20+ 个桶的温和双向流量, 净流向在 0 附近小幅波动
    var t = 0L
    (0 until 30).foreach { i =>
      f.onTrade(t, 100.0, takerBuy = true)
      f.onTrade(t, if i % 2 == 0 then 98.0 else 102.0, takerBuy = false)
      t += 1000
    }
    val calm = f.zScore.get
    assert(math.abs(calm) < 3.0, s"平稳期 z 分不该大, got $calm")
    // 一记大额主动买
    f.onTrade(t, 100_000.0, takerBuy = true)
    val spike = f.zScore.get
    assert(spike > 10.0, s"爆量后 z 分应显著为正, got $spike")

  test("方向相反的爆量 -> z 分显著为负"):
    val f = flowOf(window = 3, baseline = 20)
    var t = 0L
    (0 until 30).foreach { _ =>
      f.onTrade(t, 100.0, takerBuy = true)
      f.onTrade(t, 100.0, takerBuy = false)
      t += 1000
    }
    f.onTrade(t, 100_000.0, takerBuy = false)
    assert(f.zScore.get < -10.0, s"卖爆应为负 z, got ${f.zScore.get}")

  test("中位数与 MAD: 对少数极端值不敏感 (这正是不用均值/标准差的理由)"):
    val calm = Array(1.0, 2.0, 3.0, 4.0, 5.0)
    val withSpike = Array(1.0, 2.0, 3.0, 4.0, 5.0, 1000.0)
    assertEquals(TakerFlowStats.median(calm), 3.0)
    // 加入一个极端值后中位数只从 3.0 移到 3.5, 而均值会从 3.0 跳到 169
    assertEquals(TakerFlowStats.median(withSpike), 3.5)
    assert(TakerFlowStats.mad(withSpike, 3.5) <= 2.0, "MAD 不该被单个极端值撑大")

  test("median 不改动调用方数组"):
    val xs = Array(3.0, 1.0, 2.0)
    TakerFlowStats.median(xs)
    assertEquals(xs.toList, List(3.0, 1.0, 2.0))

  test("非法参数装配期即拒"):
    intercept[IllegalArgumentException](SymbolTakerFlow(0, 3, 10))
    intercept[IllegalArgumentException](SymbolTakerFlow(1000, 0, 10))
    intercept[IllegalArgumentException](SymbolTakerFlow(1000, 3, 1))
