package strategy.strategies.ivsellhedge.live

import strategy.strategies.ivsellhedge.logic.{SellPlan, SigmaSource}
import strategy.utils.hedge.DeltaCtx
import hft.domain.{Coin, Price}

import java.nio.file.Files

/** 配置单测：JSON 解析 + 默认回填、K 线粒度换算 (SSOT)、越界即抛。 */
class IvSellHedgeConfigSpec extends munit.FunSuite:

  private def tuning = IvSellTuning(symbol = "ETH", baseCoin = "ETH", ccy = "ETH", ivStart = 0.2)

  test("K 线粒度只配一处, 毫秒由它派生 (MACD 与快线各一条)"):
    assertEquals(tuning.copy(macdBar = "1H").macdBarMs, 3_600_000L)
    assertEquals(tuning.copy(fastBar = "1m").fastBarMs, 60_000L)
    assertEquals(tuning.copy(fastBar = "5m").fastBarMs, 300_000L)
    intercept[RuntimeException](tuning.copy(fastBar = "7m").fastBarMs)
    assertEquals(tuning.copy(macdBar = "5m").macdBarMs, 300_000L)
    assertEquals(tuning.copy(macdBar = "1D").macdBarMs, 86_400_000L)

  test("未知粒度抛错, 不猜默认值 (猜错的后果是 MACD 跑在没人想要的周期上且无症状)"):
    intercept[RuntimeException](tuning.copy(macdBar = "7m").macdBarMs)

  test("敞口陈旧阈值缺省 = 4×发布间隔; 显式配置优先"):
    assertEquals(tuning.copy(publishExposureMs = 1000).exposureStaleMs, 4000L)
    assertEquals(tuning.copy(publishExposureMs = 1000, maxExposureStaleMs = Some(7000)).exposureStaleMs, 7000L)

  test("映射到 actor 配置时做参数校验 (启动即失败, 不带病上线)"):
    val t = tuning.copy(ivQtyStart = 5, ivQtySlope = 1, ivQtyMax = 30)
    val ok = t.toSellerConfig
    assertEquals(ok.ivQty, SellPlan.IvQty(t.ivStart, 5, 1, 30))
    assertEquals(ok.minTtlMs, t.minTtlDays * SellPlan.DayMs, "天 -> 毫秒的换算")
    // 起卖量超上限 -> 起点会被静默改写, 故必须抛
    intercept[IllegalArgumentException](tuning.copy(ivQtyStart = 99, ivQtyMax = 10).toSellerConfig)
    intercept[IllegalArgumentException](tuning.copy(targetDays = 0).toSellerConfig)
    intercept[IllegalArgumentException](tuning.copy(maxSpreadRatio = 0.9).toSellerConfig)
    intercept[IllegalArgumentException](tuning.copy(publishExposureMs = 0).toSellerConfig)
    intercept[IllegalArgumentException](tuning.copy(settleRounds = 0).toSellerConfig)
    // ivStart=0 的语义与参考实现相反 ("从零波动起线性放大" 而非 "不启用缩放"), 更可能是漏配
    intercept[IllegalArgumentException](tuning.copy(ivStart = 0.0).toSellerConfig)

  test("死区阈值按预测波动范围定, 方向决定两侧不对称"):
    val t = tuning.copy(tightMult = 0.5, looseMult = 2.0, hedgeHorizonMinutes = 30,
      minTheta = 0.001, maxTheta = 100.0)
    val b = t.deltaBand
    // range = |gamma| × spot × σ × √(30分/一年)
    val gamma = 0.01; val spot = 3000.0; val sigma = 0.6
    val range = gamma * spot * sigma * math.sqrt(30 * 60_000.0 / hft.option.BlackScholes.MillisPerYear)
    val (up, down) = b.bands(DeltaCtx(Coin.Zero, 1, Coin(gamma), Price(spot), Some(sigma)))
    assert(math.abs(down.value - range * 0.5) < 1e-12, s"空头侧应 = 0.5×range, 实为 ${down.value}")
    assert(math.abs(up.value - range * 2.0) < 1e-12, s"多头侧应 = 2.0×range, 实为 ${up.value}")

  test("maxTheta 是敞口硬上界, 与波动率无关"):
    val t = tuning.copy(minTheta = 0.001, maxTheta = 0.5)
    val b = t.deltaBand
    val (up, down) = b.bands(DeltaCtx(Coin.Zero, 0, Coin(1.0), Price(3000.0), Some(5.0))) // 极端波动
    assertEquals(up, Coin(0.5))
    assertEquals(down, Coin(0.5))

  test("σ 来源可切换; 未知取值抛错而不是静默回退"):
    assertEquals(tuning.copy(sigmaSource = "realized").sigma, SigmaSource.Realized)
    assertEquals(tuning.copy(sigmaSource = "iv").sigma, SigmaSource.ImpliedVol)
    assertEquals(tuning.copy(sigmaSource = "MAX").sigma, SigmaSource.MaxOfBoth)
    intercept[RuntimeException](tuning.copy(sigmaSource = "vix").sigma)

  test("配置文件缺失 -> Left(原因), 不静默"):
    assert(IvSellHedgeConfig.loadOkx("conf/definitely-not-here.json").isLeft)

  test("模板配置可解析, 且未列出的字段回填默认值"):
    IvSellHedgeConfig.loadOkx("conf/iv-sell-hedge-okx.example.json") match
      case Left(e) => fail(s"模板应可解析: $e")
      case Right(c) =>
        assertEquals(c.tuning.symbol, "ETH")
        assertEquals(c.tuning.macdBar, "1H")
        assertEquals(c.tuning.enableOpen, false)
        assertEquals(c.tuning.macdFast, 12)  // 模板未列出 -> 默认值
        assertEquals(c.tuning.fastBar, "1m")
        assertEquals(c.tuning.passiveTtlMs, 60_000L)
        assertEquals(c.tuning.crossTtlMs, 1000L)
        // 模板里 simulated 必须是 false: true 会被启动器拒绝启动 (永续腿只连主网,
        // 期权腿去模拟环境 -> 两条腿持仓互不相干), 一个跑不起来的模板不该是模板。
        assertEquals(c.simulated, false)

  test("JSON 漏写 ivStart 即解析失败 —— 它没有默认值"):
    // 给它一个"看着合理"的默认值等于埋一个错配置: 起卖点决定从多高的 IV 开始卖,
    // 猜错就是在不该卖的波动率上持续卖出, 而没有任何症状。
    val f = Files.createTempFile("iv-sell-hedge", ".json")
    try
      Files.writeString(f, """{"apiKey":"k","apiSecret":"s","passphrase":"p","tuning":{"symbol":"ETH","baseCoin":"ETH","ccy":"ETH"}}""")
      assert(IvSellHedgeConfig.loadOkx(f.toString).isLeft, "漏写 ivStart 必须解析失败")
    finally Files.deleteIfExists(f)

  test("minTtlDays 默认值来自唯一来源, 且不超过 targetDays"):
    // 两者是同一条选到期逻辑的两头: minTtl 一旦超过 targetDays, 目标那一档就被自己的下限
    // 筛掉, 策略静默改卖更远的到期 (配置侧从前写死 7、而 targetDays 是 3, 正是这个形态)。
    assertEquals(tuning.minTtlDays, OptionSellerActor.Defaults.MinTtlDays)
    assert(tuning.minTtlDays <= tuning.targetDays, s"minTtlDays=${tuning.minTtlDays} 不该超过 targetDays=${tuning.targetDays}")
    assertEquals(tuning.toSellerConfig.minTtlMs, OptionSellerActor.Defaults.MinTtlMs)

  test("riskFreeRate 默认值与 BS 定价那份是同一个事实"):
    assertEquals(tuning.toSellerConfig.riskFreeRate, strategy.strategies.ivsellhedge.logic.PortfolioDelta.DefaultRate)
