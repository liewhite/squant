package strategy.strategies.ivsellhedge.live

import strategy.strategies.ivsellhedge.logic.SellPlan
import strategy.utils.hedge.DeltaCtx

/** 配置单测：JSON 解析 + 默认回填、K 线粒度换算 (SSOT)、越界即抛。 */
class IvSellHedgeConfigSpec extends munit.FunSuite:

  private def tuning = IvSellTuning(symbol = "ETH", baseCoin = "ETH", ccy = "ETH", ivStart = 0.2)

  test("K 线粒度只配一处, 毫秒由它派生 (MACD 与 KAMA 各一条)"):
    assertEquals(tuning.copy(macdBar = "1H").macdBarMs, 3_600_000L)
    assertEquals(tuning.copy(erBar = "1m").erBarMs, 60_000L)
    assertEquals(tuning.copy(erBar = "5m").erBarMs, 300_000L)
    intercept[RuntimeException](tuning.copy(erBar = "7m").erBarMs)
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

  test("死区阈值随两个信号缩放, 且给出真实敞口的上界"):
    val t = tuning.copy(deltaThreshold = 0.4, macdTightenRatio = 0.5, chopWidenMult = 2.0, trendTightenMult = 0.5)
    assertEquals(t.maxExposure, 0.8, "上界 = 基准 × 震荡放宽倍数")
    val b = t.deltaBand
    assertEquals(b.bands(DeltaCtx(hft.domain.Coin.Zero, 0, Some(0.0))), (hft.domain.Coin(0.8), hft.domain.Coin(0.8)))
    assertEquals(b.bands(DeltaCtx(hft.domain.Coin.Zero, 0, Some(1.0))), (hft.domain.Coin(0.2), hft.domain.Coin(0.2)))

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
        assertEquals(c.tuning.erBar, "1m")
        assertEquals(c.tuning.passiveTtlMs, 60_000L)
        assertEquals(c.tuning.crossTtlMs, 1000L)
        assertEquals(c.simulated, true)
