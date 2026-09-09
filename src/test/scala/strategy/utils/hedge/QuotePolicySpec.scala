package strategy.utils.hedge

/** QuotePolicy 单测：ER 阈值两侧的选择与预热不足时的取舍方向。 */
class QuotePolicySpec extends munit.FunSuite:
  private val calm = QuoteStyle.passive(0.0002, 60_000)
  private val trending = QuoteStyle.crossing(0.0005, 1000)
  private val policy = QuotePolicy.byEfficiency(0.5, calm, trending)

  test("ER 低 (来回折返) -> 被动慢挂, 省 taker 费"):
    assertEquals(policy.styleFor(Some(0.0)), calm)
    assertEquals(policy.styleFor(Some(0.49)), calm)

  test("ER 高 (走单边) -> 跨价追单, 宁可付费也要冲上"):
    assertEquals(policy.styleFor(Some(0.5)), trending, "阈值本身算单边")
    assertEquals(policy.styleFor(Some(1.0)), trending)

  test("ER 预热不足 -> 按单边处理 (不动意味着裸着敞口, 与'读数不可信就不动'方向相反)"):
    assertEquals(policy.styleFor(None), trending)

  test("fixed: 恒用一种方式, ER 不参与 (价格轴对冲的既有行为)"):
    val f = QuotePolicy.fixed(calm)
    assertEquals(f.styleFor(None), calm)
    assertEquals(f.styleFor(Some(0.99)), calm)

  test("阈值越界抛错 (ER 恒在 [0,1], 阈值落在区间外等于该规则从不生效)"):
    intercept[IllegalArgumentException](QuotePolicy.byEfficiency(0.0, calm, trending))
    intercept[IllegalArgumentException](QuotePolicy.byEfficiency(1.0, calm, trending))
    intercept[IllegalArgumentException](QuotePolicy.byEfficiency(-0.1, calm, trending))
