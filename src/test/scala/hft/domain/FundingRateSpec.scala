package hft.domain

class FundingRateSpec extends munit.FunSuite:
  private val hour = 60 * 60 * 1000L
  private val t0 = 1_700_000_000_000L

  private def rate(r: Rate, hoursToSettle: Double): FundingRate =
    FundingRate(
      Exchange.Binance,
      "BTCUSDT",
      rate = r,
      nextSettleTime = t0 + (hoursToSettle * hour).toLong,
      timestamp = t0,
    )

  test("dailyRate = rate * 24 / 距结算小时数"):
    // 0.01% 费率、8 小时后结算 -> 日化 0.03%
    assertEqualsDouble(rate(0.0001, 8).dailyRate, 0.0003, 1e-12)
    // 5 小时后结算 -> 0.05% * 24 / 5 = 0.24%
    assertEqualsDouble(rate(0.0005, 5).dailyRate, 0.0024, 1e-12)

  test("临近结算时按最小 1 小时计算，防止日化爆炸"):
    assertEqualsDouble(rate(0.0001, 0.5).dailyRate, 0.0001 * 24, 1e-12)

  test("数据已过结算时间则视为过期，返回 0"):
    assertEquals(FundingRate(Exchange.Binance, "BTCUSDT", 0.0001, nextSettleTime = t0, timestamp = t0).dailyRate, 0.0)

