package hft.option

/** Black-Scholes 纯函数单测：已知参考值、put-call 平价、希腊字母范围与到期边界。 */
class BlackScholesSpec extends munit.FunSuite:
  import OptionRight.*

  private def near(a: Double, b: Double, eps: Double = 1e-3): Unit =
    assert(math.abs(a - b) < eps, s"expected $b, got $a (eps=$eps)")

  test("normCdf 关键点"):
    near(BlackScholes.normCdf(0.0), 0.5)
    near(BlackScholes.normCdf(1.96), 0.975, 1e-3)
    near(BlackScholes.normCdf(-1.96), 0.025, 1e-3)

  // 参考: S=K=100, T=1, σ=0.2, r=0 (ATM)
  // d1=0.1 -> call delta=N(0.1)=0.53983, price=100*(2N(0.1)-1)=7.9656, gamma=n(0.1)/20=0.019848, vega=100*n(0.1)=39.695
  test("ATM call 参考值"):
    val g = BlackScholes.greeks(Call, s = 100, k = 100, tYears = 1.0, sigma = 0.2, r = 0.0)
    near(g.delta, 0.53983, 1e-4)
    near(g.price, 7.9656, 1e-3)
    near(g.gamma, 0.019848, 1e-5)
    near(g.vega, 39.695, 1e-2)
    assert(g.theta < 0, "long option theta 应为负")

  test("put-call 平价: C - P = S - K·e^(-rT)"):
    val s = 100.0; val k = 95.0; val t = 0.75; val sigma = 0.3; val r = 0.05
    val c = BlackScholes.greeks(Call, s, k, t, sigma, r)
    val p = BlackScholes.greeks(Put, s, k, t, sigma, r)
    near(c.price - p.price, s - k * math.exp(-r * t), 1e-6)
    // delta 平价: delta_call - delta_put = 1
    near(c.delta - p.delta, 1.0, 1e-9)
    // gamma/vega 与方向无关，call/put 相等
    near(c.gamma, p.gamma, 1e-12)
    near(c.vega, p.vega, 1e-9)

  test("delta 范围: call∈[0,1], put∈[-1,0]"):
    val c = BlackScholes.greeks(Call, 120, 100, 0.5, 0.4, 0.0)
    val p = BlackScholes.greeks(Put, 80, 100, 0.5, 0.4, 0.0)
    assert(c.delta > 0 && c.delta < 1, s"call delta=${c.delta}")
    assert(p.delta > -1 && p.delta < 0, s"put delta=${p.delta}")

  test("到期边界 (T<=0): 内在价值 + 零阶以上希腊字母"):
    val itmCall = BlackScholes.greeks(Call, s = 110, k = 100, tYears = 0.0, sigma = 0.2, r = 0.0)
    near(itmCall.price, 10.0)
    near(itmCall.delta, 1.0)
    near(itmCall.gamma, 0.0)
    near(itmCall.vega, 0.0)
    near(itmCall.theta, 0.0)
    val otmPut = BlackScholes.greeks(Put, s = 110, k = 100, tYears = -0.1, sigma = 0.2, r = 0.0)
    near(otmPut.price, 0.0)
    near(otmPut.delta, 0.0)

  test("非法波动率/价格退化为内在价值，不抛异常或除零"):
    val g = BlackScholes.greeks(Call, s = 100, k = 100, tYears = 1.0, sigma = 0.0, r = 0.0)
    assert(g.gamma == 0.0 && g.vega == 0.0)
