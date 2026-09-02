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

  test("非法波动率/标的价/行权价 -> 抛错, 不退化成'已到期'"):
    // 到期是合法市场状态; sigma/s/k 非正只能来自上游取数失败。共用一条退化分支的代价是:
    // 一条 markVol=0 的期权腿被按内在价值定价、希腊值全归零, 对冲把它从敞口里悄悄剔掉。
    val badVol = intercept[IllegalArgumentException](BlackScholes.greeks(Call, s = 100, k = 100, tYears = 1.0, sigma = 0.0, r = 0.0))
    assert(badVol.getMessage.contains("隐含波动率必须为正"), badVol.getMessage)
    intercept[IllegalArgumentException](BlackScholes.greeks(Call, s = 0.0, k = 100, tYears = 1.0, sigma = 0.2, r = 0.0))
    intercept[IllegalArgumentException](BlackScholes.greeks(Call, s = 100, k = 0.0, tYears = 1.0, sigma = 0.2, r = 0.0))
    intercept[IllegalArgumentException](BlackScholes.greeks(Call, s = Double.NaN, k = 100, tYears = 1.0, sigma = 0.2, r = 0.0))

  // 希腊字母必须是价格的导数 (而非恰好命中某参考点)：用中心差分逐一核对。
  // 这能抓住"某点对、但导数公式错"的 bug (delta/gamma/vega/theta 任一系数/符号错都会暴露)。
  test("希腊字母 = 价格的有限差分导数 (delta/gamma/vega/theta)"):
    for
      right <- Seq(Call, Put)
      s <- Seq(80.0, 100.0, 125.0) // OTM / ATM / ITM
    do
      val k = 100.0; val t = 0.5; val sigma = 0.4; val r = 0.02
      def price(ss: Double = s, tt: Double = t, vv: Double = sigma): Double =
        BlackScholes.greeks(right, ss, k, tt, vv, r).price
      val g = BlackScholes.greeks(right, s, k, t, sigma, r)
      val hS = s * 1e-5
      val deltaFd = (price(ss = s + hS) - price(ss = s - hS)) / (2 * hS)
      val gammaFd = (price(ss = s + hS) - 2 * g.price + price(ss = s - hS)) / (hS * hS)
      val hV = 1e-6
      val vegaFd = (price(vv = sigma + hV) - price(vv = sigma - hV)) / (2 * hV)
      val hT = 1e-6
      val thetaFd = (price(tt = t - hT) - price(tt = t)) / hT // dP/d(日历时间) = -dP/d(剩余期限)
      def rel(a: Double, b: Double) = math.abs(a - b) / math.max(math.abs(b), 1e-6)
      val tag = s"$right S=$s"
      assert(rel(deltaFd, g.delta) < 1e-4, s"$tag delta fd=$deltaFd bs=${g.delta}")
      assert(rel(gammaFd, g.gamma) < 1e-3, s"$tag gamma fd=$gammaFd bs=${g.gamma}")
      assert(rel(vegaFd, g.vega) < 1e-4, s"$tag vega fd=$vegaFd bs=${g.vega}")
      assert(rel(thetaFd, g.theta) < 1e-3, s"$tag theta fd=$thetaFd bs=${g.theta}")
