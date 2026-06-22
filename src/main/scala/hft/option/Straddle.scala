package hft.option

/** 跨式/宽跨 (N 份 long call@callStrike + N put@putStrike) 的 BS 估值与**聚合希腊字母** —— 框架级复用,
  * 消除 BsGreeksSource / 回测验证 / 各 demo 各算一遍 call+put 的重复。
  *
  * 单位为标准 BS (theta 每年、vega 对 1.0)；消费侧 (如 OKX/Bybit 通道) 自行换算 (theta/365、vega/100)。 */
object Straddle:
  /** 聚合后的跨式希腊字母 (已乘份数 n)。theta 每年、vega 对 1.0 (标准 BS 口径)。 */
  final case class Greeks(price: Double, delta: Double, gamma: Double, vega: Double, theta: Double)

  /** N 份跨式/宽跨在 (s, tYears, iv, r) 的聚合希腊字母。callStrike==putStrike 即标准 ATM 跨式。 */
  def greeks(n: Double, s: Double, callStrike: Double, putStrike: Double, tYears: Double, iv: Double, r: Double): Greeks =
    val c = BlackScholes.greeks(OptionRight.Call, s, callStrike, tYears, iv, r)
    val p = BlackScholes.greeks(OptionRight.Put, s, putStrike, tYears, iv, r)
    Greeks(
      price = n * (c.price + p.price),
      delta = n * (c.delta + p.delta),
      gamma = n * (c.gamma + p.gamma),
      vega = n * (c.vega + p.vega),
      theta = n * (c.theta + p.theta),
    )

  /** N 份跨式/宽跨的理论价值 (= 聚合 price)。 */
  def value(n: Double, s: Double, callStrike: Double, putStrike: Double, tYears: Double, iv: Double, r: Double): Double =
    greeks(n, s, callStrike, putStrike, tYears, iv, r).price
