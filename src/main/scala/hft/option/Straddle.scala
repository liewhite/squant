package hft.option

/** 跨式/宽跨 (N 份 call@callStrike + N put@putStrike) 的 BS 估值与**聚合希腊字母** —— 框架级复用,
  * 消除 BsGreeksSource / 回测验证各算一遍 call+put 的重复。N 为负即空头。
  *
  * 聚合结果直接用 [[BsGreeks]]：字段、单位与单份期权完全相同 (theta 每年、vega 对 1.0)，
  * 再定义一个同构的 `Straddle.Greeks` 只是让消费侧多认一个类型。
  * 消费侧 (如 OKX/Bybit 通道) 自行换算 (theta/365、vega/100)。 */
object Straddle:
  /** N 份跨式/宽跨在 (s, tYears, iv, r) 的聚合希腊字母。callStrike==putStrike 即标准 ATM 跨式。
    *
    * 参数契约同 [[BlackScholes.greeks]]：`s`/`callStrike`/`putStrike`/`iv` 必须为正，非法即抛。 */
  def greeks(n: Double, s: Double, callStrike: Double, putStrike: Double, tYears: Double, iv: Double, r: Double): BsGreeks =
    val c = BlackScholes.greeks(OptionRight.Call, s, callStrike, tYears, iv, r)
    val p = BlackScholes.greeks(OptionRight.Put, s, putStrike, tYears, iv, r)
    BsGreeks(
      price = n * (c.price + p.price),
      delta = n * (c.delta + p.delta),
      gamma = n * (c.gamma + p.gamma),
      vega = n * (c.vega + p.vega),
      theta = n * (c.theta + p.theta),
    )

  /** N 份跨式/宽跨的理论价值 (= 聚合 price)。 */
  def value(n: Double, s: Double, callStrike: Double, putStrike: Double, tYears: Double, iv: Double, r: Double): Double =
    greeks(n, s, callStrike, putStrike, tYears, iv, r).price
