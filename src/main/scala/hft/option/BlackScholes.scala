package hft.option

import hft.domain.Timestamp

/** 期权类型 */
enum OptionRight:
  case Call
  case Put

/** 欧式期权合约定义 (回测配置，不来自交易所)。strike 行权价，expiry 到期时间 (ms epoch)。 */
final case class OptionSpec(right: OptionRight, strike: Double, expiry: Timestamp)

/** 期权持仓：合约定义 + 数量 (以标的单位计，每张对应 1 单位标的；正数多头、负数空头) */
final case class OptionPosition(spec: OptionSpec, quantity: Double)

/** 单份期权的 Black-Scholes 希腊字母 (per-contract)。
  *
  * 单位约定 (标准 BS, 纯数学定义)：
  *   - delta: 无量纲, dPrice/dS, call∈[0,1] put∈[-1,0]
  *   - gamma: d²Price/dS²
  *   - vega : dPrice/dσ, 对 1.0 (=100%) 波动率
  *   - theta: dPrice/dt, **每年** (负值表示时间衰减)
  *   - price: 理论价 (与标的同计价单位)
  *
  * 注：与交易所 (如 OKX deltaBS/thetaBS) 的单位约定可能不同 (OKX theta 常为每日、vega 常为每 1%)，
  * 若需与实盘 greeks 完全对齐，应在消费侧统一约定或在此做换算。
  */
final case class BsGreeks(price: Double, delta: Double, gamma: Double, vega: Double, theta: Double)

/** Black-Scholes 欧式期权定价与希腊字母 —— 纯函数，无副作用、无外部依赖，便于直接断言测试。 */
object BlackScholes:

  /** 一年的天数 (365)，全仓年化基准的单一数据源 (派生 [[HoursPerYear]]/[[MillisPerYear]]) */
  val DaysPerYear: Double = 365.0

  /** 一年的小时数，逐小时采样年化的基准 (= DaysPerYear·24) */
  val HoursPerYear: Double = DaysPerYear * 24.0

  /** 一年的毫秒数，用于把到期时间差换算为年化剩余期限 */
  val MillisPerYear: Double = HoursPerYear * 60 * 60 * 1000

  /** 标准正态分布概率密度函数 */
  def normPdf(x: Double): Double = math.exp(-0.5 * x * x) / math.sqrt(2 * math.Pi)

  /** 标准正态分布累积分布函数 N(x) = 0.5·erfc(-x/√2)，erfc 用 Numerical Recipes 有理逼近 (|误差|<1.2e-7) */
  def normCdf(x: Double): Double = 0.5 * erfc(-x / math.sqrt(2.0))

  private def erfc(x: Double): Double =
    val z = math.abs(x)
    val t = 1.0 / (1.0 + 0.5 * z)
    val ans = t * math.exp(
      -z * z - 1.26551223 + t * (1.00002368 + t * (0.37409196 + t * (0.09678418 + t * (-0.18628806 +
        t * (0.27886807 + t * (-1.13520398 + t * (1.48851587 + t * (-0.82215223 + t * 0.17087277))))))))
    )
    if x >= 0.0 then ans else 2.0 - ans

  /** 计算单份期权的希腊字母。
    *
    * @param s        标的价 (>0)
    * @param k        行权价 (>0)
    * @param tYears   年化剩余期限；<=0 视为已到期，返回内在价值与零阶以上希腊字母
    * @param sigma    年化隐含波动率 (>0)
    * @param r        无风险年化利率
    */
  def greeks(right: OptionRight, s: Double, k: Double, tYears: Double, sigma: Double, r: Double): BsGreeks =
    // 边界：到期 / 非法波动率或价格 -> 退化为内在价值，导数为 0 (避免除零)
    if tYears <= 0.0 || sigma <= 0.0 || s <= 0.0 || k <= 0.0 then
      val intrinsic = right match
        case OptionRight.Call => math.max(s - k, 0.0)
        case OptionRight.Put  => math.max(k - s, 0.0)
      val delta = right match
        case OptionRight.Call => if s > k then 1.0 else 0.0
        case OptionRight.Put  => if s < k then -1.0 else 0.0
      BsGreeks(price = intrinsic, delta = delta, gamma = 0.0, vega = 0.0, theta = 0.0)
    else
      val sqrtT = math.sqrt(tYears)
      val d1 = (math.log(s / k) + (r + 0.5 * sigma * sigma) * tYears) / (sigma * sqrtT)
      val d2 = d1 - sigma * sqrtT
      val nd1 = normPdf(d1)
      val discount = math.exp(-r * tYears)
      val gamma = nd1 / (s * sigma * sqrtT)
      val vega = s * nd1 * sqrtT
      right match
        case OptionRight.Call =>
          val delta = normCdf(d1)
          val price = s * delta - k * discount * normCdf(d2)
          val theta = -(s * nd1 * sigma) / (2 * sqrtT) - r * k * discount * normCdf(d2)
          BsGreeks(price, delta, gamma, vega, theta)
        case OptionRight.Put =>
          val delta = normCdf(d1) - 1.0
          val price = k * discount * normCdf(-d2) - s * normCdf(-d1)
          val theta = -(s * nd1 * sigma) / (2 * sqrtT) + r * k * discount * normCdf(-d2)
          BsGreeks(price, delta, gamma, vega, theta)
