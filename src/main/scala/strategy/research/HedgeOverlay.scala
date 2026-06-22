package strategy.research

import hft.strategy.{OutcomeEvent, Strategy}

import hft.domain.{Price, Quantity, Timestamp}
import hft.indicator.{Kama, KlineSeries}

/** **方向 overlay** —— 决定对冲的"目标净 delta"与"对冲带宽乘子"。
  *
  * 这是 [[TargetDeltaHedgeStrategy]] 的方向信号抽象：策略只问 overlay 要 [[targetDelta]] 与
  * [[bandMult]]，不关心它由 MACD 还是 KAMA-趋势算出。新增方向逻辑只需新增实现 (开放封闭)，
  * 策略代码不变、不在策略里用 if/match 分发信号来源。
  *
  *   - [[targetDelta]]：对冲把净 delta 拉到的目标 (币本位)，0 = 中性对冲。
  *   - [[bandMult]]   ：对冲容忍带的乘子 (×策略基础带)，>1 放宽 (少对冲、留敞口)、<1 收紧。
  */
trait HedgeOverlay:
  /** 喂入逐笔成交 (聚合内部指标，如 MACD K 线 / KAMA) */
  def update(ts: Timestamp, price: Price, qty: Quantity): Unit

  /** 目标净 delta；gammaScale = |gamma|·S (单位价幅对应的 delta 漂移，币本位) */
  def targetDelta(gammaScale: Double): Double

  /** 对冲带宽乘子 (×基础带)，默认 1.0 (不调整) */
  def bandMult: Double = 1.0

/** MACD 方向 overlay：水上目标正 delta、水下目标负 delta，幅度按 MACD 柱强度 (bias∈{-2..2}) ×
  * [[tiltMoveRatio]]。[[tiltMoveRatio]]=0 即 target 恒 0 (纯中性对冲)。带宽不调整 (bandMult=1)。
  * 与 [[GammaScalpStrategy]] 共用同一 [[MacdBiasSignal]] (SSOT)。 */
final class MacdBiasOverlay(
    barIntervalMs: Long = 3_600_000,
    maxBars: Int = 200,
    biasMode: MacdBiasMode = MacdBiasMode.Graded,
    macdTrendBars: Int = 2,
    tiltMoveRatio: Double = 0.005,
) extends HedgeOverlay:
  private val signal = MacdBiasSignal(barIntervalMs, maxBars, biasMode, macdTrendBars)
  def update(ts: Timestamp, price: Price, qty: Quantity): Unit = signal.update(ts, price, qty)
  def targetDelta(gammaScale: Double): Double = signal.bias * tiltMoveRatio * gammaScale

/** KAMA-趋势 overlay：用 1min KAMA 的**效率比 ER**(趋势/震荡判据) 与**价相对 KAMA 的方向**驱动
  * "小波动留敞口 / 大波动顺势超量对冲"。
  *
  *   - 方向 dir = sign(price − KAMA)，含死区 [[dirDeadband]] (贴近均线视为无方向)：
  *     price 在 KAMA 上方=上升趋势→追多敞口；跌破 KAMA→dir 翻号、目标归 0/转空 (回落即完全对冲)。
  *   - 目标 delta = dir · ER · [[tiltMoveRatio]] · gammaScale：
  *     趋势 (ER 高) 顺势侧建立 ±A 敞口 (超量对冲)；逆势侧目标永不越过 0 (绝不建反向仓)；
  *     震荡 (ER≈0) 目标≈0 (中性)。目标挂在 price-KAMA 这一**缓变外部参考**上，故稳定不发散。
  *   - 带宽乘子按 ER 线性插值：ER=0 取 [[bandChopMult]] (放宽、留敞口)，ER=1 取 [[bandTrendMult]] (收紧、及时追)。
  */
final class KamaTrendOverlay(
    /** 区制 KAMA 的 bar 周期：须足够慢以免方向频繁翻号 whipsaw (默认 15min) */
    barIntervalMs: Long = 900_000,
    maxBars: Int = 200,
    erPeriod: Int = 10,
    /** 顺势超量对冲幅度系数: target = dir·ER·tiltMoveRatio·|gamma|·S */
    tiltMoveRatio: Double = 0.01,
    /** 方向死区: |price/KAMA−1| ≤ 此值视为无方向 (dir=0)，抑制贴均线时方向翻号 (默认 0.3%) */
    dirDeadband: Double = 0.003,
    /** ER=0 (震荡) 时的带宽乘子 (>1 放宽、留敞口) */
    bandChopMult: Double = 2.0,
    /** ER=1 (趋势) 时的带宽乘子 (≤1 收紧、及时对冲；默认 1.0=不窄于基础带, 避免趋势中过度对冲) */
    bandTrendMult: Double = 1.0,
) extends HedgeOverlay:
  private val series = new KlineSeries(barIntervalMs, maxBars) with Kama:
    override protected def kamaErPeriod: Int = erPeriod
  private var lastPrice = 0.0

  def update(ts: Timestamp, price: Price, qty: Quantity): Unit =
    series.update(ts, price, qty)
    lastPrice = price

  /** 价相对 KAMA 的方向 (含死区)；预热不足 -> 0 (无方向) */
  private def dir: Int = series.kama match
    case Some(k) if k != 0.0 =>
      val rel = lastPrice / k - 1.0
      if rel > dirDeadband then 1 else if rel < -dirDeadband then -1 else 0
    case _ => 0

  /** 当前效率比 (预热不足 -> 0) */
  private def er: Double = series.efficiencyRatio.getOrElse(0.0)

  def targetDelta(gammaScale: Double): Double = dir * er * tiltMoveRatio * gammaScale

  /** 带宽乘子 = lerp(chop, trend) by ER：ER 越高带越窄 (越及时)。预热不足 ER=0 -> 取 chop (放宽) */
  override def bandMult: Double = bandChopMult + (bandTrendMult - bandChopMult) * er
