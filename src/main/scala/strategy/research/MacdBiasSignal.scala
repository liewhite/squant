package strategy.research

import hft.strategy.{OutcomeEvent, Strategy}

import hft.indicator.{KlineSeries, Macd}

/** MACD 柱 -> 方向偏移强度的映射模式：
  *   - [[MacdBiasMode.Sign]]   : 只看柱符号 (水上+1 / 水下-1)，幅度 1
  *   - [[MacdBiasMode.Graded]] : 颜色×趋势分级 (同向最激进)，幅度 {-2..2}，见 [[hft.indicator.Macd.histBias]]
  */
enum MacdBiasMode:
  case Sign
  case Graded

/** MACD 方向信号 —— 逐笔成交聚合 K 线算 MACD(12,26,9)，输出方向偏移强度 [[bias]]。
  *
  * 这是"K 线 -> MACD -> 方向强度"的**统一映射实现 (算法 SSOT)**，供各策略复用 (各持一份独立实例、各喂各的成交，
  * 非运行时共享同一份状态)：
  *   - [[GammaScalpStrategy]] 用它做对冲**间距**偏移 (顺势挂更远)；
  *   - [[MacdBiasOverlay]] (经 [[TargetDeltaHedgeStrategy]]) 用它做对冲**目标 delta** (水上正、水下负)。
  *
  * 水上 (柱>0) 看多、水下 (柱<0) 看空；[[MacdBiasMode.Graded]] 按颜色×趋势分级 {-2..2}
  * (同向最激进)，[[MacdBiasMode.Sign]] 只看柱符号 ±1。预热不足时 [[bias]] 返回 0。
  */
final class MacdBiasSignal(
    barIntervalMs: Long,
    maxBars: Int,
    biasMode: MacdBiasMode,
    /** "连续上升/下降"判定的周期数 (仅 Graded 模式用)；颜色与趋势同向时偏移最激进 (±2) */
    macdTrendBars: Int,
):
  private val klines = new KlineSeries(barIntervalMs, maxBars) with Macd

  /** 喂入一笔成交 (聚合 K 线并更新 MACD) */
  def update(timestamp: Long, price: Double, qty: Double): Unit =
    klines.update(timestamp, price, qty)

  /** 当前方向偏移强度：Sign=±1(柱符号) / Graded=±2(颜色×趋势分级)，预热不足或持平=0 */
  def bias: Int = biasMode match
    case MacdBiasMode.Sign   => klines.macdDirection
    case MacdBiasMode.Graded => klines.histBias(macdTrendBars)
