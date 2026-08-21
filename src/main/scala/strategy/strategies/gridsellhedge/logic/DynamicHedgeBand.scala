package strategy.strategies.gridsellhedge.logic

import hft.option.OptionRight

/** 规则 2 的**阈值动态** (纯逻辑, 与执行方式无关, 便于单测)。
  *
  * 每档一份状态: 一个会伸缩的**离场阈值** `threshold`。开/平的判定与执行 (maker 挂单/成交) 由调用方 (模拟器) 负责,
  * 本模块只提供阈值的纯变换。语义 (本版):
  *   - **开仓判定** (调用方): 价越过行权价即对冲 —— 风险方向 `d ≥ 0` (短 call 价上穿 K, 短 put 价下穿 K)。
  *   - **对冲实际开仓时** ([[expandOnOpen]]): 每次越过行权价 `threshold ×= expandFactor` (放大离场阈值)。
  *   - **平仓判定** (调用方): 已持对冲且价回落超过阈值 —— `d ≤ -threshold` (回到行权价安全侧 threshold 之外)。
  *   - **衰减** ([[decayed]]): 未持对冲时每满 `decayIntervalMs` → `threshold ×= decayFactor`, 下限 `initialThreshold`。
  *   - **平仓后** ([[resetClock]]): 衰减时钟重置到平仓时刻。
  *
  * 数值默认对应需求原文: 初始 0.4% / 每次越过 ×1.2 / 每小时 ×0.9。阈值不设上限。 */
object DynamicHedgeBand:

  /** 状态机参数 (常量集中, 单一数据源)。衰减下限 = 初始阈值。 */
  final case class Params(
      initialThreshold: Double = 0.004,   // 0.4% (离场阈值初值/下限)
      expandFactor: Double = 1.2,         // 每次越过行权价放大 20%
      decayFactor: Double = 0.9,          // 未触发每小时缩 10%
      decayIntervalMs: Long = 3_600_000L, // 一小时
  ):
    require(initialThreshold > 0.0 && expandFactor >= 1.0 && decayFactor > 0.0 && decayFactor <= 1.0)

  /** 单档阈值状态。`threshold` 当前离场阈值 (安全侧距行权价的比例); `lastDecayTs` 上次衰减对齐的时间戳。 */
  final case class State(threshold: Double, lastDecayTs: Long)

  /** 初始状态: 阈值 = 初值, 衰减时钟从 `now` 起。 */
  def initial(now: Long, params: Params = Params()): State = State(params.initialThreshold, lastDecayTs = now)

  /** 风险方向上的价偏离比例: 短 call 用 (S−K)/K, 短 put 用 (K−S)/K。>0 表示朝风险方向偏离 (已越过行权价)。 */
  def riskyDistance(right: OptionRight, strike: Double, price: Double): Double =
    right match
      case OptionRight.Call => (price - strike) / strike
      case OptionRight.Put  => (strike - price) / strike

  /** 按已过的整点小时衰减离场阈值 (调用方保证仅在未持对冲时调用)。纯函数。 */
  def decayed(state: State, now: Long, params: Params = Params()): State =
    val elapsed = math.max(0L, now - state.lastDecayTs)
    val intervals = elapsed / params.decayIntervalMs
    if intervals <= 0 then state
    else
      val t = math.max(params.initialThreshold, state.threshold * math.pow(params.decayFactor, intervals.toDouble))
      State(t, state.lastDecayTs + intervals * params.decayIntervalMs)

  /** 对冲实际开仓时放大离场阈值 (每次越过行权价放大对冲带)。 */
  def expandOnOpen(state: State, params: Params = Params()): State =
    state.copy(threshold = state.threshold * params.expandFactor)

  /** 平仓后把衰减时钟重置到 `now` (衰减从平仓时刻重新起算)。 */
  def resetClock(state: State, now: Long): State = state.copy(lastDecayTs = now)
