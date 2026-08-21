package strategy.strategies.gridsellhedge.logic

import hft.option.OptionRight

/** 规则 1 —— **网格卖出** (纯逻辑, 无副作用/无状态, 便于单测)。
  *
  * 网格线 = [[spacing]] 的整数倍 (默认 100)。轮询模型 (每 3 秒检查一次):
  *   - 维护当前价**上下最近两档**各有一份空头: 上档 call、下档 put ([[bracket]])。
  *   - 每次检查时对缺失的一侧补卖 (调用方按持仓过滤 [[bracket]] 结果)。价格漂移进新的 100 区间后, 新的上下档被补上;
  *     旧档持有到期后释放, 可再次被补卖。
  *
  * 全部以整数倍网格索引计算, 行权价恰落在网格线上, 避免浮点漂移。 */
object OptionGrid:

  /** 待卖出的一档: 期权类型 + 行权价 (恰落在网格线上)。 */
  final case class Sell(right: OptionRight, strike: Double)

  /** 严格大于 `price` 的最近网格线 (price 恰在线上则取上一格)。 */
  def nextAbove(price: Double, spacing: Double): Double = (math.floor(price / spacing) + 1) * spacing

  /** 严格小于 `price` 的最近网格线 (price 恰在线上则取下一格)。 */
  def nextBelow(price: Double, spacing: Double): Double = (math.ceil(price / spacing) - 1) * spacing

  /** 当前价上下最近两档应持有的空头: 上档 call + 下档 put (价 2350 → call@2400 + put@2300)。
    * 调用方按已有持仓过滤后, 对缺失的补卖。 */
  def bracket(price: Double, spacing: Double): Seq[Sell] =
    Seq(Sell(OptionRight.Call, nextAbove(price, spacing)), Sell(OptionRight.Put, nextBelow(price, spacing)))
