package strategy.research

import hft.strategy.{OutcomeEvent, Strategy}

import hft.domain.Price

/** 棘轮滞回开关——防"高空坠落"。
  *
  * 棘轮在急反弹见顶后会把仓位顶在高位裸多，随后暴跌又因旧低位卖锚被锁住、无法及时对冲 → 重亏
  * (见 03-21 周)。本开关在出现"大幅反弹"时**临时关闭棘轮**(改回常规逐带对冲，保证反转能及时止损)，
  * 待价格"再下杀"到一定程度才重新启用 (并要求调用方重置棘轮锚，清除 stale 锚)。
  *
  * 与"反向锚重置=退化成 baseline"不同：开关只在大反弹这种**危险 setup**才放开，平时小震荡里棘轮照常
  * 抑制 whipsaw，故有区分度、不退化为 baseline。
  *
  * @param disablePct 自启用期间最低点反弹达此比例 -> 关闭棘轮 (0.04 = +4%)
  * @param rearmPct   关闭期间自最高点回落达此比例 -> 重新启用 (0.04 = -4%)
  */
final class RatchetSwitch(disablePct: Double = 0.04, rearmPct: Double = 0.04):
  private var enabledFlag = true
  private var lo = Double.PositiveInfinity   // 启用期间的最低价 (反弹基准)
  private var peak = Double.NegativeInfinity // 关闭期间的最高价 (下杀基准)

  def enabled: Boolean = enabledFlag

  /** 喂入最新价，推进状态机；返回是否**刚刚重新启用** (供调用方重置棘轮锚)。 */
  def update(price: Price): Boolean =
    if enabledFlag then
      if price < lo then lo = price
      if lo.isFinite && price >= lo * (1 + disablePct) then // 大幅反弹 -> 关闭棘轮
        enabledFlag = false
        peak = price
      false
    else
      if price > peak then peak = price
      if price <= peak * (1 - rearmPct) then // 再下杀 -> 重新启用并请求重置锚
        enabledFlag = true
        lo = price
        true
      else false
