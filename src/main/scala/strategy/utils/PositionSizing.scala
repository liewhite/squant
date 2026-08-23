package strategy.utils

import hft.domain.{Price, Coin, Side}

/** 离散仓位 (满多 / 平 / 满空) 策略共用的纯仓位原语。
  *
  * 把"方向 + 杠杆 + 权益 + 价"折算成目标币数, 再由 (目标仓, 当前仓) 推出该下的市价单。
  * 离散三态 (满多 / 平 / 满空) 策略共用同一套折算与对账, 避免重复。
  */
object PositionSizing:
  /** 目标币数 = 方向 × **leverage × 当前权益 / 进场价** (满仓顺势 = leverage 倍当前权益, 默认 1×)。
    * dir=0 或非法价/非法权益 -> 0。按权益折算 (而非固定名义), 使敞口/权益比例恒定、不随盈亏漂移、无杠杆失控。
    * 调用方在**方向切换时**取此值并冻结, 持仓期间不随价格变动重算 (避免无谓换手)。 */
  def targetQty(dir: Int, leverage: Double, equity: Double, price: Price): Coin =
    if dir == 0 || price <= 0.0 || equity <= 0.0 then Coin.Zero
    else Coin(dir * leverage * equity / price)

  /** 由 (目标仓, 当前仓, 最小下单量) 推出该下的市价单。|gap|<minQty -> None (已对齐, 不动)。 */
  def orderFor(targetPos: Coin, pos: Coin, minQty: Coin): Option[(Side, Coin)] =
    val gap = targetPos - pos
    if gap.abs < minQty then None
    else Some((if gap > Coin.Zero then Side.Long else Side.Short, gap.abs))
