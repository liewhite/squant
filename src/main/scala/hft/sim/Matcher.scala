package hft.sim

import hft.domain.*

/** 撮合判定 (纯函数) —— 回测与模拟盘共用的成交模型，两侧刻意不对称：
  *
  *   - **maker (被动挂单) 走悲观侧**：只有价格**严格穿越**挂单价才算成交 ([[crossedByBbo]] /
  *     [[crossedByTrade]])。价格仅仅触及挂单价时不成交 —— 真实交易所里价位上排着队，
  *     价格没穿过去意味着队列没消化到我。无深度数据可判队列位置，故取下界。
  *   - **taker (主动吃单) 走乐观侧**：与盘口**价格重合即全量成交** ([[marketable]])，
  *     在对手价 ([[touchPrice]]) 一次成交，不看盘口挂单量、不建模吃穿多档的冲击成本。
  *
  * 两侧用同一个盘口但判据不同 (`<` 与 `<=`)，中间留出一格**有意的悲观间隙**：买单挂在 L、
  * 盘口 ask 恰好等于 L 时，若订单是此刻到达则按 taker 成交 (主动吃单)，若订单早已在簿上
  * 则不成交 (被动排队)。这不是漏洞，正是"主动吃 vs 被动等"的真实差别。
  */
object Matcher:
  /** taker 可成交性：到达单与盘口**价格重合即可成交** (含相等)。买单出价够到最优卖价、
    * 卖单要价够到最优买价即成立。乐观侧 —— 不看量、不建模冲击，价内即全量成交。
    *
    * 也是 PostOnly 的拒单判据：到达即可吃单的 PostOnly 必须拒 (与真实交易所一致)。
    */
  def marketable(side: Side, limitPrice: Price, bbo: BBO): Boolean = side match
    case Side.Long  => bbo.askPrice <= limitPrice
    case Side.Short => bbo.bidPrice >= limitPrice

  /** resting 单是否被 BBO **严格穿越** (悲观, 不含相等)：买单在最优卖价**跌破**挂单价时成交、
    * 卖单在最优买价**升破**挂单价时成交。与 [[crossedByTrade]] 同一口径 —— 无论行情是 L1
    * 盘口还是逐笔成交，maker 成交判据都是"价格穿过去了"。
    */
  def crossedByBbo(side: Side, limitPrice: Price, bbo: BBO): Boolean = side match
    case Side.Long  => bbo.askPrice < limitPrice
    case Side.Short => bbo.bidPrice > limitPrice

  /** resting 单是否被一笔**真实成交**严格穿越 (逐笔撮合，不含相等)：
    * 买单在成交价**跌破**挂单价时成交、卖单在成交价**升破**挂单价时成交。
    */
  def crossedByTrade(side: Side, limitPrice: Price, tradePrice: Price): Boolean = side match
    case Side.Long  => tradePrice < limitPrice
    case Side.Short => tradePrice > limitPrice

  /** 主动成交 (taker) 的对手价：买单吃最优卖价，卖单吃最优买价 */
  def touchPrice(side: Side, bbo: BBO): Price = side match
    case Side.Long  => bbo.askPrice
    case Side.Short => bbo.bidPrice
