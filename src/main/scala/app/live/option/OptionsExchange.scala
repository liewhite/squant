package app.live.option

/** 期权交易所边界 (隔离 Bybit 细节, 便于单测策略 / 换交易所)。错误以 Left(String) 显式返回, 不静默。 */
trait OptionsExchange:
  /** 标的最近 `bars` 根 5 分钟收盘价 (最旧->最新), 用于算 RV */
  def underlyingCloses5m(symbol: String, bars: Int): Either[String, Vector[Double]]

  /** 标的现价 (最新 5min 收盘) */
  def underlyingSpot(symbol: String): Either[String, Double]

  /** baseCoin (如 ETH) 的期权链 */
  def optionChain(baseCoin: String): Either[String, Vector[OptionInstrument]]

  /** 期权盘口最优买卖价 (bid1/ask1)；任一边缺失/≤0 返回 None (无法两边定价)。卖价决策据此算价差/中价。 */
  def optionQuote(symbol: String): Either[String, Option[Quote]]

  /** 卖出期权 (做空, 真实限价下单)。postOnly=true -> 只做 maker (越价被拒); false -> taker (IOC, 立即成交)。
    * 返回交易所 orderId。 */
  def sellOption(symbol: String, qty: Double, price: Double, postOnly: Boolean, orderLinkId: String): Either[String, String]

  /** 期权账户**净 (delta, gamma)** = Σ各期权持仓 delta/gamma (Bybit position/list 已按方向/张数给出)。需 API key。
    * 对冲腿据此把账户对冲到 delta 中性; gamma 供两次轮询之间用现价一阶修正 delta (tick 级新鲜)。 */
  def optionAccountGreeks(): Either[String, (Double, Double)]

  /** 永续 K 线 (category=linear) 的 (high, low, close), 最旧->最新, 供对冲带算 ATR/均线。 */
  def linearKlines(symbol: String, interval: String, bars: Int): Either[String, Vector[(Double, Double, Double)]]
