package voltrade

/** 期权交易所边界 (隔离 Bybit 细节, 便于单测策略 / 换交易所)。错误以 Left(String) 显式返回, 不静默。 */
trait OptionsExchange:
  /** 标的最近 `bars` 根 5 分钟收盘价 (最旧->最新), 用于算 RV */
  def underlyingCloses5m(symbol: String, bars: Int): Either[String, Vector[Double]]

  /** 标的现价 (最新 5min 收盘) */
  def underlyingSpot(symbol: String): Either[String, Double]

  /** baseCoin (如 ETH) 的期权链 */
  def optionChain(baseCoin: String): Either[String, Vector[OptionInstrument]]

  /** 期权最优卖价 ask1 (卖方 PostOnly 挂单价：挂在卖一才是 maker, 挂买一会越价被拒)；无报价返回 None */
  def optionBestAsk(symbol: String): Either[String, Option[Double]]

  /** 卖出期权 (做空)。limitPrice=Some -> PostOnly 限价, None -> 市价。返回交易所 orderId (dry-run 返回合成 id)。 */
  def sellOption(symbol: String, qty: Double, limitPrice: Option[Double], orderLinkId: String): Either[String, String]

  /** 期权账户**净 (delta, gamma)** = Σ各期权持仓 delta/gamma (Bybit position/list 已按方向/张数给出)。需 API key。
    * 对冲腿据此把账户对冲到 delta 中性; gamma 供两次轮询之间用现价一阶修正 delta (tick 级新鲜)。 */
  def optionAccountGreeks(): Either[String, (Double, Double)]

  /** 永续 K 线 (category=linear) 的 (high, low, close), 最旧->最新, 供对冲带算 ATR/均线。 */
  def linearKlines(symbol: String, interval: String, bars: Int): Either[String, Vector[(Double, Double, Double)]]
