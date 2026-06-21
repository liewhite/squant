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
