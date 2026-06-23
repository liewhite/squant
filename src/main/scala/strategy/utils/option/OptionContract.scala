package strategy.utils.option

/** 期权卖方实盘代码——**与 hft 回测/交易框架隔离**的独立 package (voltrade)。
  * 只复用 hft 里的纯数学工具 (RealizedVol/BlackScholes) 与底层 Bybit 签名原语, 不掺入 hft 交易引擎。 */

/** 看涨/看跌 */
enum OptionRight:
  case Call, Put

/** 一个期权合约 (来自交易所 option chain)。
  * @param symbol   交易所原始符号 (Bybit: ETH-26SEP25-3000-C-USDT), 下单时原样回传, 不自行拼接避免格式错
  * @param expiryMs 交割时间 (ms epoch), 取自 instruments-info.deliveryTime (比解析日期串稳)
  * @param strike   行权价
  * @param right    Call/Put
  * @param minQty   最小下单量 (lotSizeFilter.minOrderQty)
  * @param qtyStep  下单量步长 (lotSizeFilter.qtyStep)
  * @param tickSize 价格最小变动 (priceFilter.tickSize)
  */
final case class OptionInstrument(
    symbol: String,
    expiryMs: Long,
    strike: Double,
    right: OptionRight,
    minQty: Double = 0.0,
    qtyStep: Double = 0.0,
    tickSize: Double = 0.0,
)

/** 期权盘口最优买卖价 (bid1/ask1)。供卖价决策: 价差 = ask−bid, 公允(中)价 = (bid+ask)/2。 */
final case class Quote(bid: Double, ask: Double):
  def mid: Double = (bid + ask) / 2.0
  def spread: Double = ask - bid

object OptionContract:
  /** 从 Bybit 期权符号解析 (base, strike, right)；兼容币本位 `BASE-EXPIRY-STRIKE-{C|P}` 与
    * USDT 结算 `BASE-EXPIRY-STRIKE-{C|P}-USDT` (Bybit ETH/BTC 期权实际为后者)。expiry 不从符号解析
    * (用 deliveryTime 字段), 此处只取 strike/right。解析失败返回 None。 */
  def parseSymbol(symbol: String): Option[(String, Double, OptionRight)] =
    val parts = symbol.split("-")
    val core = parts match
      case Array(base, _, strikeStr, rt)    => Some((base, strikeStr, rt)) // 币本位
      case Array(base, _, strikeStr, rt, _) => Some((base, strikeStr, rt)) // -USDT 结算
      case _                                => None
    core.flatMap { case (base, strikeStr, rt) =>
      val right = rt.toUpperCase match
        case "C" => Some(OptionRight.Call)
        case "P" => Some(OptionRight.Put)
        case _   => None
      strikeStr.toDoubleOption.zip(right).map { case (k, r) => (base, k, r) }
    }
