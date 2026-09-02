package strategy.utils.option

/** 期权卖方实盘代码——**与 hft 回测/交易框架隔离**的独立 package (voltrade)。
  * 只复用 hft 里的纯数学工具 (RealizedVol/BlackScholes) 与底层 Bybit 签名原语, 不掺入 hft 交易引擎。 */

/** 看涨/看跌 —— **不另立一份**，就是 [[hft.option.OptionRight]]。
  *
  * 从前这里有一个同名 enum，与 BlackScholes 用的那个是两个互不相容的类型：同一个"看涨"
  * 概念有两个定义，跨边界就得写一次映射，而映射写反了编译器不会有任何反应 (Call/Put 两个
  * 分支形状相同)。类型别名让它们成为同一个类型，映射连存在的必要都没有。
  */
type OptionRight = hft.option.OptionRight
val OptionRight: hft.option.OptionRight.type = hft.option.OptionRight

/** 一个期权合约 (来自交易所 option chain)。
  * @param symbol   交易所原始符号 (Bybit: ETH-26SEP25-3000-C-USDT), 下单时原样回传, 不自行拼接避免格式错
  * @param expiryMs 交割时间 (ms epoch), 取自 instruments-info.deliveryTime (比解析日期串稳)
  * @param strike   行权价
  * @param right    Call/Put
  * @param minQty   最小下单量 (lotSizeFilter.minOrderQty)
  * @param qtyStep  下单量步长 (lotSizeFilter.qtyStep)
  * @param tickSize 价格最小变动 (priceFilter.tickSize)
  * @param ctVal    **每张对应多少标的** (OKX ctVal, 如 ETH 期权 0.01)。张数 -> 标的敞口的换算靠它，
  *                 故**无默认值**：给个 1.0 的"合理默认"在 ctVal≠1 的交易所上会把 delta 静默错算
  *                 一个整数倍，而这类错误没有任何外在症状
  */
final case class OptionInstrument(
    symbol: String,
    expiryMs: Long,
    strike: Double,
    right: OptionRight,
    ctVal: Double,
    minQty: Double,
    qtyStep: Double,
    tickSize: Double,
):
  // 三个精度参数**没有默认值**: 交易所一定会给 (给不出的合约在客户端就被整条跳过),
  // 而 `= 0.0` 会让"忘了填"编译通过, 再在下单时以 `require(tickSize > 0)` 的形式在实盘炸出来。
  require(minQty > 0, s"$symbol 最小下单量必须为正, 实际 $minQty")
  require(qtyStep > 0, s"$symbol 下单量步长必须为正, 实际 $qtyStep")
  require(tickSize > 0, s"$symbol 报价最小变动单位必须为正, 实际 $tickSize")

  /** 带符号张数 -> **币本位**标的量。张到币的换算只在这里写一次：写两处的话，改了其中一处
    * 不会有任何编译错误 —— 只是从此 delta 与名义价值按不同的倍数算，两个数各自看着都合理。 */
  def toCoin(contracts: Double): Double = contracts * ctVal

/** 期权盘口最优买卖价 (bid1/ask1)。供卖价决策: 价差 = ask−bid, 公允(中)价 = (bid+ask)/2。
  *
  * **两边都有正报价**是这个类型的不变量：只有单边报价的期权 (深度价外常见) 由
  * `OptionsExchange.optionQuote` 返回 `None`，而不是构造一个 `bid = 0` 的 Quote。
  * 判据放在类型上, 下游就不必各自防 —— 从前 `SellPlan` 写 `if quote.bid > 0 then ask/bid else Infinity`、
  * `sellQuote` 写 `require(mid > 0)`, 两处都在替一个上游已经排除的情况兜底。 */
final case class Quote(bid: Double, ask: Double):
  require(bid > 0, s"买一价必须为正, 实际 $bid (单边报价的期权应返回 None)")
  require(ask > 0, s"卖一价必须为正, 实际 $ask (单边报价的期权应返回 None)")

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

/** 一个期权合约的**标记读数** (交易所每腿各自给出)。
  *
  * @param symbol  期权 instId
  * @param markVol 标记隐含波动率 (年化, 1.0 = 100%)。卖出定量与 delta 计算都用它
  */
final case class OptionMark(symbol: String, markVol: Double)

/** 一笔期权持仓。
  *
  * @param symbol    期权 instId
  * @param contracts **带符号**张数 (正=多头, 负=空头)
  */
final case class OptionHolding(symbol: String, contracts: Double)

/** 账户的现金读数：净值 (杠杆闸门) 与某币种现金余额 (币本位期权的裸多头敞口)。
  *
  * 两者出自同一个账户接口，故一起返回 —— 分两次拉会看到两个时刻的账户，而杠杆率与对冲目标
  * 都是"此刻这个账户"的性质。
  *
  * @param equity      账户净值 (计价货币)
  * @param coinBalance 该币种**现金**余额 (不含期权未实现盈亏：那部分的价格敏感性已由 delta 描述，
  *                    再当成一笔静态余额对冲就是把同一份敞口算两遍)
  */
final case class OptionAccountCash(equity: Double, coinBalance: Double)
