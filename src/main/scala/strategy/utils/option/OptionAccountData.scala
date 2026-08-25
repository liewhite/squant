package strategy.utils.option

/** **声明式对账与实时 delta 所需的账户/标记读数** —— 与 [[OptionsExchange]] 并列的一条边界。
  *
  * 为什么不直接加进 [[OptionsExchange]]：那个 trait 有两个实现 (OKX / Bybit)，加方法就要求
  * Bybit 也实现，而当前只有 OKX 这条路在跑。给 Bybit 塞几个 `Left("未实现")` 是虚假抽象 ——
  * 一个声称实现了接口、却在一半方法上表态"我不会"的实现，调用方读接口读不出这件事。
  * 需要两者的组件把依赖写成 `OptionsExchange & OptionAccountData`，缺哪半边编译期就知道。
  *
  * 错误一律以 `Left(String)` 显式返回，不静默。
  */
trait OptionAccountData:
  /** 该基础币下**每腿的标记读数** (标记 IV)。卖出定量与 delta 计算的同一次取数 ——
    * 分两次拉会拿到两个时刻的 IV，而"卖多少"和"敞口多大"应当基于同一份观测。 */
  def optionMarks(baseCoin: String): Either[String, Vector[OptionMark]]

  /** 该基础币下的**期权持仓** (带符号张数)。声明式对账的左值：目标张数与它比，差多少补多少。
    * 无持仓返回空 Vector (不是错误)。 */
  def optionPositions(baseCoin: String): Either[String, Vector[OptionHolding]]

  /** 标的**最新成交价**。delta 每秒重算靠它 —— [[OptionsExchange.underlyingSpot]] 取的是
    * 5 分钟 K 线收盘，最多滞后一整根 bar，用来算实时 delta 会让敞口读数系统性落后于行情。 */
  def underlyingLast(symbol: String): Either[String, Double]

  /** 账户净值 + 该币种现金余额 (一次取数，见 [[OptionAccountCash]]) */
  def accountCash(ccy: String): Either[String, OptionAccountCash]
