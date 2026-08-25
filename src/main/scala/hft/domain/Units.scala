package hft.domain

/** 价格 —— 每一单位 [[Coin]] 值多少计价货币。
  *
  * 与 [[Notional]] 分开：价格是**强度**（每单位多少钱），名义额是**总量**（一共多少钱）。
  * 两者都是 double，混用编译器从前不管 —— 而 `qty.notional(price)` 的结果被当成价格接着用，
  * 是那种跑得下去、数字却毫无意义的错误。
  *
  * 与 [[Coin]] 一样不设 `<: Double` 上界，理由见下。
  */
opaque type Price = Double

object Price:
  inline def apply(d: Double): Price = d
  val Zero: Price = 0.0

  given Ordering[Price] = Ordering.Double.TotalOrdering

  extension (p: Price)
    /** 解包成裸 double —— 只在格式化输出、喂给通用数值计算（指标等）时用 */
    inline def value: Double = p
    inline def +(o: Price): Price = p + o
    inline def -(o: Price): Price = p - o
    inline def unary_- : Price = -p
    inline def abs: Price = math.abs(p)
    inline def min(o: Price): Price = math.min(p, o)
    inline def max(o: Price): Price = math.max(p, o)
    inline def <(o: Price): Boolean = (p: Double) < (o: Double)
    inline def <=(o: Price): Boolean = (p: Double) <= (o: Double)
    inline def >(o: Price): Boolean = (p: Double) > (o: Double)
    inline def >=(o: Price): Boolean = (p: Double) >= (o: Double)
    inline def isZero: Boolean = math.abs(p) < 1e-12
    inline def isNaN: Boolean = (p: Double).isNaN
    /** 按比例缩放（挂单偏移、滑点等），仍是价格。
      * 只接受裸 double 系数 —— 两个 Price 相乘没有意义，而 opaque 在外部不与 Double 兼容，
      * 所以 `price * price` 根本写不出来。 */
    inline def scaled(k: Double): Price = p * k
    inline def *(k: Double): Price = p * k
    inline def /(k: Double): Price = p / k
    /** 两个价格之比 —— 跨概念相除，结果是无量纲的倍数 */
    inline def ratioTo(o: Price): Double = (p: Double) / (o: Double)

  /** 两个价格的中点 */
  inline def mid(a: Price, b: Price): Price = (a + b) / 2.0

/** 币本位数量 —— **框架内唯一的数量表示**：策略、账本、仓位、回报、撮合全用它。
  *
  * 与 [[Contracts]] 分开是因为这两个数字长得一模一样（都是 double）却不能互换，而混用没有
  * 任何外在症状：在 `contractSize = 1` 的交易所上完全正确，换到别的交易所就整体差一个倍数。
  * 这类 bug 靠测试很难抓 —— 除非恰好用了 `contractSize ≠ 1` 的标的。
  *
  * ## 为什么不设 `<: Double` 上界
  *
  * 设了上界虽然能直接参与算术，但 `Coin + Coin` 会优先匹配 `Double` 的 `+` 并悄悄退化成
  * `Double`，等于白设这个类型。所以宁可要求跨概念运算显式解包 —— `qty.notional(price)`
  * 而不是 `qty * price`，因为"数量 × 价格"的结果本就不再是数量。
  *
  * 运行时零开销：opaque type 编译后就是 `double`，没有装箱、没有包装对象。
  */
opaque type Coin = Double

/** 合约张数 —— 交易所侧的数量单位。
  *
  * **只应出现在 exchange 适配层内部**：下单前从 [[Coin]] 换过去、回报解析时换回来。
  * 它一旦泄漏进框架其余部分，就说明某处边界没守住。
  */
opaque type Contracts = Double

object Coin:
  inline def apply(d: Double): Coin = d
  val Zero: Coin = 0.0

  given Ordering[Coin] = Ordering.Double.TotalOrdering

  extension (c: Coin)
    /** 解包成裸 double —— 只在格式化输出、跨概念运算时用 */
    inline def value: Double = c
    inline def +(o: Coin): Coin = c + o
    inline def -(o: Coin): Coin = c - o
    inline def unary_- : Coin = -c
    inline def abs: Coin = math.abs(c)
    inline def min(o: Coin): Coin = math.min(c, o)
    inline def max(o: Coin): Coin = math.max(c, o)
    inline def <(o: Coin): Boolean = (c: Double) < (o: Double)
    inline def <=(o: Coin): Boolean = (c: Double) <= (o: Double)
    inline def >(o: Coin): Boolean = (c: Double) > (o: Double)
    inline def >=(o: Coin): Boolean = (c: Double) >= (o: Double)
    inline def isZero: Boolean = math.abs(c) < Position.Epsilon
    inline def nonZero: Boolean = math.abs(c) >= Position.Epsilon
    inline def signum: Int = math.signum(c).toInt
    /** 名义额 = 数量 × 价格。跨概念相乘，结果不再是数量 */
    inline def notional(price: Price): Notional = Notional(c * price.value)
    /** 盈亏 = 带符号数量 × (标记价 − 成本价)。
      *
      * 单独命名而不是复用 [[notional]]：`notional(mark - entry)` 是把一个**价差**当**价格**
      * 传进去，算得对但概念是混的 —— 名义额与盈亏不是一回事，量纲相同不代表可以互相顶替。 */
    inline def pnl(entry: Price, mark: Price): Notional = Notional(c * (mark - entry).value)
    /** 按比例缩放（杠杆、分档等），仍是数量 */
    inline def scaled(k: Double): Coin = c * k
    inline def isNaN: Boolean = (c: Double).isNaN

  extension (xs: IterableOnce[Coin]) def sumCoin: Coin = xs.iterator.foldLeft(Coin.Zero)(_ + _)

/** 名义额（计价货币金额，本框架里就是 USDT）。
  *
  * 与 [[Price]] 分开的理由与 [[Coin]] / [[Contracts]] 完全同源：两者都是 double，混用没有
  * 任何外在症状。`qty.notional(price)` 从前返回裸 double，于是它可以被当成一个价格接着用下去
  * —— 编译器不吭声，跑起来是一个量纲错误的数字在账本里游荡。
  *
  * 净值、名义敞口、手续费、成交额都是它。
  */
opaque type Notional = Double

object Notional:
  inline def apply(d: Double): Notional = d
  val Zero: Notional = 0.0

  given Ordering[Notional] = Ordering.Double.TotalOrdering

  extension (n: Notional)
    inline def value: Double = n
    inline def +(o: Notional): Notional = n + o
    inline def -(o: Notional): Notional = n - o
    inline def unary_- : Notional = -n
    inline def abs: Notional = math.abs(n)
    inline def min(o: Notional): Notional = math.min(n, o)
    inline def max(o: Notional): Notional = math.max(n, o)
    inline def <(o: Notional): Boolean = (n: Double) < (o: Double)
    inline def <=(o: Notional): Boolean = (n: Double) <= (o: Double)
    inline def >(o: Notional): Boolean = (n: Double) > (o: Double)
    inline def >=(o: Notional): Boolean = (n: Double) >= (o: Double)
    inline def isZero: Boolean = math.abs(n) < Position.Epsilon
    /** 按比例缩放（仓位比例、费率等），仍是名义额 */
    inline def scaled(k: Double): Notional = n * k
    inline def *(k: Double): Notional = n * k
    inline def /(k: Double): Notional = n / k
    /** 名义额 ÷ 价格 = 数量。跨概念相除，结果不再是名义额 */
    inline def quantityAt(price: Price): Coin = Coin(n / price.value)
    /** 名义额 ÷ 数量 = 单价。用于加权平均成本一类的换算 */
    inline def pricePer(qty: Coin): Price = Price(n / qty.value)
    inline def isNaN: Boolean = (n: Double).isNaN

  extension (xs: IterableOnce[Notional]) def sumNotional: Notional = xs.iterator.foldLeft(Notional.Zero)(_ + _)

object Contracts:
  inline def apply(d: Double): Contracts = d
  extension (q: Contracts)
    inline def value: Double = q
    inline def >(o: Contracts): Boolean = (q: Double) > (o: Double)
    inline def >=(o: Contracts): Boolean = (q: Double) >= (o: Double)
