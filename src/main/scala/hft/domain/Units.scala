package hft.domain

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
    inline def notional(price: Price): Double = c * price
    /** 按比例缩放（杠杆、分档等），仍是数量 */
    inline def scaled(k: Double): Coin = c * k
    inline def isNaN: Boolean = (c: Double).isNaN

  extension (xs: IterableOnce[Coin]) def sumCoin: Coin = xs.iterator.foldLeft(Coin.Zero)(_ + _)

object Contracts:
  inline def apply(d: Double): Contracts = d
  extension (q: Contracts)
    inline def value: Double = q
    inline def >(o: Contracts): Boolean = (q: Double) > (o: Double)
    inline def >=(o: Contracts): Boolean = (q: Double) >= (o: Double)
