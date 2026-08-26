package hft.domain

/** 单位类型上 `isNaN` 的回归测试。
  *
  * 这三个方法从前是**递归定义**的：opaque type 在自己的伴生对象内部是透明的 (就是 `Double`)，
  * 而 `Double.isNaN` 是经隐式转换到 `RichDouble` 拿到的，于是本扩展方法优先级更高、
  * `x.isNaN` 解析回它自己。因为是 `inline`，症状不是运行时栈溢出，而是**调用点编译失败**
  * ("Maximal number of successive inlines exceeded")，即这个方法压根调不动。
  *
  * 全仓当时没有一个调用点，所以编译一直是绿的 —— 只有编译器的 "Infinite loop in function body"
  * warning 在提示。本文件的价值就在于**存在调用点**：谁再把它写回递归形式，这里立刻编译失败。
  */
class UnitsSpec extends munit.FunSuite:

  test("Coin.isNaN 可调用且判定正确"):
    assertEquals(Coin(1.0).isNaN, false)
    assertEquals(Coin(0.0).isNaN, false)
    assertEquals(Coin(Double.NaN).isNaN, true)

  test("Price.isNaN 可调用且判定正确"):
    assertEquals(Price(3000.0).isNaN, false)
    assertEquals(Price(Double.NaN).isNaN, true)

  test("Notional.isNaN 可调用且判定正确"):
    assertEquals(Notional(1e6).isNaN, false)
    assertEquals(Notional(Double.NaN).isNaN, true)

  test("无穷不是 NaN (别把两种非常规值混为一谈)"):
    assertEquals(Coin(Double.PositiveInfinity).isNaN, false)
    assertEquals(Price(Double.NegativeInfinity).isNaN, false)

  test("isNaN 与 isZero 是两件事: NaN 不是零"):
    assertEquals(Coin(Double.NaN).isZero, false)
    assertEquals(Coin(0.0).isZero, true)
