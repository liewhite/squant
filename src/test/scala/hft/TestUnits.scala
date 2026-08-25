package hft

import hft.domain.{Coin, Notional, Price}

/** 测试专用：把字面量读作带单位的量（币本位数量 / 价格 / 名义额）。
  *
  * 测试里的数字是作者直接写下的，单位由意图决定，不存在"这个数是交易所给的张数还是币"
  * 这种歧义 —— 那种歧义只发生在解析外部数据的边界上，而边界代码在主源码里，不受本文件影响。
  *
  * **它不会削弱单位检查**：只提供 `Double -> X` 这一个方向，各单位**彼此之间**依旧不可互换
  * —— 把张数当币本位、把名义额当价格，仍然编译失败。真正要验证换算的用例
  * （如 `contractSize ≠ 1`）照旧显式写 `Contracts(...)` / `Coin(...)`。
  *
  * 主源码里没有这些转换：解析外部数据的边界必须显式说明"这个数是什么"，那正是单位类型
  * 要守住的地方。
  */
object TestUnits:
  import scala.language.implicitConversions
  given Conversion[Double, Coin] = Coin(_)
  given Conversion[Int, Coin] = i => Coin(i.toDouble)
  given Conversion[Double, Price] = Price(_)
  given Conversion[Int, Price] = i => Price(i.toDouble)
  given Conversion[Double, Notional] = Notional(_)
