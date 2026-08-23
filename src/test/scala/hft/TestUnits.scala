package hft

import hft.domain.Coin

/** 测试专用：把字面量读作币本位数量。
  *
  * 测试里的数字是作者直接写下的，单位由意图决定，不存在"这个数是交易所给的张数还是币"
  * 这种歧义 —— 那种歧义只发生在解析外部数据的边界上，而边界代码在主源码里，不受本文件影响。
  *
  * **它不会削弱单位检查**：只提供 `Double -> Coin`，[[hft.domain.Contracts]] 与
  * [[Coin]] 之间依旧不可互换，把张数当币本位仍然编译失败。真正要验证换算的用例
  * （如 `contractSize ≠ 1`）照旧显式写 `Contracts(...)` / `Coin(...)`。
  */
object TestUnits:
  import scala.language.implicitConversions
  given Conversion[Double, Coin] = Coin(_)
  given Conversion[Int, Coin] = i => Coin(i.toDouble)
