package demo

import com.github.plokhotnyuk.jsoniter_scala.core.*
import com.github.plokhotnyuk.jsoniter_scala.macros.*

/** 验证 jsoniter-scala 能否把「无参数的 Scala 3 enum case（即 case object 风格）」
  * 正确地序列化 / 反序列化为字符串。
  */
object JsoniterEnumDemo:

  /** 纯无参 case 的 enum —— 等价于 Scala 2 的 sealed trait + case object。 */
  enum Color:
    case Red, Green, Blue

  /** 包含 enum 字段的数据载体。 */
  case class Item(name: String, color: Color)

  // 关键：jsoniter-scala 默认把 Scala 3 enum 编码成带判别字段的对象 {"type":"Red"}。
  // 把 discriminatorFieldName 设为 None，无参 case 就会被编码为「纯字符串」"Red"。
  //
  // config 是宏参数必须内联成常量表达式，不能抽成普通 val；
  // 但可以封装进一个 inline def —— 内联会在每个调用点重建该常量表达式，
  // 于是配置只需写一次，各 enum 直接复用。
  inline def stringEnumCodec[A]: JsonValueCodec[A] =
    JsonCodecMaker.make(CodecMakerConfig.withDiscriminatorFieldName(None))

  given colorCodec: JsonValueCodec[Color]        = stringEnumCodec
  given itemCodec: JsonValueCodec[Item]          = stringEnumCodec
  given colorsCodec: JsonValueCodec[List[Color]] = stringEnumCodec

  def main(args: Array[String]): Unit =
    println("=== 1. 单个 enum case 序列化 ===")
    for c <- Color.values do
      val json = writeToString(c)
      println(s"$c -> $json")
      // 反序列化回来并校验
      val back = readFromString[Color](json)
      assert(back == c, s"round-trip 失败: $c != $back")

    println("\n=== 2. enum 列表序列化 ===")
    val colors        = List(Color.Red, Color.Blue, Color.Green, Color.Red)
    val colorsJson    = writeToString(colors)
    println(s"list  -> $colorsJson")
    val colorsBack    = readFromString[List[Color]](colorsJson)
    println(s"back  -> $colorsBack")
    assert(colorsBack == colors)

    println("\n=== 3. 嵌套在 case class 中的 enum ===")
    val item     = Item("apple", Color.Green)
    val itemJson = writeToString(item)
    println(s"item  -> $itemJson")
    val itemBack = readFromString[Item](itemJson)
    println(s"back  -> $itemBack")
    assert(itemBack == item)

    println("\n=== 4. 从手写 JSON 字符串反序列化 ===")
    val raw      = """{"name":"banana","color":"Blue"}"""
    val parsed   = readFromString[Item](raw)
    println(s"$raw -> $parsed")
    assert(parsed == Item("banana", Color.Blue))

    println("\n=== 5. 非法枚举值的错误处理 ===")
    try
      readFromString[Color](""""Purple"""")
      println("应当抛异常但没有！")
    catch
      case e: JsonReaderException =>
        println(s"按预期拒绝非法值: ${e.getMessage.linesIterator.next()}")

    println("\n全部断言通过 ✅ —— jsoniter-scala 可正确处理 enum case object <-> 字符串")
