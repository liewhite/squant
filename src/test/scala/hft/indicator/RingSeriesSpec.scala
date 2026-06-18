package hft.indicator

/** 有界历史序列单测：回看索引、容量上限、严格递减/递增判定。 */
class RingSeriesSpec extends munit.FunSuite:

  test("push / last / apply 回看索引"):
    val s = RingSeries(10)
    assertEquals(s.last, None)
    Seq(1.0, 2.0, 3.0).foreach(s.push)
    assertEquals(s.last, Some(3.0))
    assertEquals(s(0), Some(3.0)) // 最新
    assertEquals(s(1), Some(2.0))
    assertEquals(s(2), Some(1.0))
    assertEquals(s(3), None)

  test("容量上限丢弃最旧"):
    val s = RingSeries(3)
    (1 to 5).foreach(i => s.push(i.toDouble))
    assertEquals(s.values.toList, List(3.0, 4.0, 5.0))

  test("falling(n): 连续 n 周期严格递减"):
    val s = RingSeries(10)
    Seq(5.0, 4.0, 3.0).foreach(s.push) // 2 次递减
    assert(s.falling(2))
    assert(s.falling(1))
    assert(!s.falling(3)) // 只有 3 个值, 不足 4 个
    s.push(3.0)           // 持平打断递减
    assert(!s.falling(1)) // 3.0 !< 3.0

  test("rising(n): 连续 n 周期严格递增"):
    val s = RingSeries(10)
    Seq(1.0, 2.0, 4.0, 7.0).foreach(s.push)
    assert(s.rising(3))
    assert(!s.falling(1))

  test("数据不足 -> false"):
    val s = RingSeries(10)
    s.push(1.0)
    assert(!s.falling(1))
    assert(!s.rising(1))
