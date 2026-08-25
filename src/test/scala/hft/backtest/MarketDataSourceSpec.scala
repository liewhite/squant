package hft.backtest

import hft.TestUnits.given
import hft.domain.{Coin, Exchange, MarketTrade}
import hft.event.{AnyEvent, Event, Topics}

/** 流式归并与有界重排序的单测 —— 回测"全局时间有序"契约的两块承重砖。 */
class MarketDataSourceSpec extends munit.FunSuite:
  private val ex = Exchange.Binance

  /** 用 symbol 标记事件来源, 便于断言同刻定序。 */
  private def ev(ts: Long, tag: String = "A"): AnyEvent =
    Event.stamped(Topics.Trade, MarketTrade(ex, tag, ts.toDouble, Coin(1.0), isBuyerMaker = false, ts), ts, ts)

  private def tss(it: Iterator[AnyEvent]): Vector[Long] = it.map(_.exchangeTs).toVector
  private def tags(it: Iterator[AnyEvent]): Vector[String] = it.flatMap(_.as(Topics.Trade)).map(_.symbol).toVector

  test("merge: 多条有序流合成一条全局有序流"):
    val a = Iterator(ev(1), ev(4), ev(9))
    val b = Iterator(ev(2), ev(3))
    val c = Iterator(ev(5), ev(11))
    assertEquals(tss(MarketDataSource.merge(Seq(a, b, c))), Vector(1L, 2L, 3L, 4L, 5L, 9L, 11L))

  test("merge: 同时间戳按输入流下标定序 (确定性, 不依赖哈希序)"):
    val first = Iterator(ev(5, "first"))
    val second = Iterator(ev(5, "second"))
    val third = Iterator(ev(5, "third"))
    assertEquals(tags(MarketDataSource.merge(Seq(first, second, third))), Vector("first", "second", "third"))

  test("merge: 空流与单流的边界"):
    assertEquals(tss(MarketDataSource.merge(Nil)), Vector.empty)
    assertEquals(tss(MarketDataSource.merge(Seq(Iterator.empty, Iterator.empty))), Vector.empty)
    assertEquals(tss(MarketDataSource.merge(Seq(Iterator.empty, Iterator(ev(7)), Iterator.empty))), Vector(7L))

  test("merge: 惰性 —— 不预读整条流 (取一个只推进必要的游标)"):
    var pulled = 0
    val counted = Iterator(ev(1), ev(2), ev(3)).map { e => pulled += 1; e }
    val merged = MarketDataSource.merge(Seq(counted, Iterator(ev(10))))
    merged.next()
    assert(pulled <= 2, s"取首个事件不应把整条流读完, pulled=$pulled")

  test("reorder: 窗口内的乱序被修正为升序"):
    val disordered = Iterator(ev(30), ev(10), ev(20), ev(5))
    assertEquals(tss(MarketDataSource.reorder(disordered, window = 4)), Vector(5L, 10L, 20L, 30L))

  test("reorder: 已有序的流原样通过 (含事件本身不变)"):
    assertEquals(tss(MarketDataSource.reorder(Iterator(ev(1), ev(2), ev(3)), window = 8)), Vector(1L, 2L, 3L))

  test("reorder: 同时间戳保持到达序 (稳定, 故确定)"):
    val same = Iterator(ev(5, "first"), ev(5, "second"), ev(5, "third"))
    assertEquals(tags(MarketDataSource.reorder(same, window = 4)), Vector("first", "second", "third"))

  test("reorder: 乱序跨度超过窗口则修不动 (如实暴露上界, 不假装万能)"):
    // window=2 只能吸收相邻互换; ts=1 在第 4 位, 已被更早产出的 20 甩在后面
    assertEquals(tss(MarketDataSource.reorder(Iterator(ev(30), ev(20), ev(10), ev(1)), window = 2)), Vector(20L, 10L, 1L, 30L))

  test("reorder: 空流与非法窗口"):
    assertEquals(tss(MarketDataSource.reorder(Iterator.empty, window = 4)), Vector.empty)
    intercept[IllegalArgumentException](MarketDataSource.reorder(Iterator(ev(1)), window = 0))
