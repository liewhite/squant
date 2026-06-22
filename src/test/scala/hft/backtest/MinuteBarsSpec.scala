package hft.backtest

import hft.domain.{Exchange, MarketTrade}
import hft.messaging.{EventData, IncomeEvent}

class MinuteBarsSpec extends munit.FunSuite:

  private def trade(ts: Long, px: Double, qty: Double): IncomeEvent =
    IncomeEvent(ts, ts, EventData.MarketTradeUpdate(MarketTrade(Exchange.Binance, "ETHUSDT", px, qty, isBuyerMaker = false, ts)))

  private def sourceOf(evs: IncomeEvent*): MarketDataSource =
    new MarketDataSource:
      override def events(): Iterator[IncomeEvent] = evs.iterator

  test("aggregate: 按分钟分桶取 OHLCV, 跨桶 flush"):
    val src = sourceOf(
      // bucket 0 (ts 0..59999)
      trade(0, 102.0, 1.0), trade(30_000, 105.0, 2.0), trade(59_000, 100.0, 1.0), trade(59_500, 104.0, 1.0),
      // bucket 1 (ts 60000..)
      trade(60_000, 101.0, 1.0), trade(90_000, 98.0, 1.0),
    )
    val tmp = java.io.File.createTempFile("bars", ".csv").getAbsolutePath
    val n = MinuteBars.aggregate(src, tmp)
    assertEquals(n, 2L)
    val bars = MinuteBars.load(tmp)
    assertEquals(bars.length, 2)
    assertEquals(bars(0), MinuteBar(0L, 102.0, 105.0, 100.0, 104.0, 5.0))
    assertEquals(bars(1), MinuteBar(60_000L, 101.0, 101.0, 98.0, 98.0, 2.0))

  test("aggregate: 稀疏 (无成交分钟跳过), bar 数 = 有成交的桶数"):
    val src = sourceOf(trade(0, 10.0, 1.0), trade(180_000, 11.0, 1.0)) // 桶0 与 桶3, 中间空
    val tmp = java.io.File.createTempFile("bars", ".csv").getAbsolutePath
    assertEquals(MinuteBars.aggregate(src, tmp), 2L)
    val bars = MinuteBars.load(tmp)
    assertEquals(bars.map(_.ts), Vector(0L, 180_000L))

  test("replay: 每根 bar 发 4 个 trade, 时间戳 +0/15/30/45s"):
    val bars = Vector(MinuteBar(60_000L, 102.0, 105.0, 100.0, 104.0, 8.0))
    val evs = MinuteBarReplaySource(bars, Exchange.Binance, "ETHUSDT").events().toVector
    assertEquals(evs.length, 4)
    assertEquals(evs.map(_.exchangeTs), Vector(60_000L, 75_000L, 90_000L, 105_000L))

  test("replay: 先触及离开盘价更近的极值 (开盘价更近低点 → O,L,H,C)"):
    // o=102 离 low=100 (距2) 比离 high=105 (距3) 近 → 路径 O→L→H→C
    val bars = Vector(MinuteBar(0L, 102.0, 105.0, 100.0, 104.0, 8.0))
    val px = MinuteBarReplaySource(bars, Exchange.Binance, "ETHUSDT").events().toVector.map {
      _.data match
        case EventData.MarketTradeUpdate(t) => t.price
        case _                              => Double.NaN
    }
    assertEquals(px, Vector(102.0, 100.0, 105.0, 104.0))

  test("replay: 跨 bar 时间严格递增 (满足回测全局有序)"):
    val bars = Vector(
      MinuteBar(0L, 10.0, 11.0, 9.0, 10.5, 4.0),
      MinuteBar(60_000L, 10.5, 12.0, 10.0, 11.5, 4.0),
      MinuteBar(180_000L, 11.5, 11.5, 10.0, 10.2, 4.0), // 稀疏: 跳过 120000
    )
    val ts = MinuteBarReplaySource(bars, Exchange.Binance, "ETHUSDT").events().toVector.map(_.exchangeTs)
    assertEquals(ts.length, 12)
    assert(ts.sliding(2).forall(p => p(0) < p(1)), s"timestamps not strictly increasing: $ts")
