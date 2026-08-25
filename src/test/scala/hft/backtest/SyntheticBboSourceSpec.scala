package hft.backtest

import hft.TestUnits.given
import hft.domain.{BBO, Coin, Exchange, MarketTrade}
import hft.event.{AnyEvent, Event, Topics}

/** 合成盘口装饰器：trade 后追加零价差 BBO；上游已带真实 BBO 则 fail-fast。 */
class SyntheticBboSourceSpec extends munit.FunSuite:
  private val ex = Exchange.Binance
  private val sym = "BTCUSDT"

  private def tradeEv(px: Double, ts: Long): AnyEvent =
    Event.stamped(Topics.Trade, MarketTrade(ex, sym, px, Coin(1.0), isBuyerMaker = false, ts), ts, ts)
  private def bboEv(ts: Long): AnyEvent =
    Event.stamped(Topics.Bbo, BBO(ex, sym, 100.0, Coin(1.0), 100.1, Coin(1.0), ts), ts, ts)

  private def sourceOf(evs: AnyEvent*): MarketDataSource =
    new MarketDataSource:
      def events(): Iterator[AnyEvent] = evs.iterator

  test("每条 trade 之后追加零价差 BBO, trade 本身仍留在流中"):
    val out = SyntheticBboSource(sourceOf(tradeEv(100.0, 1), tradeEv(101.0, 2))).events().toVector
    assertEquals(out.size, 4)
    assert(out(0).is(Topics.Trade) && out(1).is(Topics.Bbo))
    assert(out(2).is(Topics.Trade) && out(3).is(Topics.Bbo))
    val bbo = out(1).as(Topics.Bbo).get
    assertEquals(bbo.bidPrice.value, 100.0)
    assertEquals(bbo.askPrice.value, 100.0) // 零价差
    assertEquals(out(1).exchangeTs, 1L) // 与来源 trade 同刻, 不破坏升序

  test("非 trade 事件原样透传"):
    val markEv = Event.stamped(Topics.MarkPrice, hft.domain.MarkPrice(ex, sym, 100.0, 5), 5, 5)
    assertEquals(SyntheticBboSource(sourceOf(markEv)).events().toVector, Vector(markEv))

  test("上游已带真实 BBO -> 立即抛错, 绝不静默产出双重盘口"):
    val src = SyntheticBboSource(sourceOf(tradeEv(100.0, 1), bboEv(2)))
    val e = intercept[IllegalStateException](src.events().toVector)
    assert(e.getMessage.contains("real BBO"), e.getMessage)
