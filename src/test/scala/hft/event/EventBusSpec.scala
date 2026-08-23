package hft.event

import hft.domain.*
import ox.supervised
import hft.TestUnits.given

/** 总线投递: 按 (topic, key) 精确路由，且与 [[Subscription.accepts]] 判据同源。 */
class EventBusSpec extends munit.FunSuite:
  private val ex = Exchange.Binance
  private val btc = Instrument(ex, "BTCUSDT")
  private val eth = Instrument(ex, "ETHUSDT")
  private val t0 = 1_700_000_000_000L

  private def bboOf(i: Instrument) = BBO(i.exchange, i.symbol, 100.0, Coin(1.0), 100.1, Coin(1.0), t0)
  private def tradeOf(i: Instrument) = MarketTrade(i.exchange, i.symbol, 1.0, 1.0, isBuyerMaker = false, t0)

  test("按 key 定向: 只有订阅了该标的的订阅者收到"):
    supervised:
      val bus = EventBus()
      val onBtc = bus.subscribe(Set(Interest.Keyed(Topics.Bbo, Set(btc))))
      val onEth = bus.subscribe(Set(Interest.Keyed(Topics.Bbo, Set(eth))))

      bus.publish(Event.at(Topics.Bbo, bboOf(btc), t0))

      assertEquals(onBtc.events.receive().as(Topics.Bbo).map(_.symbol), Some("BTCUSDT"))
      // onEth 不该收到：用一条它确实订阅的事件作栅栏，若它先收到 BTC 这里就会失败
      bus.publish(Event.at(Topics.Bbo, bboOf(eth), t0))
      assertEquals(onEth.events.receive().as(Topics.Bbo).map(_.symbol), Some("ETHUSDT"))

  test("按 topic 隔离: 同一个 key 上不同 topic 互不投递"):
    supervised:
      val bus = EventBus()
      val onBbo = bus.subscribe(Set(Interest.Keyed(Topics.Bbo, Set(btc))))
      bus.publish(Event.at(Topics.Trade, tradeOf(btc), t0))
      bus.publish(Event.at(Topics.Bbo, bboOf(btc), t0))
      assert(onBbo.events.receive().is(Topics.Bbo), "Trade 不该进 Bbo 订阅者的邮箱")

  test("Interest.All 收该 topic 的全部 key"):
    supervised:
      val bus = EventBus()
      val all = bus.subscribe(Set(Interest.All(Topics.Bbo)))
      bus.publish(Event.at(Topics.Bbo, bboOf(btc), t0))
      bus.publish(Event.at(Topics.Bbo, bboOf(eth), t0))
      assertEquals(all.events.receive().as(Topics.Bbo).map(_.symbol), Some("BTCUSDT"))
      assertEquals(all.events.receive().as(Topics.Bbo).map(_.symbol), Some("ETHUSDT"))

  test("同 topic 上 All + Keyed 只投递一次"):
    supervised:
      val bus = EventBus()
      val both = bus.subscribe(Set(Interest.All(Topics.Bbo), Interest.Keyed(Topics.Bbo, Set(btc))))
      bus.publish(Event.at(Topics.Bbo, bboOf(btc), t0))
      // 第二条作栅栏: 若第一条被投了两次，这里读到的会是重复的 BTC 而不是 ETH
      bus.publish(Event.at(Topics.Bbo, bboOf(eth), t0))
      assertEquals(both.events.receive().as(Topics.Bbo).map(_.symbol), Some("BTCUSDT"))
      assertEquals(both.events.receive().as(Topics.Bbo).map(_.symbol), Some("ETHUSDT"))

  test("同 topic 上多条 Keyed 声明的 key 相交时, 交集只投一次"):
    // 极易发生: 策略自己声明了某标的的私有回报, 框架又补一条覆盖全部标的的 ——
    // 两个 Interest 不相等 (Set 去重不了) 但 key 相交。投两次 = 一条 Fill 消费两次 = 仓位翻倍。
    supervised:
      val bus = EventBus()
      val sub = bus.subscribe(Set(
        Interest.Keyed(Topics.Fill, Set(AccountInstrument(AccountId.Live, btc))),
        Interest.Keyed(Topics.Fill, Set(AccountInstrument(AccountId.Live, btc), AccountInstrument(AccountId.Live, eth))),
      ))
      val btcFill = Fill(AccountId.Live, ex, "BTCUSDT", Side.Long, 100.0, Coin(1.0), t0)
      bus.publish(Event.local(Topics.Fill, btcFill))
      // 栅栏: 若上一条被投了两次, 这里读到的会是重复的 BTC 而不是 ETH
      bus.publish(Event.local(Topics.Fill, Fill(AccountId.Live, ex, "ETHUSDT", Side.Long, 100.0, Coin(1.0), t0)))
      assertEquals(sub.events.receive().as(Topics.Fill).map(_.symbol), Some("BTCUSDT"))
      assertEquals(sub.events.receive().as(Topics.Fill).map(_.symbol), Some("ETHUSDT"))

  test("用户自定义 topic: 框架零改动即可路由"):
    supervised:
      val bus = EventBus()
      val onBtc = bus.subscribe(Set(Interest.Keyed(EventBusSpec.AlphaSignal, Set(btc))))
      bus.publish(Event.local(EventBusSpec.AlphaSignal, EventBusSpec.Score(btc, 0.7)))
      assertEquals(onBtc.events.receive().as(EventBusSpec.AlphaSignal).map(_.value), Some(0.7))

  test("无人订阅的 topic: publish 不抛错"):
    supervised:
      val bus = EventBus()
      bus.publish(Event.at(Topics.Bbo, bboOf(btc), t0))

  test("退订后不再收到事件"):
    supervised:
      val bus = EventBus()
      val sub = bus.subscribe(Set(Interest.Keyed(Topics.Bbo, Set(btc))))
      bus.publish(Event.at(Topics.Bbo, bboOf(btc), t0))
      assertEquals(sub.events.receive().as(Topics.Bbo).map(_.symbol), Some("BTCUSDT"))
      sub.close()
      bus.publish(Event.at(Topics.Bbo, bboOf(btc), t0))
      // 若仍在索引里, 这条会进它的无界邮箱 —— 动态起停场景下就是一条稳定的内存泄漏
      assertEquals(bus.subscriberCount(Topics.Bbo, btc), 0, "退订后索引里不该再有它")

  test("退订幂等, 且不影响别的订阅者"):
    supervised:
      val bus = EventBus()
      val a = bus.subscribe(Set(Interest.Keyed(Topics.Bbo, Set(btc))))
      val b = bus.subscribe(Set(Interest.Keyed(Topics.Bbo, Set(btc))))
      a.close()
      a.close() // 幂等: 停机路径上重复调用是常态
      assertEquals(bus.subscriberCount(Topics.Bbo, btc), 1, "b 仍应在索引里")
      bus.publish(Event.at(Topics.Bbo, bboOf(btc), t0))
      assertEquals(b.events.receive().as(Topics.Bbo).map(_.symbol), Some("BTCUSDT"))

  test("投递索引与 Subscription.accepts 同源"):
    // 索引是判据的扇出优化，不是第二份判据。两者若错开，失效方式是某订阅者静默收不到事件，
    // 所以这里对同一批事件逐条比对"总线投了没"与"判据说该不该收"。
    val interests: Set[Interest] = Set(
      Interest.Keyed(Topics.Bbo, Set(btc)),
      Interest.All(Topics.Clock),
      Interest.Keyed(Topics.AccountInfo, Set(AccountExchange(AccountId.Live, Exchange.Binance))),
    )
    val sub = Subscription(interests)
    val events: Vector[AnyEvent] = Vector(
      Event.at(Topics.Bbo, bboOf(btc), t0),
      Event.at(Topics.Bbo, bboOf(eth), t0),
      Event.at(Topics.Trade, tradeOf(btc), t0),
      Topics.clockAt(t0),
      Event.local(Topics.AccountInfo, AccountInfo(AccountId.Live, Exchange.Binance, 1.0, 0.0)),
      Event.local(Topics.AccountInfo, AccountInfo(AccountId.Live, Exchange.Okx, 1.0, 0.0)),
      Event.local(Topics.Balance, Balance(AccountId.Live, Exchange.Binance, "USDT", 1.0, t0)),
    )
    supervised:
      val bus = EventBus()
      val src = bus.subscribe(interests)
      events.foreach(bus.publish)
      val expected = events.filter(sub.accepts)
      val delivered = (1 to expected.size).map(_ => src.events.receive()).toVector
      assertEquals(delivered, expected)

object EventBusSpec:
  /** 用户域的自定义事件族: 自带路由键类型与载荷类型，框架不需要知道它的存在 */
  final case class Score(instrument: Instrument, value: Double)
  object AlphaSignal extends Topic[Instrument, Score]("alphaSignal"):
    def keyOf(p: Score): Instrument = p.instrument
