package hft.engine

import hft.domain.*
import hft.event.{AnyEvent, Event, Interest, Subscription, Topics}
import hft.exchange.SubscriptionKind
import hft.state.StateManager
import hft.strategy.{OutcomeEvent, Strategy}
import hft.TestUnits.given

/** 策略订阅范围的派生: 框架补齐了什么、又据此向交易所订了什么。 */
class StrategyRunnerSpec extends munit.FunSuite:
  private val ex = Exchange.Binance
  private val btc = Instrument(ex, "BTCUSDT")

  /** 直接喂声明，绕过策略实例 —— 这里测的是"框架据声明补齐了什么" */
  private def subOf(interests: Set[Interest]): Subscription =
    StrategyRunner.subscriptionFor(interests, AccountId.Live)

  test("补齐所声明标的的私有回报 —— 策略不该有机会漏订成交"):
    val sub = subOf(Set(Interest.Keyed(Topics.Bbo, Set(btc))))
    val fill = Event.local(Topics.Fill, Fill(AccountId.Live, ex, "BTCUSDT", Side.Long, 100.0, Coin(1.0), 0L))
    val position = Event.local(Topics.Position, Position(AccountId.Live, ex, "BTCUSDT", 1.0, 100.0, 0.0))
    val orderUpdate = Event.local(
      Topics.OrderUpdate,
      OrderUpdate(AccountId.Live, "1", Some("c1"), ex, "BTCUSDT", Side.Long, OrderStatus.Filled, 100.0, Coin(1.0), Coin(1.0), Coin(1.0), 0L),
    )
    assert(sub.accepts(fill), "Fill 必须自动补齐: 漏订会让本地仓位与交易所长期发散")
    assert(sub.accepts(position))
    assert(sub.accepts(orderUpdate))

  test("补齐所涉交易所的账户级读数, 但不越界到别的交易所"):
    val sub = subOf(Set(Interest.Keyed(Topics.Bbo, Set(btc))))
    assert(sub.accepts(Event.local(Topics.AccountInfo, AccountInfo(AccountId.Live, ex, 1.0, 0.0))))
    assert(sub.accepts(Event.local(Topics.Balance, Balance(AccountId.Live, ex, "USDT", 1.0, 0L))))
    assert(sub.accepts(Event.local(Topics.Greeks, Greeks(AccountId.Live, ex, "BTC", 0.0, 0.0, 0.0, 0.0, 0L))))
    assert(
      !sub.accepts(Event.local(Topics.AccountInfo, AccountInfo(AccountId.Live, Exchange.Okx, 1.0, 0.0))),
      "未订阅交易所的净值不该到达策略 —— 杠杆闸门就是拿它算的",
    )

  test("补齐时钟 —— 订单超时检测由它驱动"):
    assert(subOf(Set(Interest.Keyed(Topics.Bbo, Set(btc)))).accepts(Topics.clockAt(0L)))

  test("不补齐未声明标的的任何东西"):
    val sub = subOf(Set(Interest.Keyed(Topics.Bbo, Set(btc))))
    val ethFill = Event.local(Topics.Fill, Fill(AccountId.Live, ex, "ETHUSDT", Side.Long, 100.0, Coin(1.0), 0L))
    assert(!sub.accepts(ethFill))

  test("无任何声明的策略只收时钟"):
    val sub = subOf(Set.empty)
    assert(sub.accepts(Topics.clockAt(0L)))
    assert(!sub.accepts(Event.local(Topics.AccountInfo, AccountInfo(AccountId.Live, ex, 1.0, 0.0))))

  test("行情订阅由同一份声明派生 —— 一处声明, 两处派生"):
    val sub = subOf(Set(
      Interest.Keyed(Topics.Bbo, Set(btc)),
      Interest.Keyed(Topics.Trade, Set(Instrument(Exchange.Okx, "ETHUSDT"))),
    ))
    assertEquals(
      SubscriptionKind.from(sub),
      Set[(Exchange, SubscriptionKind)](
        (ex, SubscriptionKind.BBO("BTCUSDT")),
        (Exchange.Okx, SubscriptionKind.Trade("ETHUSDT")),
      ),
    )

  test("补齐的私有回报不会被误当成要订阅的行情流"):
    // Position/OrderUpdate/Fill 由账户流推送，不该出现在向交易所下的行情订阅里
    val kinds = SubscriptionKind.from(subOf(Set(Interest.Keyed(Topics.Bbo, Set(btc)))))
    assertEquals(kinds, Set[(Exchange, SubscriptionKind)]((ex, SubscriptionKind.BBO("BTCUSDT"))))

  test("自定义的按标的路由 topic 表示关注, 不表示交易 —— 不触发补齐"):
    // 一个只订阅别处指标的监控/元策略, 不该被补上该标的的私有回报, 更不该被引擎
    // 拉去做持仓对齐、要求 SymbolMeta —— 它根本不交易那个标的。
    val sub = subOf(Set(Interest.Keyed(StrategyRunnerSpec.AlphaSignal, Set(btc))))
    assertEquals(sub.instruments, Set.empty[Instrument])
    assert(!sub.accepts(Event.local(Topics.Fill, Fill(AccountId.Live, ex, "BTCUSDT", Side.Long, 100.0, Coin(1.0), 0L))))
    assertEquals(SubscriptionKind.from(sub), Set.empty[(Exchange, SubscriptionKind)])
    // 但它自己声明的那条依然收得到
    assert(sub.accepts(Event.local(StrategyRunnerSpec.AlphaSignal, StrategyRunnerSpec.Score(btc, 1.0))))

  test("对公共行情 topic 用 Interest.All -> 无从派生订阅, 立即报错"):
    val e = intercept[RuntimeException] {
      SubscriptionKind.from(Subscription(Set(Interest.All(Topics.Bbo))))
    }
    assert(e.getMessage.contains("Interest.Keyed"), e.getMessage)

object StrategyRunnerSpec:
  final case class Score(instrument: Instrument, value: Double)
  object AlphaSignal extends hft.event.Topic[Instrument, Score]("alphaSignal"):
    def keyOf(p: Score): Instrument = p.instrument
