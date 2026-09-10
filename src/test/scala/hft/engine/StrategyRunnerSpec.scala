package hft.engine

import hft.domain.*
import hft.event.{AnyEvent, Event, Interest, Subscription, Topics}
import hft.state.StateManager
import hft.event.Commands.OutcomeEvent
import hft.strategy.Strategy
import hft.TestUnits.given

/** 策略订阅范围的派生: 框架补齐了什么、又据此向交易所订了什么。 */
class StrategyRunnerSpec extends munit.FunSuite:
  private val ex = Exchange.Binance
  private val btc = Instrument.perp(ex, "BTCUSDT")

  /** 直接喂声明，绕过策略实例 —— 这里测的是"框架据声明补齐了什么" */
  private def subOf(interests: Set[Interest]): Subscription =
    StrategyRunner.subscriptionFor(interests, AccountId.Live)

  test("补齐持仓与订单回报 —— 策略不该有机会漏订这两样"):
    val sub = subOf(Set(Interest.Keyed(Topics.Bbo, Set(btc))))
    val position = Event.local(Topics.Position, Position(AccountId.Live, ex, "BTCUSDT", 1.0))
    val orderUpdate = Event.local(
      Topics.OrderUpdate,
      OrderUpdate(AccountId.Live, "1", Some("c1"), ex, "BTCUSDT", Side.Long, OrderStatus.Filled, 100.0, Coin(1.0), Coin(1.0), reduceOnly = false, 0L),
    )
    assert(sub.accepts(position), "持仓必须补齐: 漏订就是拿着错的敞口决策")
    assert(sub.accepts(orderUpdate), "订单回报必须补齐: 超时检测与停机撤单都靠它")

  test("成交明细**不**补齐 —— 仓位归柜台算之后, 策略不再非它不可"):
    // 少补一条不是省事: 成交是热路径, 多策略部署下每笔成交都要白投几份。
    // 想看成交明细的策略自己声明 own(Topics.Fill), 那本来就是它该说的话。
    val sub = subOf(Set(Interest.Keyed(Topics.Bbo, Set(btc))))
    val fill = Event.local(Topics.Fill, Fill(AccountId.Live, ex, "BTCUSDT", Side.Long, 100.0, Coin(1.0), 0L))
    assert(!sub.accepts(fill))

    val declared = subOf(Set(Interest.Keyed(Topics.Bbo, Set(btc)), Interest.Keyed(Topics.Fill, Set(AccountInstrument(AccountId.Live, btc)))))
    assert(declared.accepts(fill), "显式声明了就该收到")

  test("补齐所涉交易所的账户级读数, 但不越界到别的交易所"):
    val sub = subOf(Set(Interest.Keyed(Topics.Bbo, Set(btc))))
    assert(sub.accepts(Event.local(Topics.AccountInfo, AccountInfo(AccountId.Live, ex, 1.0))))
    assert(sub.accepts(Event.local(Topics.Balance, Balance(AccountId.Live, ex, "USDT", 1.0, 0L))))
    assert(sub.accepts(Event.local(Topics.Greeks, Greeks(AccountId.Live, ex, "BTC", 0.0, 0.0, 0.0, 0.0, 0L))))
    assert(
      !sub.accepts(Event.local(Topics.AccountInfo, AccountInfo(AccountId.Live, Exchange.Okx, 1.0))),
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
    assert(!sub.accepts(Event.local(Topics.AccountInfo, AccountInfo(AccountId.Live, ex, 1.0))))

  test("行情订阅由同一份声明派生 —— 一处声明, 两处派生"):
    val sub = subOf(Set(
      Interest.Keyed(Topics.Bbo, Set(btc)),
      Interest.Keyed(Topics.Trade, Set(Instrument.perp(Exchange.Okx, "ETHUSDT"))),
    ))
    assertEquals(
      sub.marketStreams,
      Set[(Exchange, SubscriptionKind)](
        (ex, SubscriptionKind.BBO(Instrument.perp(ex, "BTCUSDT"))),
        (Exchange.Okx, SubscriptionKind.Trade(Instrument.perp(Exchange.Okx, "ETHUSDT"))),
      ),
    )

  test("补齐的私有回报不会被误当成要订阅的行情流"):
    // Position/OrderUpdate/Fill 由账户流推送，不该出现在向交易所下的行情订阅里
    val kinds = subOf(Set(Interest.Keyed(Topics.Bbo, Set(btc)))).marketStreams
    assertEquals(kinds, Set[(Exchange, SubscriptionKind)]((ex, SubscriptionKind.BBO(Instrument.perp(ex, "BTCUSDT")))))

  test("自定义的按标的路由 topic 表示关注, 不表示交易 —— 不触发补齐"):
    // 一个只订阅别处指标的监控/元策略, 不该被补上该标的的私有回报, 更不该被引擎
    // 拉去做持仓对齐、要求 SymbolMeta —— 它根本不交易那个标的。
    val sub = subOf(Set(Interest.Keyed(StrategyRunnerSpec.AlphaSignal, Set(btc))))
    assertEquals(sub.instruments, Set.empty[Instrument])
    assert(!sub.accepts(Event.local(Topics.Fill, Fill(AccountId.Live, ex, "BTCUSDT", Side.Long, 100.0, Coin(1.0), 0L))))
    assertEquals(sub.marketStreams, Set.empty[(Exchange, SubscriptionKind)])
    // 但它自己声明的那条依然收得到
    assert(sub.accepts(Event.local(StrategyRunnerSpec.AlphaSignal, StrategyRunnerSpec.Score(btc, 1.0))))

  test("对公共行情 topic 用 Interest.All -> 无从派生订阅, 立即报错"):
    val e = intercept[RuntimeException] {
      Subscription(Set(Interest.All(Topics.Bbo))).marketStreams
    }
    assert(e.getMessage.contains("Interest.Keyed"), e.getMessage)

  test("用户自定义的行情 topic 同样能派生出订阅流 (旧的内置映射表覆盖不到这种)"):
    // MarketTopic 把"我对应哪条流"做成抽象成员, 于是自定义行情源声明时就必须回答;
    // 从前那张 topic->流 的表只列了框架内置的五个, 自定义的会被认作交易标的却永远订不到数据。
    val sub = subOf(Set(Interest.Keyed(StrategyRunnerSpec.CustomFeed, Set(btc))))
    assertEquals(sub.instruments, Set(btc), "继承 MarketTopic 即被认作交易标的")
    assertEquals(sub.marketStreams, Set[(Exchange, SubscriptionKind)]((ex, SubscriptionKind.Trade(Instrument.perp(ex, "BTCUSDT")))))

  test("行情订阅流带着标的的品种 —— 期权盘口与永续盘口是两条不同的流"):
    // 从前 SubscriptionKind 只带 symbol, 适配层拼订阅参数时只能假定一种品种 (OKX 侧恒拼
    // -SWAP)。少了品种就订不到期权盘口, 而"订了个空"没有任何症状。
    val option = Instrument.option(Exchange.Okx, "ETH-USD-250101-3000-C")
    val streams = subOf(Set(Interest.Keyed(Topics.Bbo, Set(option)))).marketStreams
    assertEquals(streams, Set[(Exchange, SubscriptionKind)]((Exchange.Okx, SubscriptionKind.BBO(option))))
    assertEquals(streams.head._2.subscribedInstrument.kind, InstrumentKind.Option)

object StrategyRunnerSpec:
  final case class Score(instrument: Instrument, value: Double)

  /** 一个"用户自定义"的行情源 —— 继承 MarketTopic 就必须回答 streamKind, 否则编译不过 */
  object CustomFeed extends hft.event.MarketTopic[Score]("customFeed"):
    def keyOf(p: Score): Instrument = p.instrument
    def streamKind(instrument: Instrument): SubscriptionKind = SubscriptionKind.Trade(instrument)

  object AlphaSignal extends hft.event.Topic[Instrument, Score]("alphaSignal"):
    def keyOf(p: Score): Instrument = p.instrument

