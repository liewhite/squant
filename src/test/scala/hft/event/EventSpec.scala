package hft.event

import hft.domain.*

/** 事件体系的基本契约: key 由载荷派生、按 topic 还原类型、订阅判据。 */
class EventSpec extends munit.FunSuite:
  private val ex = Exchange.Binance
  private val sym = "BTCUSDT"
  private val inst = Instrument(ex, sym)
  private val t0 = 1_700_000_000_000L

  private def bbo(symbol: String = sym, exchange: Exchange = ex) =
    BBO(exchange, symbol, 100.0, 1.0, 100.1, 1.0, t0)

  test("key 由载荷派生，不可能与载荷不一致"):
    val ev = Event.at(Topics.Bbo, bbo(), t0)
    assertEquals(ev.key, inst)
    // 构造入口只接受载荷，没有任何途径传入一个与载荷矛盾的 key
    assertEquals(ev.key, Topics.Bbo.keyOf(ev.payload))

  test("as: 命中 topic 才还原载荷，且带静态类型"):
    val ev: AnyEvent = Event.at(Topics.Bbo, bbo(), t0)
    val got: Option[BBO] = ev.as(Topics.Bbo)
    assertEquals(got.map(_.bidPrice), Some(100.0))
    assertEquals(ev.as(Topics.Trade), None)
    assert(ev.is(Topics.Bbo))
    assert(!ev.is(Topics.Trade))

  test("topic 身份即引用: 子类改不掉, 索引与 as 用同一判据"):
    // 若 equals 能被结构相等覆盖, 两个类型参数不同却结构相等的 topic 会撞进同一个索引槽,
    // 事件投给错误订阅者、而 as 因引用不等静默返回 None —— 无症状的错投。
    object A extends Topic[Instrument, BBO]("dup"):
      def keyOf(p: BBO): Instrument = Instrument(p.exchange, p.symbol)
    object B extends Topic[Instrument, BBO]("dup"): // 同名, 不同 topic
      def keyOf(p: BBO): Instrument = Instrument(p.exchange, p.symbol)
    assertNotEquals[Topic[?, ?], Topic[?, ?]](A, B, "同名不等价 —— 身份是引用不是名字")
    assertEquals[Topic[?, ?], Topic[?, ?]](A, A)
    assertEquals(Event.at(A, bbo(), t0).as(B), None, "as 与索引判据一致")

  test("载荷类型相同的两个 topic 互不串味"):
    // Position 与 Fill 都按 Instrument 路由；判别靠 topic 身份而非载荷结构
    val pos = Position(AccountId.Live, ex, sym, 1.0, 100.0, 0.0)
    val ev: AnyEvent = Event.local(Topics.Position, pos)
    assert(ev.as(Topics.Position).isDefined)
    assertEquals(ev.as(Topics.Fill), None)

  test("时间戳: at 取交易所时间, local 两者同为本地时刻, stamped 完全指定"):
    val at = Event.at(Topics.Bbo, bbo(), t0)
    assertEquals(at.exchangeTs, t0)
    assert(at.localTs >= t0 || at.localTs > 0)
    val local = Event.local(Topics.Bbo, bbo())
    assertEquals(local.exchangeTs, local.localTs)
    val stamped = Event.stamped(Topics.Bbo, bbo(), 1L, 2L)
    assertEquals((stamped.exchangeTs, stamped.localTs), (1L, 2L))

  test("Interest.Keyed: 只接受声明过的 key"):
    val i = Interest.Keyed(Topics.Bbo, Set(inst))
    assert(i.accepts(Event.at(Topics.Bbo, bbo(), t0)))
    assert(!i.accepts(Event.at(Topics.Bbo, bbo(symbol = "ETHUSDT"), t0)), "未声明的标的不该收")
    assert(!i.accepts(Event.at(Topics.Bbo, bbo(exchange = Exchange.Okx), t0)), "未声明的交易所不该收")
    assert(!i.accepts(Event.at(Topics.Trade, MarketTrade(ex, sym, 1.0, 1.0, false, t0), t0)), "同 key 不同 topic 不该收")

  test("Interest.All: 收该 topic 全部, 但不越到别的 topic"):
    val i = Interest.All(Topics.Bbo)
    assert(i.accepts(Event.at(Topics.Bbo, bbo(symbol = "ANY"), t0)))
    assert(!i.accepts(Event.at(Topics.Trade, MarketTrade(ex, sym, 1.0, 1.0, false, t0), t0)))

  test("空 keys 等价于不订阅, 不会退化成全收"):
    val i = Interest.Keyed(Topics.Bbo, Set.empty[Instrument])
    assert(!i.accepts(Event.at(Topics.Bbo, bbo(), t0)))

  test("keysOf: 按 topic 取回静态类型的 key 集合, 不匹配则空"):
    val i = Interest.Keyed(Topics.Bbo, Set(inst))
    val keys: Set[Instrument] = i.keysOf(Topics.Bbo)
    assertEquals(keys, Set(inst))
    assertEquals(i.keysOf(Topics.Trade), Set.empty)
    assertEquals(Interest.All(Topics.Bbo).keysOf(Topics.Bbo), Set.empty, "全量订阅没有 key 集合可枚举")

  test("Subscription: 跨 topic 汇总标的与交易所"):
    val sub = Subscription(Set(
      Interest.Keyed(Topics.Bbo, Set(inst)),
      Interest.Keyed(Topics.Trade, Set(Instrument(Exchange.Okx, "ETHUSDT"))),
      Interest.Keyed(Topics.AccountInfo, Set(AccountExchange(AccountId.Live, Exchange.Bybit))),
    ))
    assertEquals(sub.instruments, Set(inst, Instrument(Exchange.Okx, "ETHUSDT")))
    assertEquals(sub.exchanges, Set(Exchange.Binance, Exchange.Okx, Exchange.Bybit))

  test("账户级读数按交易所过滤 —— 越界防线"):
    // 此前账户级事件没有路由键因而广播，策略能读到自己没订阅的交易所的净值，
    // 而杠杆闸门正是拿净值算的。
    val sub = Subscription(Set(Interest.Keyed(Topics.AccountInfo, Set(AccountExchange(AccountId.Live, Exchange.Binance)))))
    assert(sub.accepts(Event.local(Topics.AccountInfo, AccountInfo(AccountId.Live, Exchange.Binance, 1.0, 0.0))))
    assert(!sub.accepts(Event.local(Topics.AccountInfo, AccountInfo(AccountId.Live, Exchange.Okx, 1.0, 0.0))))
