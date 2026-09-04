package hft.dashboard

import hft.domain.*
import hft.event.{Event, Topics}
import hft.TestUnits.given

/** 看板折叠的契约: 每个读数带着它的到达时刻; "没有读数"与"读数是 0"分得开;
  * 挂单在不在场只认框架那一条判据; 全量钱包整表替换、逐币种增量维持。 */
class BoardSnapshotSpec extends munit.FunSuite:
  private val ex = Exchange.Binance
  private val sym = "BTCUSDT"
  private val inst = Instrument(ex, sym)
  private val t0 = 1_700_000_000_000L

  private def bboEv(bid: Double, ask: Double, ts: Timestamp) =
    Event.stamped(Topics.Bbo, BBO(ex, sym, bid, Coin(1.0), ask, Coin(2.0), ts), ts, ts)

  private def orderEv(id: OrderId, status: OrderStatus, ts: Timestamp, account: AccountId = AccountId.Live) =
    Event.stamped(
      Topics.OrderUpdate,
      OrderUpdate(account, id, Some("c-" + id), ex, sym, Side.Long, status, 50000.0, 0.01, 0.0, reduceOnly = false, ts),
      ts,
      ts,
    )

  private def fold(events: Seq[hft.event.AnyEvent]): BoardSnapshot =
    events.foldLeft(BoardSnapshot.empty)((s, e) => s.apply(e))

  test("空快照: eventsApplied = 0 —— 与'来过事件但内容为空'必须分得开"):
    // 页面拿它判断"看板没接上总线" (订阅漏了/引擎没起/行情源没连上), 而不是"无事发生"。
    val s = BoardSnapshot.empty
    assertEquals(s.eventsApplied, 0L)
    assertEquals(s.lastEventAt, None)
    assertEquals(s.symbols, Map.empty[Instrument, SymbolBoard])

  test("行情读数带到达时刻, 年龄相对读取时刻算"):
    val s = fold(Seq(bboEv(49999.0, 50001.0, t0)))
    val b = s.symbols(inst).bbo.getOrElse(fail("应记下盘口"))
    assertEquals(b.at, t0)
    assertEquals(b.ageMs(t0 + 3000), 3000L, "年龄 = 读取时刻 − 到达时刻")
    assertEquals(s.eventsApplied, 1L)
    assertEquals(s.lastEventAt, Some(t0))

  test("后到的盘口覆盖先到的 (年龄跟着一起更新)"):
    val s = fold(Seq(bboEv(49999.0, 50001.0, t0), bboEv(50100.0, 50102.0, t0 + 5000)))
    val b = s.symbols(inst).bbo.get
    assertEquals(b.value.bidPrice.value, 50100.0)
    assertEquals(b.at, t0 + 5000)

  test("没有仓位读数 = None, 不是 0 —— 那两者在交易上完全不是一回事"):
    // 页面据此显示"—"而不是"0"。显示 0 等于宣称"这个账户在这个标的上是空仓",
    // 而事实是这条读数还没到过。
    val s = fold(Seq(bboEv(49999.0, 50001.0, t0)))
    assertEquals(s.symbols(inst).accounts.get(AccountId.Live), None)

  test("仓位为 0 与没有仓位读数分得开"):
    val flat = Event.stamped(Topics.Position, Position(AccountId.Live, ex, sym, Coin.Zero), t0, t0)
    val s = fold(Seq(flat))
    assertEquals(s.symbols(inst).accounts(AccountId.Live).position.map(_.value.value), Some(0.0))

  test("挂单: 非终态入场, 终态离场 —— 判据只认 OrderStatus.isTerminal"):
    // 这里若自己列一遍终态, 迟早与框架那份错开一个状态, 而失效形态是看板上挂着一张早就没了的单。
    val s1 = fold(Seq(orderEv("o1", OrderStatus.Pending, t0), orderEv("o2", OrderStatus.Created, t0)))
    assertEquals(s1.symbols(inst).accounts(AccountId.Live).pendingOrders.keySet, Set("o1", "o2"))
    val s2 = s1.apply(orderEv("o1", OrderStatus.Filled, t0 + 100))
    assertEquals(s2.symbols(inst).accounts(AccountId.Live).pendingOrders.keySet, Set("o2"))
    val s3 = s2.apply(orderEv("o2", OrderStatus.Rejected("bad tick"), t0 + 200))
    assert(s3.symbols(inst).accounts(AccountId.Live).pendingOrders.isEmpty)

  test("同一 orderId 的后续回报替换前一条, 不重复堆积"):
    val s = fold(Seq(orderEv("o1", OrderStatus.Created, t0), orderEv("o1", OrderStatus.Pending, t0 + 50)))
    val pending = s.symbols(inst).accounts(AccountId.Live).pendingOrders
    assertEquals(pending.size, 1)
    assertEquals(pending("o1").value.status, OrderStatus.Pending)

  test("实盘与影子账户各自一行, 互不混淆"):
    val s = fold(Seq(
      orderEv("live-1", OrderStatus.Pending, t0, AccountId.Live),
      orderEv("paper-1", OrderStatus.Pending, t0, AccountId.Paper(1)),
    ))
    val accs = s.symbols(inst).accounts
    assertEquals(accs(AccountId.Live).pendingOrders.keySet, Set("live-1"))
    assertEquals(accs(AccountId.Paper(1)).pendingOrders.keySet, Set("paper-1"))

  test("钱包全量整表替换, 且置上 walletKnown"):
    // walletKnown 之前, "某币不在表里"不代表余额是 0 —— 那份全量只来自启动对齐的一次 REST 查询。
    val key = AccountExchange(AccountId.Live, ex)
    val s0 = fold(Seq(Event.local(Topics.Balance, Balance(AccountId.Live, ex, "ETH", 2.0, t0))))
    assertEquals(s0.accounts(key).wallet.known, false, "逐币种推送给不出'这就是整份钱包'")
    val s1 = s0.apply(Event.local(Topics.Wallet, Wallet(AccountId.Live, ex, Map("USDT" -> 500.0), t0)))
    assertEquals(s1.accounts(key).wallet.known, true)
    assertEquals(s1.accounts(key).wallet.balances.keySet, Set("USDT"), "快照里没有 ETH = ETH 已清空")

  test("钱包快照不抹掉净值 (两者是不同的读数)"):
    val key = AccountExchange(AccountId.Live, ex)
    val s = fold(Seq(
      Event.local(Topics.AccountInfo, AccountInfo(AccountId.Live, ex, 10_000.0)),
      Event.local(Topics.Wallet, Wallet(AccountId.Live, ex, Map("USDT" -> 500.0), t0)),
    ))
    assertEquals(s.accounts(key).equity.map(_.value), Some(10_000.0))

  test("逐币种余额在全量之后只改这一个币种"):
    val key = AccountExchange(AccountId.Live, ex)
    val s = fold(Seq(
      Event.local(Topics.Wallet, Wallet(AccountId.Live, ex, Map("USDT" -> 500.0, "ETH" -> 2.0), t0)),
      Event.local(Topics.Balance, Balance(AccountId.Live, ex, "USDT", 600.0, t0)),
    ))
    assertEquals(s.accounts(key).wallet.balances("USDT"), 600.0)
    assertEquals(s.accounts(key).wallet.balances("ETH"), 2.0, "只有 USDT 变动, ETH 必须还在")

  test("未声明的 topic 即抛 —— 订阅与折叠是同一张表, 走到这里说明有人绕过了它"):
    // 从前这里是"原样返回": 往订阅里加一个 topic 却忘了加折叠分支时, 事件被计数、心跳在跳、
    // 页面上那一列永远是空的, 没有任何症状。现在两者由同一张表派生 (BoardSnapshot.folds),
    // 漂移在类型上就不可能; 这条断言守的是"有人绕过订阅直接投递"的情形。
    val e = intercept[IllegalStateException](BoardSnapshot.empty.apply(Topics.clockAt(t0)))
    assert(e.getMessage.contains("未声明的 topic"), e.getMessage)

  test("订阅列表由折叠表派生 —— 两者不可能错开"):
    assert(BoardSnapshot.topics.contains(Topics.Bbo))
    assert(BoardSnapshot.topics.contains(Topics.Wallet))
    assert(!BoardSnapshot.topics.contains(Topics.Clock), "没有折叠规则的 topic 不该出现在订阅里")

  test("钱包规则复用 WalletState —— 与 StateManager 是同一份实现"):
    // 这三条 (全量整表替换 / 逐币种只覆盖一个键 / 全量之前缺失不等于 0) 从前在
    // StateManager 与看板各写一遍, 两处都只在注释里指向同一份文档 —— 而文档保证不了同步。
    val key = AccountExchange(AccountId.Live, ex)
    val s = fold(Seq(
      Event.local(Topics.Wallet, Wallet(AccountId.Live, ex, Map("USDT" -> 500.0, "ETH" -> 2.0), t0)),
      Event.local(Topics.Balance, Balance(AccountId.Live, ex, "USDT", 600.0, t0)),
    ))
    val w = s.accounts(key).wallet
    assertEquals(w.get("USDT"), Some(600.0))
    assertEquals(w.get("ETH"), Some(2.0))
    assertEquals(w.get("SOL"), Some(0.0), "全量之后, 不在表里 = 0")
    assertEquals(WalletState.empty.get("SOL"), None, "全量之前, 不在表里 = 不知道")
