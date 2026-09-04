package hft.dashboard

import com.github.plokhotnyuk.jsoniter_scala.core.writeToString
import hft.domain.*
import hft.event.{Event, Topics}
import hft.TestUnits.given

/** 视图投影的契约: 年龄在**读取时刻**才算得出来; 缺读数是 JSON 里的 null 而不是 0。 */
class BoardViewSpec extends munit.FunSuite:
  private val ex = Exchange.Binance
  private val sym = "BTCUSDT"
  private val t0 = 1_700_000_000_000L

  private val snapshot = BoardSnapshot.empty
    .apply(Event.stamped(Topics.Bbo, BBO(ex, sym, 49999.0, Coin(1.0), 50001.0, Coin(2.0), t0), t0, t0))
    .apply(Event.stamped(Topics.Position, Position(AccountId.Live, ex, sym, Coin(0.5)), t0, t0))

  test("年龄相对读取时刻算, 同一份快照读两次给出不同的年龄"):
    // 年龄不能在折叠时算死: 那样页面上的"3s 前"会永远停在 3s。
    assertEquals(BoardView.of(snapshot, t0 + 1000).assets.head.venues.head.bbo.get.ageMs, 1000L)
    assertEquals(BoardView.of(snapshot, t0 + 9000).assets.head.venues.head.bbo.get.ageMs, 9000L)

  test("缺读数序列化成 null, 不是 0"):
    // 0 是一个合法的价格/仓位取值。把"还没有这条读数"写成 0, 页面就再也分不出
    // "空仓"与"仓位读数没到过" —— 这正是这一整轮审查在消灭的东西。
    val json = writeToString(BoardView.of(snapshot, t0))
    assert(json.contains("\"markPrice\":null"), json)
    assert(json.contains("\"funding\":null"), json)
    assert(json.contains("\"lastFill\":null"), json)

  test("空快照的视图: eventsApplied = 0 且没有心跳"):
    val v = BoardView.of(BoardSnapshot.empty, t0)
    assertEquals(v.eventsApplied, 0L)
    assertEquals(v.lastEventAgeMs, None)
    assert(v.assets.isEmpty && v.accounts.isEmpty)

  test("行按 (交易所, 标的) 排序 —— 页面顺序不随 Map 的哈希序抖"):
    val s = Seq(("ETHUSDT", Exchange.Okx), ("AAAUSDT", Exchange.Binance), ("BBBUSDT", Exchange.Binance))
      .foldLeft(BoardSnapshot.empty) { case (acc, (sy, exch)) =>
        acc.apply(Event.stamped(Topics.Bbo, BBO(exch, sy, 1.0, Coin(1.0), 2.0, Coin(1.0), t0), t0, t0))
      }
    assertEquals(
      BoardView.of(s, t0).assets.flatMap(a => a.venues.map(v => (v.exchange, v.symbol))),
      Vector(("Binance", "AAAUSDT"), ("Binance", "BBBUSDT"), ("Okx", "ETHUSDT")),
    )

  test("单位在边界解包成裸数字 —— opaque type 是进程内的防线, 不出到线上"):
    val row = BoardView.of(snapshot, t0).assets.head.venues.head
    assertEquals(row.bbo.get.value.mid, 50000.0)
    assertEquals(row.accounts.head.position.get.value, 0.5)

  test("空集合序列化成 [] 而不是整个字段消失"):
    // jsoniter 默认丢空集合。页面拿到的会是 undefined —— "字段不存在"与"这个读数没有"
    // 在 JS 那边又合并成同一件事, 而分开它们正是这份视图存在的理由;
    // 而且 `undefined.length` 直接抛 TypeError, 空看板会白屏。
    val json = writeToString(BoardView.of(BoardSnapshot.empty, t0))
    assert(json.contains("\"assets\":[]"), json)
    assert(json.contains("\"accounts\":[]"), json)
    assert(json.contains("\"lastEventAgeMs\":null"), json)

  test("同一资产在各家交易所并成一行 —— 分组由调用方给, 看板不猜"):
    // 各所的 symbol 串本就不同 (AAPLUSDT / AAPL / AAPL)。靠去后缀之类的规则去猜, 迟早在
    // BTCUSDC、1000PEPEUSDT、ETH-USD-240329-3000-C 上撞翻; 而且那是第二份已经存在的知识 ——
    // 建立标的宇宙的那个组件 (crossspread 的 listings) 才知道正确答案。
    val quotes = Vector(
      (Exchange.Binance, "AAPLUSDT", 328.34, 328.35),
      (Exchange.Okx, "AAPL", 328.39, 328.40),
      (Exchange.Hyperliquid, "AAPL", 328.02, 328.03),
      (Exchange.Binance, "NVDAUSDT", 230.72, 230.73),
    )
    val snap = quotes.foldLeft(BoardSnapshot.empty) { case (acc, (exch, sy, bid, ask)) =>
      acc.apply(Event.stamped(Topics.Bbo, BBO(exch, sy, bid, Coin(1.0), ask, Coin(1.0), t0), t0, t0))
    }
    // 装配方给的权威映射 (crossspread 就是这么反建的)
    val assetOf: Instrument => String = i => if i.symbol.startsWith("AAPL") then "AAPL" else "NVDA"
    val view = BoardView.of(snap, t0, assetOf)
    assertEquals(view.assets.map(_.asset), Vector("AAPL", "NVDA"))
    assertEquals(
      view.assets.head.venues.map(v => (v.exchange, v.symbol)),
      Vector(("Binance", "AAPLUSDT"), ("Hyperliquid", "AAPL"), ("Okx", "AAPL")),
      "组内按 (交易所, 标的) 排序, 顺序不随 Map 哈希序抖",
    )
    assertEquals(view.assets(1).venues.map(_.symbol), Vector("NVDAUSDT"))

  test("默认按 symbol 本身分组 —— 不发明任何知识"):
    // 默认行为下, 只有本就同名的 symbol 才会合并; AAPLUSDT 与 AAPL 各自成行。
    val snap = Vector((Exchange.Binance, "AAPLUSDT"), (Exchange.Okx, "AAPL"), (Exchange.Hyperliquid, "AAPL"))
      .foldLeft(BoardSnapshot.empty) { case (acc, (exch, sy)) =>
        acc.apply(Event.stamped(Topics.Bbo, BBO(exch, sy, 1.0, Coin(1.0), 2.0, Coin(1.0), t0), t0, t0))
      }
    val view = BoardView.of(snap, t0)
    assertEquals(view.assets.map(_.asset), Vector("AAPL", "AAPLUSDT"))
    assertEquals(view.assets.head.venues.map(_.exchange), Vector("Hyperliquid", "Okx"), "本就同名的自然合并")
    assertEquals(view.assets(1).venues.map(_.exchange), Vector("Binance"))

  test("跨所极差的判据是同时性, 不是新鲜度 —— 三家一起安静时价差照给"):
    // 股票永续在美股闭市时段几乎不成交, 报价几十秒不动是常态, 而那个价仍是当前真实盘口。
    // 从前按绝对年龄卡 (5s), 于是非交易时段一律显示"—", 把一个真实存在的价差藏了起来。
    val old = t0 - 40_000 // 三家都是 40 秒前, 但彼此只差 1 秒
    val quiet = Vector(
      (Exchange.Binance, "AAPLUSDT", old),
      (Exchange.Okx, "AAPL", old + 1000),
      (Exchange.Hyperliquid, "AAPL", old + 500),
    ).foldLeft(BoardSnapshot.empty) { case (acc, (exch, sy, ts)) =>
      acc.apply(Event.stamped(Topics.Bbo, BBO(exch, sy, 100.0, Coin(1.0), 100.1, Coin(1.0), ts), ts, ts))
    }
    val row = BoardView.of(quiet, t0, _ => "AAPL").assets.head
    assertEquals(row.venues.size, 3)
    assertEquals(row.quoteSkewMs, Some(1000L), "彼此只差 1 秒 -> 可比")

  test("一边现价配另一边旧价 -> skew 大, 由它说明不可比"):
    // 这正是 crossspread 的 maxQuoteAgeMs 防的事: 差出来的里面掺着这段时间里对方走过的路。
    val snap = Vector(
      (Exchange.Binance, "AAPLUSDT", t0),          // 现价
      (Exchange.Okx, "AAPL", t0 - 30_000),         // 30 秒前
    ).foldLeft(BoardSnapshot.empty) { case (acc, (exch, sy, ts)) =>
      acc.apply(Event.stamped(Topics.Bbo, BBO(exch, sy, 100.0, Coin(1.0), 100.1, Coin(1.0), ts), ts, ts))
    }
    assertEquals(BoardView.of(snap, t0, _ => "AAPL").assets.head.quoteSkewMs, Some(30_000L))

  test("只有一家有报价 -> 没有 skew 可言"):
    val one = BoardSnapshot.empty
      .apply(Event.stamped(Topics.Bbo, BBO(ex, sym, 1.0, Coin(1.0), 2.0, Coin(1.0), t0), t0, t0))
    assertEquals(BoardView.of(one, t0).assets.head.quoteSkewMs, None)

  test("各家交易所的心跳: 某个标的不报价 ≠ 那家的流卡住了"):
    // 单个标的的年龄回答不了"这家还活着吗" —— 它可能只是没人交易。跨所别的标的还在报,
    // 就说明流是活的。所以心跳按交易所聚合。
    val snap = BoardSnapshot.empty
      .apply(Event.stamped(Topics.Bbo, BBO(Exchange.Okx, "AAPL", 1.0, Coin(1.0), 2.0, Coin(1.0), t0 - 60_000), t0 - 60_000, t0 - 60_000))
      .apply(Event.stamped(Topics.Bbo, BBO(Exchange.Okx, "NVDA", 1.0, Coin(1.0), 2.0, Coin(1.0), t0 - 100), t0 - 100, t0 - 100))
      .apply(Event.stamped(Topics.Bbo, BBO(Exchange.Binance, "AAPLUSDT", 1.0, Coin(1.0), 2.0, Coin(1.0), t0 - 50_000), t0 - 50_000, t0 - 50_000))
    val hb = BoardView.of(snap, t0).venues.map(v => v.exchange -> v.ageMs).toMap
    assertEquals(hb("Okx"), 100L, "AAPL 一分钟没报价, 但 NVDA 刚报过 -> OKX 的流是活的")
    assertEquals(hb("Binance"), 50_000L, "Binance 全部标的都停了 -> 它的心跳如实反映")
