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
    assertEquals(BoardView.of(snapshot, t0 + 1000).symbols.head.bbo.get.ageMs, 1000L)
    assertEquals(BoardView.of(snapshot, t0 + 9000).symbols.head.bbo.get.ageMs, 9000L)

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
    assert(v.symbols.isEmpty && v.accounts.isEmpty)

  test("行按 (交易所, 标的) 排序 —— 页面顺序不随 Map 的哈希序抖"):
    val s = Seq(("ETHUSDT", Exchange.Okx), ("AAAUSDT", Exchange.Binance), ("BBBUSDT", Exchange.Binance))
      .foldLeft(BoardSnapshot.empty) { case (acc, (sy, exch)) =>
        acc.apply(Event.stamped(Topics.Bbo, BBO(exch, sy, 1.0, Coin(1.0), 2.0, Coin(1.0), t0), t0, t0))
      }
    assertEquals(
      BoardView.of(s, t0).symbols.map(r => (r.exchange, r.symbol)),
      Vector(("Binance", "AAAUSDT"), ("Binance", "BBBUSDT"), ("Okx", "ETHUSDT")),
    )

  test("单位在边界解包成裸数字 —— opaque type 是进程内的防线, 不出到线上"):
    val row = BoardView.of(snapshot, t0).symbols.head
    assertEquals(row.bbo.get.value.mid, 50000.0)
    assertEquals(row.accounts.head.position.get.value, 0.5)

  test("空集合序列化成 [] 而不是整个字段消失"):
    // jsoniter 默认丢空集合。页面拿到的会是 undefined —— "字段不存在"与"这个读数没有"
    // 在 JS 那边又合并成同一件事, 而分开它们正是这份视图存在的理由;
    // 而且 `undefined.length` 直接抛 TypeError, 空看板会白屏。
    val json = writeToString(BoardView.of(BoardSnapshot.empty, t0))
    assert(json.contains("\"symbols\":[]"), json)
    assert(json.contains("\"accounts\":[]"), json)
    assert(json.contains("\"lastEventAgeMs\":null"), json)
