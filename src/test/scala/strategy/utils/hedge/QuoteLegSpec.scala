package strategy.utils.hedge

import hft.TestUnits.given
import hft.domain.*

/** QuoteLeg 单测：四态流转、防重、按 orderId 匹配、存活超时与撤单确认超时的区别、体制抢占。 */
class QuoteLegSpec extends munit.FunSuite:
  private val ex = Exchange.Okx
  private val sym = "ETH"

  private def upd(status: OrderStatus, px: Price = 3000.0, ts: Timestamp = 1000, oid: OrderId = "o1") =
    OrderUpdate(AccountId.Live, oid, Some("c1"), ex, sym, Side.Short, status, px, 1.0, 0.0, ts)

  private def bbo(bid: Price, ask: Price) = BBO(ex, sym, bid, 1.0, ask, 1.0, 0)

  /** 喂一条回报。回报自带的时间戳同时当作**本地处理时刻** —— QuoteLeg 如今一律算在本地钟上
    * (见它的"时钟域")，这里让两者取同一个值，用例的语义与从前完全一致。 */
  private def feed(leg: QuoteLeg, u: OrderUpdate): Option[Price] = leg.onOrderUpdate(u, u.timestamp)

  /** 默认方式：被动挂对手价外 1%, 给 5s 成交 (与从前写死的那对参数等价) */
  private val passive = QuoteStyle.passive(0.01, 5000)
  private val crossing = QuoteStyle.crossing(0.001, 1000)

  test("初始空闲 -> Ready"):
    assertEquals(QuoteLeg(cancelConfirmMs = 3000).step(0, passive), QuoteLeg.Step.Ready)

  test("place 后进入等确认 -> Blocked (一次穿越不会挂出两张单)"):
    val leg = QuoteLeg(cancelConfirmMs = 3000)
    leg.place(passive, Side.Short, Some(bbo(2999.0, 3001.0)), Price(3000.0))
    assertEquals(leg.step(0, passive), QuoteLeg.Step.Blocked)
    assertEquals(leg.step(999_999, passive), QuoteLeg.Step.Blocked, "等确认期间, 再久也不该动")

  test("Pending 确认后在簿上: requoteMs 内 Blocked, 超时 Requote"):
    val leg = QuoteLeg(cancelConfirmMs = 3000)
    leg.place(passive, Side.Short, Some(bbo(2999.0, 3001.0)), Price(3000.0))
    assertEquals(feed(leg, upd(OrderStatus.Pending, ts = 1000)), None)
    assertEquals(leg.step(5999, passive), QuoteLeg.Step.Blocked)
    assertEquals(leg.step(6001, passive), QuoteLeg.Step.Requote(OrderRef.ByExchangeId("o1"), QuoteLeg.Requote.Expired(passive)))
    assertEquals(leg.step(6002, passive), QuoteLeg.Step.Blocked, "撤单在途, 还不能挂新单")
    assertEquals(feed(leg, upd(OrderStatus.Cancelled, ts = 6100)), None)
    assertEquals(leg.step(6101, passive), QuoteLeg.Step.Ready, "撤单终态到达后才回到空闲")

  test("requote 用的是回报里的交易所时钟 (调用方须传同一时钟域)"):
    val leg = QuoteLeg(cancelConfirmMs = 3000)
    leg.place(passive, Side.Short, Some(bbo(2999.0, 3001.0)), Price(3000.0))
    feed(leg, upd(OrderStatus.Pending, ts = 100_000)) // 挂上的交易所时刻
    assertEquals(leg.step(101_000, passive), QuoteLeg.Step.Blocked, "交易所时钟只过了 1s")

  test("Filled 返回成交价 (价格轴策略据此重置对冲中心), 并回到空闲"):
    val leg = QuoteLeg(cancelConfirmMs = 3000)
    leg.place(passive, Side.Short, Some(bbo(2999.0, 3001.0)), Price(3000.0))
    feed(leg, upd(OrderStatus.Pending))
    assertEquals(feed(leg, upd(OrderStatus.Filled, px = 3031.0)), Some(Price(3031.0)))
    assertEquals(leg.step(0, passive), QuoteLeg.Step.Ready)

  test("PartiallyFilled 视为仍在簿上 (未成交部分还要 requote)"):
    val leg = QuoteLeg(cancelConfirmMs = 3000)
    leg.place(passive, Side.Short, Some(bbo(2999.0, 3001.0)), Price(3000.0))
    assertEquals(feed(leg, upd(OrderStatus.PartiallyFilled(Coin(0.5)), ts = 1000)), None)
    assertEquals(leg.step(2000, passive), QuoteLeg.Step.Blocked)
    assertEquals(leg.step(7000, passive), QuoteLeg.Step.Requote(OrderRef.ByExchangeId("o1"), QuoteLeg.Requote.Expired(passive)))

  test("Cancelled / Rejected / Error 都回到空闲"):
    Seq(OrderStatus.Cancelled, OrderStatus.Rejected("x"), OrderStatus.Error("y")).foreach { st =>
      val leg = QuoteLeg(cancelConfirmMs = 3000)
      leg.place(passive, Side.Short, Some(bbo(2999.0, 3001.0)), Price(3000.0))
      assertEquals(feed(leg, upd(st)), None)
      assertEquals(leg.step(0, passive), QuoteLeg.Step.Ready, s"$st 之后应可重挂")
    }

  test("Created 是本地态, 不改变状态 (仍在等确认)"):
    val leg = QuoteLeg(cancelConfirmMs = 3000)
    leg.place(passive, Side.Short, Some(bbo(2999.0, 3001.0)), Price(3000.0))
    assertEquals(feed(leg, upd(OrderStatus.Created)), None)
    assertEquals(leg.step(0, passive), QuoteLeg.Step.Blocked)

  test("被动价在盘口外侧: 卖挂 ask 之上、买挂 bid 之下 (PostOnly 不吃单)"):
    val leg = QuoteLeg(cancelConfirmMs = 3000)
    val b = Some(bbo(2999.0, 3001.0))
    assertEquals(leg.place(passive, Side.Short, b, Price(3000.0)), (Price(3001.0 * 1.01), TimeInForce.PostOnly))
    val leg2 = QuoteLeg(cancelConfirmMs = 3000)
    assertEquals(leg2.place(passive, Side.Long, b, Price(3000.0)), (Price(2999.0 * 0.99), TimeInForce.PostOnly))

  test("盘口缺失 -> 回退给定基准价 (降级, 调用方须告警)"):
    val leg = QuoteLeg(cancelConfirmMs = 3000)
    assertEquals(leg.place(passive, Side.Short, None, Price(3000.0)), (Price(3000.0 * 1.01), TimeInForce.PostOnly))

  // ---------- 撤单在途：孤儿挂单的成因与防线 ----------

  test("撤单在途不挂新单: 旧单的 Cancelled 到达之前一直 Blocked (孤儿挂单回归测试)"):
    // 从前的实现在发出撤单的同一刻就把状态清成空闲, 于是:
    //   撤 A -> 挂 B -> A 的 Cancelled 到达并清掉"等确认" -> 挂 C -> B 的回报被 C 覆盖 -> B 成孤儿。
    // 现在 A 的终态到达之前一律 Blocked, 那个交错构造不出来。
    val leg = QuoteLeg(cancelConfirmMs = 3000)
    leg.place(passive, Side.Short, Some(bbo(2999.0, 3001.0)), Price(3000.0))
    feed(leg, upd(OrderStatus.Pending, ts = 0))
    assertEquals(leg.step(6000, passive), QuoteLeg.Step.Requote(OrderRef.ByExchangeId("o1"), QuoteLeg.Requote.Expired(passive)))
    (1 to 5).foreach(i => assertEquals(leg.step(6000 + i * 100, passive), QuoteLeg.Step.Blocked, "撤单在途"))
    feed(leg, upd(OrderStatus.Cancelled, ts = 6600))
    assertEquals(leg.step(6700, passive), QuoteLeg.Step.Ready)

  test("撤单在途期间收到别的 orderId 的回报 -> 忽略 (迟到消息不该清掉当前这张单的状态)"):
    val leg = QuoteLeg(cancelConfirmMs = 3000)
    leg.place(passive, Side.Short, Some(bbo(2999.0, 3001.0)), Price(3000.0))
    feed(leg, upd(OrderStatus.Pending, ts = 0))
    leg.step(6000, passive) // -> Cancelling(o1)
    assertEquals(feed(leg, upd(OrderStatus.Cancelled, ts = 6100, oid = "旧单")), None)
    assertEquals(leg.step(6200, passive), QuoteLeg.Step.Blocked, "o1 的终态还没来")

  test("在簿上期间收到别的 orderId 的回报 -> 忽略"):
    val leg = QuoteLeg(cancelConfirmMs = 3000)
    leg.place(passive, Side.Short, Some(bbo(2999.0, 3001.0)), Price(3000.0))
    feed(leg, upd(OrderStatus.Pending, ts = 1000))
    assertEquals(feed(leg, upd(OrderStatus.Filled, px = 9999.0, ts = 1100, oid = "旧单")), None,
      "别人的成交不该被当成本腿成交 (会把对冲中心挪到一个无关价位)")
    assertEquals(leg.step(2000, passive), QuoteLeg.Step.Blocked, "本腿还在簿上")

  test("撤单确认迟迟不来 -> 重发同一张单的撤单 (不静默等下去)"):
    val leg = QuoteLeg(cancelConfirmMs = 3000)
    leg.place(passive, Side.Short, Some(bbo(2999.0, 3001.0)), Price(3000.0))
    feed(leg, upd(OrderStatus.Pending, ts = 0))
    assertEquals(leg.step(6000, passive), QuoteLeg.Step.Requote(OrderRef.ByExchangeId("o1"), QuoteLeg.Requote.Expired(passive)))
    assertEquals(leg.step(11_001, passive), QuoteLeg.Step.Requote(OrderRef.ByExchangeId("o1"), QuoteLeg.Requote.Unconfirmed(5001)),
      "重发的是同一个 id, 不可能造出新的孤儿单")
    assertEquals(leg.step(11_002, passive), QuoteLeg.Step.Blocked, "重发后重新开始等")

  test("撤单在途时那张单成交了 -> 回到空闲并给出成交价"):
    val leg = QuoteLeg(cancelConfirmMs = 3000)
    leg.place(passive, Side.Short, Some(bbo(2999.0, 3001.0)), Price(3000.0))
    feed(leg, upd(OrderStatus.Pending, ts = 0))
    leg.step(6000, passive)
    assertEquals(feed(leg, upd(OrderStatus.Filled, px = 3031.0, ts = 6100)), Some(Price(3031.0)))
    assertEquals(leg.step(6200, passive), QuoteLeg.Step.Ready)

  test("非空闲态调用 place -> 抛错 (绕过 step 就是在制造孤儿单)"):
    val leg = QuoteLeg(cancelConfirmMs = 3000)
    leg.place(passive, Side.Short, Some(bbo(2999.0, 3001.0)), Price(3000.0))
    intercept[IllegalArgumentException](leg.place(passive, Side.Short, Some(bbo(2999.0, 3001.0)), Price(3000.0)))

  test("空闲态收到迟到的回报 -> 忽略, 不改变状态"):
    val leg = QuoteLeg(cancelConfirmMs = 3000)
    assertEquals(feed(leg, upd(OrderStatus.Filled, px = 9999.0)), None)
    assertEquals(leg.step(0, passive), QuoteLeg.Step.Ready)

  // ---------- 体制抢占：由缓到急要抢在超时之前 ----------

  test("在簿上的被动单遇到跨价需求 -> 抢在存活时间到点之前撤换"):
    val leg = QuoteLeg(cancelConfirmMs = 3000)
    leg.place(passive, Side.Short, Some(bbo(2999.0, 3001.0)), Price(3000.0))
    feed(leg, upd(OrderStatus.Pending, ts = 0))
    assertEquals(leg.step(1000, passive), QuoteLeg.Step.Blocked, "同一方式, 5s 没到不动")
    assertEquals(
      leg.step(1000, crossing),
      QuoteLeg.Step.Requote(OrderRef.ByExchangeId("o1"), QuoteLeg.Requote.Preempted(passive, crossing)),
      "体制切到单边: 等完那一分钟才去追是最贵的等待",
    )

  test("反向不抢占: 在簿上的跨价单遇到被动需求 -> 等它自己到点"):
    // 反向抢占没有收益 (跨价单本来就秒级成交), 还会让 ER 在阈值附近抖动时不停撤挂
    val leg = QuoteLeg(cancelConfirmMs = 3000)
    leg.place(crossing, Side.Short, Some(bbo(2999.0, 3001.0)), Price(3000.0))
    feed(leg, upd(OrderStatus.Pending, ts = 0))
    assertEquals(leg.step(500, passive), QuoteLeg.Step.Blocked)
    assertEquals(leg.step(1001, passive),
      QuoteLeg.Step.Requote(OrderRef.ByExchangeId("o1"), QuoteLeg.Requote.Expired(crossing)),
      "1s 存活时间到了才撤, 原因是超时而非抢占")

  test("存活时间取**挂它时**用的方式, 不是此刻想用的方式"):
    // 挂的是 1s 的跨价单, 之后体制转平缓 -> 仍按 1s 判超时 (而不是被动的 5s)
    val leg = QuoteLeg(cancelConfirmMs = 3000)
    leg.place(crossing, Side.Short, Some(bbo(2999.0, 3001.0)), Price(3000.0))
    feed(leg, upd(OrderStatus.Pending, ts = 0))
    assertEquals(leg.step(1001, passive).isInstanceOf[QuoteLeg.Step.Requote], true)

  // ---------- 两个超时是两件事 ----------

  test("撤单确认超时独立于存活时间: 1s 的追单模式不会把正常撤单往返当成异常"):
    // 存活 1s、撤单确认容忍 3s。撤单发出后 2s 仍在等 —— 正常, 不该重发
    val leg = QuoteLeg(cancelConfirmMs = 3000)
    leg.place(crossing, Side.Short, Some(bbo(2999.0, 3001.0)), Price(3000.0))
    feed(leg, upd(OrderStatus.Pending, ts = 0))
    leg.step(1001, crossing) // -> Cancelling(sentAt=1001)
    assertEquals(leg.step(3500, crossing), QuoteLeg.Step.Blocked, "撤单发出 2.5s, 未超 3s 容忍")
    assertEquals(leg.step(4100, crossing),
      QuoteLeg.Step.Requote(OrderRef.ByExchangeId("o1"), QuoteLeg.Requote.Unconfirmed(3099)),
      "超 3s 才重发")

  test("跨价下单返回 GTC 与穿过盘口的价"):
    val leg = QuoteLeg(cancelConfirmMs = 3000)
    assertEquals(
      leg.place(crossing, Side.Short, Some(bbo(2999.0, 3001.0)), Price(3000.0)),
      (Price(2999.0 * 0.999), TimeInForce.GTC),
    )
