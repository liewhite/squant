package strategy.utils.hedge

import hft.TestUnits.given
import hft.domain.*

/** MakerQuoteLeg 单测：三态流转、防重、requote 时钟、被动价在盘口外侧。 */
class MakerQuoteLegSpec extends munit.FunSuite:
  private val ex = Exchange.Okx
  private val sym = "ETH"

  private def upd(status: OrderStatus, px: Price = 3000.0, ts: Timestamp = 1000, oid: OrderId = "o1") =
    OrderUpdate(AccountId.Live, oid, Some("c1"), ex, sym, Side.Short, status, px, 1.0, 0.0, 0.0, ts)

  private def bbo(bid: Price, ask: Price) = BBO(ex, sym, bid, 1.0, ask, 1.0, 0)

  test("初始空闲 -> Ready"):
    assertEquals(MakerQuoteLeg(0.01, 5000).step(0), MakerQuoteLeg.Step.Ready)

  test("place 后进入等确认 -> Blocked (一次穿越不会挂出两张单)"):
    val leg = MakerQuoteLeg(0.01, 5000)
    leg.place(Side.Short, Some(bbo(2999.0, 3001.0)), Price(3000.0))
    assertEquals(leg.step(0), MakerQuoteLeg.Step.Blocked)
    assertEquals(leg.step(999_999), MakerQuoteLeg.Step.Blocked, "等确认期间, 再久也不该动")

  test("Pending 确认后在簿上: requoteMs 内 Blocked, 超时 Requote"):
    val leg = MakerQuoteLeg(0.01, 5000)
    leg.place(Side.Short, Some(bbo(2999.0, 3001.0)), Price(3000.0))
    assertEquals(leg.onOrderUpdate(upd(OrderStatus.Pending, ts = 1000)), None)
    assertEquals(leg.step(5999), MakerQuoteLeg.Step.Blocked)
    assertEquals(leg.step(6001), MakerQuoteLeg.Step.Requote(OrderRef.ByExchangeId("o1"), retry = false))
    assertEquals(leg.step(6002), MakerQuoteLeg.Step.Blocked, "撤单在途, 还不能挂新单")
    assertEquals(leg.onOrderUpdate(upd(OrderStatus.Cancelled, ts = 6100)), None)
    assertEquals(leg.step(6101), MakerQuoteLeg.Step.Ready, "撤单终态到达后才回到空闲")

  test("requote 用的是回报里的交易所时钟 (调用方须传同一时钟域)"):
    val leg = MakerQuoteLeg(0.01, 5000)
    leg.place(Side.Short, Some(bbo(2999.0, 3001.0)), Price(3000.0))
    leg.onOrderUpdate(upd(OrderStatus.Pending, ts = 100_000)) // 挂上的交易所时刻
    assertEquals(leg.step(101_000), MakerQuoteLeg.Step.Blocked, "交易所时钟只过了 1s")

  test("Filled 返回成交价 (价格轴策略据此重置对冲中心), 并回到空闲"):
    val leg = MakerQuoteLeg(0.01, 5000)
    leg.place(Side.Short, Some(bbo(2999.0, 3001.0)), Price(3000.0))
    leg.onOrderUpdate(upd(OrderStatus.Pending))
    assertEquals(leg.onOrderUpdate(upd(OrderStatus.Filled, px = 3031.0)), Some(Price(3031.0)))
    assertEquals(leg.step(0), MakerQuoteLeg.Step.Ready)

  test("PartiallyFilled 视为仍在簿上 (未成交部分还要 requote)"):
    val leg = MakerQuoteLeg(0.01, 5000)
    leg.place(Side.Short, Some(bbo(2999.0, 3001.0)), Price(3000.0))
    assertEquals(leg.onOrderUpdate(upd(OrderStatus.PartiallyFilled(Coin(0.5)), ts = 1000)), None)
    assertEquals(leg.step(2000), MakerQuoteLeg.Step.Blocked)
    assertEquals(leg.step(7000), MakerQuoteLeg.Step.Requote(OrderRef.ByExchangeId("o1"), retry = false))

  test("Cancelled / Rejected / Error 都回到空闲"):
    Seq(OrderStatus.Cancelled, OrderStatus.Rejected("x"), OrderStatus.Error("y")).foreach { st =>
      val leg = MakerQuoteLeg(0.01, 5000)
      leg.place(Side.Short, Some(bbo(2999.0, 3001.0)), Price(3000.0))
      assertEquals(leg.onOrderUpdate(upd(st)), None)
      assertEquals(leg.step(0), MakerQuoteLeg.Step.Ready, s"$st 之后应可重挂")
    }

  test("Created 是本地态, 不改变状态 (仍在等确认)"):
    val leg = MakerQuoteLeg(0.01, 5000)
    leg.place(Side.Short, Some(bbo(2999.0, 3001.0)), Price(3000.0))
    assertEquals(leg.onOrderUpdate(upd(OrderStatus.Created)), None)
    assertEquals(leg.step(0), MakerQuoteLeg.Step.Blocked)

  test("被动价在盘口外侧: 卖挂 ask 之上、买挂 bid 之下 (PostOnly 不吃单)"):
    val leg = MakerQuoteLeg(0.01, 5000)
    val b = Some(bbo(2999.0, 3001.0))
    assertEquals(leg.place(Side.Short, b, Price(3000.0)), Price(3001.0 * 1.01))
    val leg2 = MakerQuoteLeg(0.01, 5000)
    assertEquals(leg2.place(Side.Long, b, Price(3000.0)), Price(2999.0 * 0.99))

  test("盘口缺失 -> 回退给定基准价 (降级, 调用方须告警)"):
    val leg = MakerQuoteLeg(0.01, 5000)
    assertEquals(leg.place(Side.Short, None, Price(3000.0)), Price(3000.0 * 1.01))

  // ---------- 撤单在途：孤儿挂单的成因与防线 ----------

  test("撤单在途不挂新单: 旧单的 Cancelled 到达之前一直 Blocked (孤儿挂单回归测试)"):
    // 从前的实现在发出撤单的同一刻就把状态清成空闲, 于是:
    //   撤 A -> 挂 B -> A 的 Cancelled 到达并清掉"等确认" -> 挂 C -> B 的回报被 C 覆盖 -> B 成孤儿。
    // 现在 A 的终态到达之前一律 Blocked, 那个交错构造不出来。
    val leg = MakerQuoteLeg(0.01, 5000)
    leg.place(Side.Short, Some(bbo(2999.0, 3001.0)), Price(3000.0))
    leg.onOrderUpdate(upd(OrderStatus.Pending, ts = 0))
    assertEquals(leg.step(6000), MakerQuoteLeg.Step.Requote(OrderRef.ByExchangeId("o1"), retry = false))
    (1 to 5).foreach(i => assertEquals(leg.step(6000 + i * 100), MakerQuoteLeg.Step.Blocked, "撤单在途"))
    leg.onOrderUpdate(upd(OrderStatus.Cancelled, ts = 6600))
    assertEquals(leg.step(6700), MakerQuoteLeg.Step.Ready)

  test("撤单在途期间收到别的 orderId 的回报 -> 忽略 (迟到消息不该清掉当前这张单的状态)"):
    val leg = MakerQuoteLeg(0.01, 5000)
    leg.place(Side.Short, Some(bbo(2999.0, 3001.0)), Price(3000.0))
    leg.onOrderUpdate(upd(OrderStatus.Pending, ts = 0))
    leg.step(6000) // -> Cancelling(o1)
    assertEquals(leg.onOrderUpdate(upd(OrderStatus.Cancelled, ts = 6100, oid = "旧单")), None)
    assertEquals(leg.step(6200), MakerQuoteLeg.Step.Blocked, "o1 的终态还没来")

  test("在簿上期间收到别的 orderId 的回报 -> 忽略"):
    val leg = MakerQuoteLeg(0.01, 5000)
    leg.place(Side.Short, Some(bbo(2999.0, 3001.0)), Price(3000.0))
    leg.onOrderUpdate(upd(OrderStatus.Pending, ts = 1000))
    assertEquals(leg.onOrderUpdate(upd(OrderStatus.Filled, px = 9999.0, ts = 1100, oid = "旧单")), None,
      "别人的成交不该被当成本腿成交 (会把对冲中心挪到一个无关价位)")
    assertEquals(leg.step(2000), MakerQuoteLeg.Step.Blocked, "本腿还在簿上")

  test("撤单确认迟迟不来 -> 重发同一张单的撤单 (不静默等下去)"):
    val leg = MakerQuoteLeg(0.01, 5000)
    leg.place(Side.Short, Some(bbo(2999.0, 3001.0)), Price(3000.0))
    leg.onOrderUpdate(upd(OrderStatus.Pending, ts = 0))
    assertEquals(leg.step(6000), MakerQuoteLeg.Step.Requote(OrderRef.ByExchangeId("o1"), retry = false))
    assertEquals(leg.step(11_001), MakerQuoteLeg.Step.Requote(OrderRef.ByExchangeId("o1"), retry = true),
      "重发的是同一个 id, 不可能造出新的孤儿单")
    assertEquals(leg.step(11_002), MakerQuoteLeg.Step.Blocked, "重发后重新开始等")

  test("撤单在途时那张单成交了 -> 回到空闲并给出成交价"):
    val leg = MakerQuoteLeg(0.01, 5000)
    leg.place(Side.Short, Some(bbo(2999.0, 3001.0)), Price(3000.0))
    leg.onOrderUpdate(upd(OrderStatus.Pending, ts = 0))
    leg.step(6000)
    assertEquals(leg.onOrderUpdate(upd(OrderStatus.Filled, px = 3031.0, ts = 6100)), Some(Price(3031.0)))
    assertEquals(leg.step(6200), MakerQuoteLeg.Step.Ready)

  test("非空闲态调用 place -> 抛错 (绕过 step 就是在制造孤儿单)"):
    val leg = MakerQuoteLeg(0.01, 5000)
    leg.place(Side.Short, Some(bbo(2999.0, 3001.0)), Price(3000.0))
    intercept[IllegalArgumentException](leg.place(Side.Short, Some(bbo(2999.0, 3001.0)), Price(3000.0)))

  test("空闲态收到迟到的回报 -> 忽略, 不改变状态"):
    val leg = MakerQuoteLeg(0.01, 5000)
    assertEquals(leg.onOrderUpdate(upd(OrderStatus.Filled, px = 9999.0)), None)
    assertEquals(leg.step(0), MakerQuoteLeg.Step.Ready)
