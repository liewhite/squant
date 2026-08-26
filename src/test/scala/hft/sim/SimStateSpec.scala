package hft.sim

import hft.domain.*
import hft.event.{AnyEvent, Event, Topics}
import hft.TestUnits.given

/** 撮合状态转移的纯单测：无线程、无延迟、无 sleep，直接断言 (新状态, 回流事件)。 */
class SimStateSpec extends munit.FunSuite:
  /** contractSize = 1：撮合入口的币本位还原对这些用例是恒等变换 */
  private val metasOf: Map[(Exchange, Symbol), SymbolMeta] =
    Map((Exchange.Binance, "BTCUSDT") -> SymbolMeta(Exchange.Binance, "BTCUSDT", 0.1, 0.001, 0.001, 1.0))

  private val ex = Exchange.Binance
  private val sym = "BTCUSDT"
  private def empty = SimState.empty(AccountId.Live, 10_000.0)

  private def bbo(bid: Price, ask: Price, ts: Timestamp = 1): BBO = BBO(ex, sym, bid, Coin(1.0), ask, Coin(1.0), ts)
  private def marketEv(b: BBO): AnyEvent = Event.at(Topics.Bbo, b, b.timestamp)
  private def limit(side: Side, price: Price, tif: TimeInForce, cid: String): Order =
    Order("", ex, sym, side, OrderType.Limit(price, tif), 0.002, reduceOnly = false, clientOrderId = cid)
  private def limitRO(side: Side, price: Price, tif: TimeInForce, cid: String, qty: Coin): Order =
    Order("", ex, sym, side, OrderType.Limit(price, tif), qty, reduceOnly = true, clientOrderId = cid)

  private def statuses(evs: Vector[AnyEvent]): Vector[OrderStatus] =
    evs.flatMap(_.as(Topics.OrderUpdate)).map(_.status)
  private def fills(evs: Vector[AnyEvent]): Vector[Fill] =
    evs.flatMap(_.as(Topics.Fill))

  test("非 marketable 的 PostOnly 买单 -> resting (Pending), 不成交"):
    val (s1, _) = empty.onMarket(ex, marketEv(bbo(50000, 50001)), 1)
    val (s2, evs) = s1.onOrderArrived(ex, limit(Side.Long, 49995, TimeInForce.PostOnly, "b1"), "1", 1)
    assertEquals(statuses(evs), Vector(OrderStatus.Pending))
    assert(s2.resting.contains("1"))
    assert(fills(evs).isEmpty)

  test("marketable 的 PostOnly -> 拒单, 不进簿不成交"):
    val (s1, _) = empty.onMarket(ex, marketEv(bbo(50000, 50001)), 1)
    val (s2, evs) = s1.onOrderArrived(ex, limit(Side.Long, 50001, TimeInForce.PostOnly, "b1"), "1", 1)
    assert(statuses(evs).exists(_.isInstanceOf[OrderStatus.Rejected]))
    assert(s2.resting.isEmpty)
    assert(fills(evs).isEmpty)

  test("resting 买单只被卖价**触及** (ask == 挂单价) -> 不成交 (maker 悲观: 队列未消化到我)"):
    val (s1, _) = empty.onMarket(ex, marketEv(bbo(50000, 50001)), 1)
    val (s2, _) = s1.onOrderArrived(ex, limit(Side.Long, 49995, TimeInForce.PostOnly, "b1"), "1", 1)
    val (s3, evs) = s2.onMarket(ex, marketEv(bbo(49990, 49995, ts = 2)), 2) // ask 恰等于挂单价
    assert(fills(evs).isEmpty)
    assert(s3.resting.contains("1"), "仅触及不成交, 挂单仍在簿")

  test("resting 卖单只被买价**触及** (bid == 挂单价) -> 不成交"):
    val (s1, _) = empty.onMarket(ex, marketEv(bbo(50000, 50001)), 1)
    val (s2, _) = s1.onOrderArrived(ex, limit(Side.Short, 50005, TimeInForce.PostOnly, "s1"), "1", 1)
    val (s3, evs) = s2.onMarket(ex, marketEv(bbo(50005, 50006, ts = 2)), 2) // bid 恰等于挂单价
    assert(fills(evs).isEmpty)
    assert(s3.resting.contains("1"))

  test("同一价位: 到达单按 taker 成交, 簿上单不成交 (悲观间隙, 主动吃 vs 被动等)"):
    val (s1, _) = empty.onMarket(ex, marketEv(bbo(50000, 50001)), 1)
    val (s2, _) = s1.onOrderArrived(ex, limit(Side.Long, 49995, TimeInForce.PostOnly, "resting"), "1", 1)
    val (s3, quiet) = s2.onMarket(ex, marketEv(bbo(49990, 49995, ts = 2)), 2) // ask 触及簿上单价
    assert(fills(quiet).isEmpty, "簿上单被动排队, 不成交")
    // 同一盘口下, 一张新单此刻到达同一价位 -> 主动吃单成交
    val (_, arriving) = s3.onOrderArrived(ex, limit(Side.Long, 49995, TimeInForce.GTC, "taker"), "2", 2)
    assertEquals(fills(arriving).map(_.price.value), Vector(49995.0))

  test("resting 买单被卖价严格穿越 -> 成交于挂单价, 出簿, 仓位增加"):
    val (s1, _) = empty.onMarket(ex, marketEv(bbo(50000, 50001)), 1)
    val (s2, _) = s1.onOrderArrived(ex, limit(Side.Long, 49995, TimeInForce.PostOnly, "b1"), "1", 1)
    val (s3, evs) = s2.onMarket(ex, marketEv(bbo(49990, 49994, ts = 2)), 2) // ask 49994 < 49995, 穿越
    val f = fills(evs)
    assertEquals(f.map(_.price.value), Vector(49995.0)) // maker 价
    assertEquals(s3.resting.size, 0)
    assertEquals(s3.ledger.positions(sym).size.value, 0.002)

  test("撮合输出只含回报, 不回显行情 (转发行情是网关职责, 不是撮合的)"):
    val (s1, _) = empty.onMarket(ex, marketEv(bbo(50000, 50001)), 1)
    val (s2, _) = s1.onOrderArrived(ex, limit(Side.Long, 49995, TimeInForce.PostOnly, "b1"), "1", 1)
    val (_, evs) = s2.onMarket(ex, marketEv(bbo(49990, 49994, ts = 2)), 2)
    assert(!evs.exists(_.is(Topics.Bbo)), s"不应回显行情, got $evs")
    assert(evs.exists(_.is(Topics.Fill)), "应产出成交回报")

  test("无事可撮合的行情 -> 空输出 (而非回显一条行情)"):
    val (_, evs) = empty.onMarket(ex, marketEv(bbo(50000, 50001)), 1)
    assertEquals(evs, Vector.empty)

  test("GTC 到达即可成交 -> taker 成交于对手价"):
    val (s1, _) = empty.onMarket(ex, marketEv(bbo(50000, 50001)), 1)
    val (s2, evs) = s1.onOrderArrived(ex, limit(Side.Long, 50005, TimeInForce.GTC, "b1"), "1", 1)
    assertEquals(fills(evs).map(_.price.value), Vector(50001.0)) // 吃卖价
    assert(s2.resting.isEmpty)

  test("IOC 不可成交 -> 整单取消, 不进簿"):
    val (s1, _) = empty.onMarket(ex, marketEv(bbo(50000, 50001)), 1)
    val (s2, evs) = s1.onOrderArrived(ex, limit(Side.Long, 49000, TimeInForce.IOC, "b1"), "1", 1)
    assertEquals(statuses(evs), Vector(OrderStatus.Cancelled))
    assert(s2.resting.isEmpty)

  test("撤单到达: 在簿则出簿并回报 Cancelled"):
    val (s1, _) = empty.onMarket(ex, marketEv(bbo(50000, 50001)), 1)
    val (s2, _) = s1.onOrderArrived(ex, limit(Side.Long, 49995, TimeInForce.PostOnly, "b1"), "1", 1)
    val (s3, evs) = s2.onCancelArrived(ex, OrderRef.ByExchangeId("1"), 1)
    assertEquals(statuses(evs), Vector(OrderStatus.Cancelled))
    assert(s3.resting.isEmpty)

  test("撤单到达但订单已不在簿 (已成交) -> 无事发生"):
    val (s2, evs) = empty.onCancelArrived(ex, OrderRef.ByExchangeId("404"), 1)
    assert(evs.isEmpty)
    assertEquals(s2, empty)

  test("reduceOnly 卖单无多头持仓 -> 不成交并回 Cancelled (撮合层禁止反向开仓)"):
    val (s1, _) = empty.onMarket(ex, marketEv(bbo(50000, 50001)), 1)
    val (s2, _) = s1.onOrderArrived(ex, limitRO(Side.Short, 50010, TimeInForce.PostOnly, "s1", 0.002), "1", 1) // 上方 resting
    val (s3, evs) = s2.onMarket(ex, marketEv(bbo(50011, 50012, ts = 2)), 2) // bid 50011 >= 50010 -> 越价
    assert(fills(evs).isEmpty, "无多头可平, 不应成交")
    assert(statuses(evs).contains(OrderStatus.Cancelled), "reduceOnly 无可平 -> Cancelled")
    assert(s3.ledger.positions.get(sym).forall(_.isEmpty), "不得反向开出空头")

  test("reduceOnly 卖单数量超过多头 -> 截断到持仓, 不反手"):
    val (s1, _) = empty.onMarket(ex, marketEv(bbo(50000, 50001)), 1)
    val (s2, _) = s1.onOrderArrived(ex, limit(Side.Long, 50005, TimeInForce.GTC, "b1"), "1", 1) // taker 开多 0.002
    val (s3, _) = s2.onOrderArrived(ex, limitRO(Side.Short, 50010, TimeInForce.PostOnly, "s1", 0.005), "2", 2) // 平仓单量 0.005 > 持仓
    val (s4, evs) = s3.onMarket(ex, marketEv(bbo(50011, 50012, ts = 2)), 2)
    assertEquals(fills(evs).map(_.size.value), Vector(0.002)) // 截断到多头 0.002
    assertEquals(s4.ledger.positions(sym).size.value, 0.0) // 平至 0, 不反手为 -0.003

  test("撮合按到达序 (FIFO) 而非哈希序: 同价 reduceOnly 竞争同一持仓, 先到先成交"):
    // 开多 0.003，两张同价 reduceOnly 卖单 (各 0.002, 合计 0.004 > 持仓) 同刻越价竞争。
    // 价相同 -> 价格优先级相同 -> 按到达序: 先到 "early" 全成 0.002, 后到 "late" 仅余 0.001。
    val longBuy = Order("", ex, sym, Side.Long, OrderType.Limit(50005, TimeInForce.GTC), 0.003, reduceOnly = false, "b1")
    val (s1, _) = empty.onMarket(ex, marketEv(bbo(50000, 50001)), 1)
    val (s2, _) = s1.onOrderArrived(ex, longBuy, "1", 1) // 开多 0.003
    val (s3, _) = s2.onOrderArrived(ex, limitRO(Side.Short, 50010, TimeInForce.PostOnly, "early", 0.002), "2", 1) // 先到 seq0
    val (s4, _) = s3.onOrderArrived(ex, limitRO(Side.Short, 50010, TimeInForce.PostOnly, "late", 0.002), "3", 1)  // 后到 seq1
    val (_, evs) = s4.onMarket(ex, marketEv(bbo(50011, 50012, ts = 2)), 2) // bid 50011 >= 50010 -> 两张同刻越价
    val filledOf = evs.flatMap(_.as(Topics.OrderUpdate)).collect {
      // Filled 的 filledQuantity 即本笔成交量 (撮合层不产生部分成交的中间态)
      case u if u.status == OrderStatus.Filled => (u.clientOrderId, u.filledQuantity.value)
    }
    assertEquals(filledOf, Vector((Some("early"), 0.002), (Some("late"), 0.001)))
