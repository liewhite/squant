package hft.sim

import hft.domain.*
import hft.messaging.{EventData, IncomeEvent}

/** 撮合状态转移的纯单测：无线程、无延迟、无 sleep，直接断言 (新状态, 回流事件)。 */
class SimStateSpec extends munit.FunSuite:
  private val ex = Exchange.Binance
  private val sym = "BTCUSDT"
  private def empty = SimState.empty(10_000.0)

  private def bbo(bid: Price, ask: Price, ts: Timestamp = 1): BBO = BBO(ex, sym, bid, 1.0, ask, 1.0, ts)
  private def marketEv(b: BBO): IncomeEvent = IncomeEvent.at(b.timestamp, EventData.BboUpdate(b))
  private def limit(side: Side, price: Price, tif: TimeInForce, cid: String): Order =
    Order("", ex, sym, side, OrderType.Limit(price, tif), 0.002, reduceOnly = false, clientOrderId = cid)
  private def limitRO(side: Side, price: Price, tif: TimeInForce, cid: String, qty: Quantity): Order =
    Order("", ex, sym, side, OrderType.Limit(price, tif), qty, reduceOnly = true, clientOrderId = cid)

  private def statuses(evs: Vector[IncomeEvent]): Vector[OrderStatus] =
    evs.collect { case IncomeEvent(_, _, EventData.OrderUpdated(u)) => u.status }
  private def fills(evs: Vector[IncomeEvent]): Vector[Fill] =
    evs.collect { case IncomeEvent(_, _, EventData.FillUpdate(f)) => f }

  test("非 marketable 的 PostOnly 买单 -> resting (Pending), 不成交"):
    val (s1, _) = empty.onMarket(ex, marketEv(bbo(50000, 50001)))
    val (s2, evs) = s1.onOrderArrived(ex, limit(Side.Long, 49995, TimeInForce.PostOnly, "b1"), "1")
    assertEquals(statuses(evs), Vector(OrderStatus.Pending))
    assert(s2.resting.contains("1"))
    assert(fills(evs).isEmpty)

  test("marketable 的 PostOnly -> 拒单, 不进簿不成交"):
    val (s1, _) = empty.onMarket(ex, marketEv(bbo(50000, 50001)))
    val (s2, evs) = s1.onOrderArrived(ex, limit(Side.Long, 50001, TimeInForce.PostOnly, "b1"), "1")
    assert(statuses(evs).exists(_.isInstanceOf[OrderStatus.Rejected]))
    assert(s2.resting.isEmpty)
    assert(fills(evs).isEmpty)

  test("resting 买单被卖价越过 -> 成交于挂单价, 出簿, 仓位增加"):
    val (s1, _) = empty.onMarket(ex, marketEv(bbo(50000, 50001)))
    val (s2, _) = s1.onOrderArrived(ex, limit(Side.Long, 49995, TimeInForce.PostOnly, "b1"), "1")
    val (s3, evs) = s2.onMarket(ex, marketEv(bbo(49990, 49994, ts = 2))) // ask 49994 <= 49995
    val f = fills(evs)
    assertEquals(f.map(_.price), Vector(49995.0)) // maker 价
    assertEquals(s3.resting.size, 0)
    assertEquals(s3.ledger.positions(sym).size, 0.002)

  test("行情先于成交回流: onMarket 返回的首事件是行情, 其后才是成交"):
    val (s1, _) = empty.onMarket(ex, marketEv(bbo(50000, 50001)))
    val (s2, _) = s1.onOrderArrived(ex, limit(Side.Long, 49995, TimeInForce.PostOnly, "b1"), "1")
    val (_, evs) = s2.onMarket(ex, marketEv(bbo(49990, 49994, ts = 2)))
    assert(evs.head.data.isInstanceOf[EventData.BboUpdate], "首事件应为行情转发")
    assert(evs.tail.exists(_.data.isInstanceOf[EventData.FillUpdate]), "成交回报排在行情之后")

  test("GTC 到达即可成交 -> taker 成交于对手价"):
    val (s1, _) = empty.onMarket(ex, marketEv(bbo(50000, 50001)))
    val (s2, evs) = s1.onOrderArrived(ex, limit(Side.Long, 50005, TimeInForce.GTC, "b1"), "1")
    assertEquals(fills(evs).map(_.price), Vector(50001.0)) // 吃卖价
    assert(s2.resting.isEmpty)

  test("IOC 不可成交 -> 整单取消, 不进簿"):
    val (s1, _) = empty.onMarket(ex, marketEv(bbo(50000, 50001)))
    val (s2, evs) = s1.onOrderArrived(ex, limit(Side.Long, 49000, TimeInForce.IOC, "b1"), "1")
    assertEquals(statuses(evs), Vector(OrderStatus.Cancelled))
    assert(s2.resting.isEmpty)

  test("撤单到达: 在簿则出簿并回报 Cancelled"):
    val (s1, _) = empty.onMarket(ex, marketEv(bbo(50000, 50001)))
    val (s2, _) = s1.onOrderArrived(ex, limit(Side.Long, 49995, TimeInForce.PostOnly, "b1"), "1")
    val (s3, evs) = s2.onCancelArrived(ex, "1")
    assertEquals(statuses(evs), Vector(OrderStatus.Cancelled))
    assert(s3.resting.isEmpty)

  test("撤单到达但订单已不在簿 (已成交) -> 无事发生"):
    val (s2, evs) = empty.onCancelArrived(ex, "404")
    assert(evs.isEmpty)
    assertEquals(s2, empty)

  test("reduceOnly 卖单无多头持仓 -> 不成交并回 Cancelled (撮合层禁止反向开仓)"):
    val (s1, _) = empty.onMarket(ex, marketEv(bbo(50000, 50001)))
    val (s2, _) = s1.onOrderArrived(ex, limitRO(Side.Short, 50010, TimeInForce.PostOnly, "s1", 0.002), "1") // 上方 resting
    val (s3, evs) = s2.onMarket(ex, marketEv(bbo(50011, 50012, ts = 2))) // bid 50011 >= 50010 -> 越价
    assert(fills(evs).isEmpty, "无多头可平, 不应成交")
    assert(statuses(evs).contains(OrderStatus.Cancelled), "reduceOnly 无可平 -> Cancelled")
    assert(s3.ledger.positions.get(sym).forall(_.isEmpty), "不得反向开出空头")

  test("reduceOnly 卖单数量超过多头 -> 截断到持仓, 不反手"):
    val (s1, _) = empty.onMarket(ex, marketEv(bbo(50000, 50001)))
    val (s2, _) = s1.onOrderArrived(ex, limit(Side.Long, 50005, TimeInForce.GTC, "b1"), "1") // taker 开多 0.002
    val (s3, _) = s2.onOrderArrived(ex, limitRO(Side.Short, 50010, TimeInForce.PostOnly, "s1", 0.005), "2") // 平仓单量 0.005 > 持仓
    val (s4, evs) = s3.onMarket(ex, marketEv(bbo(50011, 50012, ts = 2)))
    assertEquals(fills(evs).map(_.size), Vector(0.002)) // 截断到多头 0.002
    assertEquals(s4.ledger.positions(sym).size, 0.0) // 平至 0, 不反手为 -0.003
