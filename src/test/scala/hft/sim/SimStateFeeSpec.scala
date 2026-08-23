package hft.sim

import hft.domain.*
import hft.event.{AnyEvent, Event, Topics}
import hft.TestUnits.given

/** 撮合手续费单测：maker/taker 成交按对应费率扣现金，非成交路径 (拒单/撤单) 不扣费。 */
class SimStateFeeSpec extends munit.FunSuite:
  /** contractSize = 1：撮合入口的币本位还原对这些用例是恒等变换 */
  private val metasOf: Map[(Exchange, Symbol), SymbolMeta] =
    Map((Exchange.Binance, "ETHUSDT") -> SymbolMeta(Exchange.Binance, "ETHUSDT", 0.1, 0.001, 0.001, 1.0))

  private val ex = Exchange.Binance
  private val sym = "ETHUSDT"
  private val makerFee = 0.001 // 0.1%
  private val takerFee = 0.002 // 0.2%
  private val initCash = 10_000.0

  private def state = SimState.empty(AccountId.Live, initCash, makerFee, takerFee)
  private def bboEv(bid: Price, ask: Price, ts: Timestamp): AnyEvent =
    Event.stamped(Topics.Bbo, BBO(ex, sym, bid, Coin(1.0), ask, Coin(1.0), ts), ts, ts)
  private def order(side: Side, ot: OrderType, qty: Coin): Order =
    Order("", ex, sym, side, ot, qty, reduceOnly = false, clientOrderId = "c1")
  private def near(a: Double, b: Double): Unit = assert(math.abs(a - b) < 1e-9, s"expected $b got $a")

  test("taker 成交扣 taker 费 (吃单开仓)"):
    val (s1, _) = state.onMarket(ex, bboEv(100.0, 100.0, 1), 1) // 设 lastBbo
    val (s2, _) = s1.onOrderArrived(ex, order(Side.Long, OrderType.Market, 2.0), "o1", 1)
    // 吃单成交 @100, 开多 2, fee = 100*2*0.002 = 0.4
    near(s2.ledger.cash, initCash - 0.4)
    near(s2.ledger.positions(sym).size.value, 2.0)

  test("maker 成交扣 maker 费 (resting 单被越价)"):
    val (s1, _) = state.onMarket(ex, bboEv(100.0, 100.0, 1), 1)
    // 买单挂 99 (< ask 100, 不可成交) -> resting
    val (s2, evs2) = s1.onOrderArrived(ex, order(Side.Long, OrderType.Limit(99.0, TimeInForce.GTC), 2.0), "o1", 1)
    assert(s2.resting.nonEmpty, "应 resting")
    near(s2.ledger.cash, initCash) // resting 不扣费
    // 价格下穿至 98 -> ask 98 <= 99 越价 -> maker 成交 @99
    val (s3, _) = s2.onMarket(ex, bboEv(98.0, 98.0, 2), 2)
    near(s3.ledger.cash, initCash - 99.0 * 2.0 * makerFee) // fee = 0.198
    near(s3.ledger.positions(sym).size.value, 2.0)

  test("PostOnly 越价被拒 -> 不成交不扣费"):
    val (s1, _) = state.onMarket(ex, bboEv(100.0, 100.0, 1), 1)
    // PostOnly 买单挂 100 >= ask 100 会吃单 -> 拒单
    val (s2, evs) = s1.onOrderArrived(ex, order(Side.Long, OrderType.Limit(100.0, TimeInForce.PostOnly), 2.0), "o1", 1)
    assert(s2.resting.isEmpty, "拒单不 resting")
    near(s2.ledger.cash, initCash) // 不扣费
    assert(!s2.ledger.positions.contains(sym) || s2.ledger.positions(sym).isEmpty)

  test("撤单 -> 不扣费"):
    val (s1, _) = state.onMarket(ex, bboEv(100.0, 100.0, 1), 1)
    val (s2, _) = s1.onOrderArrived(ex, order(Side.Long, OrderType.Limit(99.0, TimeInForce.GTC), 2.0), "o1", 1)
    val restingId = s2.resting.keys.head
    val (s3, _) = s2.onCancelArrived(ex, OrderRef.ByExchangeId(restingId), 1)
    assert(s3.resting.isEmpty)
    near(s3.ledger.cash, initCash)

  test("零费率 (默认) 不扣费 -> 与历史行为一致"):
    var s = SimState.empty(AccountId.Live, initCash) // 默认 maker/taker = 0
    val (s1, _) = s.onMarket(ex, bboEv(100.0, 100.0, 1), 1)
    val (s2, _) = s1.onOrderArrived(ex, order(Side.Long, OrderType.Market, 2.0), "o1", 1)
    near(s2.ledger.cash, initCash) // 开仓无已实现、无费
