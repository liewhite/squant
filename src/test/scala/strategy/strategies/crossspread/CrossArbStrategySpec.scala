package strategy.strategies.crossspread

import hft.domain.*
import hft.engine.StrategyRunner
import hft.event.{AnyEvent, Event, Topics}
import hft.event.Commands.{OrderIntent, OutcomeEvent}
import hft.TestUnits.given
import strategy.strategies.crossspread.live.CrossArbStrategy
import strategy.strategies.crossspread.logic.*

/** **穿过 `StrategyRunner`** 的状态机测试。
  *
  * 这一层是必需的，不是锦上添花：`ArbPlan` 的纯逻辑单测全绿的同时，策略在实盘上第一轮下单之后
  * 就永久卡死过 —— 因为 `StrategyRunner.prepareIntent` 会**覆写 clientOrderId**，而当时的状态机
  * 拿策略自己生成的 id 去匹配回报，一条都匹配不上。bug 恰好活在纯逻辑与框架之间那道缝里。
  */
class CrossArbStrategySpec extends munit.FunSuite:
  private val rich = Instrument(Exchange.Binance, "AAPLUSDT")
  private val cheap = Instrument(Exchange.Okx, "AAPL")
  private val t0 = 1_700_000_000_000L
  private val minOrder = Coin(0.001)

  private def cfg(enable: Boolean = true, maxPos: Double = 5.0) = ArbPlan.Config(
    minDeviationBps = 5.0, roundTripCostBps = 10.0, minProfitBps = 2.0,
    qtyPerLeg = Coin(1.0), maxQuoteAgeMs = 5000, maxPositionPerLeg = Coin(maxPos), enableOrders = enable,
  )

  private def runner(enable: Boolean = true, maxPos: Double = 5.0) =
    StrategyRunner(
      CrossArbStrategy("AAPL", Set(rich, cheap), cfg(enable, maxPos), _ => minOrder, orderTimeoutMs = 15_000),
      AccountId.Live,
    )

  private def feed(r: StrategyRunner, ev: AnyEvent): Vector[OutcomeEvent] =
    r.onEvent(ev, ev.localTs).flatMap(_.as(OrderIntent)).map(_.outcome)

  private def bbo(i: Instrument, bid: Double, ask: Double, ts: Timestamp = t0) =
    Event.stamped(Topics.Bbo, BBO(i.exchange, i.symbol, bid, Coin(10.0), ask, Coin(10.0), ts), ts, ts)

  private def signal(deviationBps: Double = 8.0) = Event.stamped(
    SpreadDislocations,
    SpreadDislocation("AAPL", rich, cheap, 20.0, 12.0, 1.0, deviationBps, 8.0, 15.0, 100.5, 100.0, 500, 50, t0),
    t0, t0,
  )

  private def position(i: Instrument, size: Double, ts: Timestamp = t0) =
    Event.stamped(Topics.Position, Position(AccountId.Live, i.exchange, i.symbol, Coin(size)), ts, ts)

  /** 终态回报。**注意 clientOrderId 用一个与策略无关的值** —— 框架会覆写策略给的那个,
    * 策略必须按 (交易所, 标的) 认领, 不能按 id。 */
  private def terminal(i: Instrument, side: Side, filled: Double, ts: Timestamp = t0 + 100) =
    Event.stamped(
      Topics.OrderUpdate,
      OrderUpdate(AccountId.Live, "exch-id", Some("runner-generated-id"), i.exchange, i.symbol, side,
        OrderStatus.Filled, 100.0, 1.0, Coin(filled), reduceOnly = false, ts),
      ts, ts,
    )

  /** 喂好两腿盘口, 使边足够 (100.50 买一 / 100.00 卖一 -> 约 50bp)。 */
  private def quoted(r: StrategyRunner): StrategyRunner =
    feed(r, bbo(rich, 100.50, 100.52))
    feed(r, bbo(cheap, 99.98, 100.00))
    r

  private def placed(out: Vector[OutcomeEvent]): Vector[Order] =
    out.collect { case OutcomeEvent.PlaceOrders(orders, _) => orders }.flatten

  test("信号到位 -> 两条 IOC, 分成两条下单意图 (跨所不能挤在一条里)"):
    val r = quoted(runner())
    val out = feed(r, signal())
    assertEquals(out.size, 2, "两个交易所 -> 两条意图")
    val orders = placed(out)
    assertEquals(orders.map(o => (o.exchange, o.side)).toSet, Set((Exchange.Binance, Side.Short), (Exchange.Okx, Side.Long)))
    assert(orders.forall(_.orderType match
      case OrderType.Limit(_, TimeInForce.IOC) => true
      case _                                   => false
    ), "两条都必须是 IOC 限价")

  test("**回报按 (交易所, 标的) 认领, 不按 clientOrderId** —— 框架会覆写它"):
    // 这条就是那个 Critical 的回归测试: 从前策略拿自己生成的 id 匹配, 于是永久卡在 InFlight,
    // 腿不平的检测与平腿代码一次都跑不到。
    val r = quoted(runner())
    feed(r, signal())
    // 两腿都配平成交 -> 应回到 Idle
    feed(r, position(rich, -1.0))
    feed(r, position(cheap, 1.0))
    feed(r, terminal(rich, Side.Short, 1.0))
    val afterBoth = feed(r, terminal(cheap, Side.Long, 1.0))
    assert(afterBoth.isEmpty, "配平之后不该发单")
    // 回到 Idle 的证据: 下一条信号还能开仓
    assertEquals(feed(r, signal()).size, 2, "配平后应能接受下一条信号 —— 卡死的话这里是 0")

  test("一条腿全成、一条腿没成 -> 立刻市价 reduceOnly 平掉裸的那条"):
    val r = quoted(runner())
    feed(r, signal())
    feed(r, position(rich, -1.0)) // 卖腿成了
    feed(r, terminal(rich, Side.Short, 1.0))
    val out = feed(r, terminal(cheap, Side.Long, 0.0)) // 买腿一点没成
    val unwind = placed(out)
    assertEquals(unwind.size, 1)
    assertEquals(unwind.head.exchange, Exchange.Binance, "平的是有仓位的那条")
    assertEquals(unwind.head.side, Side.Long, "净空 -> 买回")
    assertEquals(unwind.head.quantity, Coin(1.0))
    assertEquals(unwind.head.orderType, OrderType.Market, "平腿用市价: 裸敞口, 少赚好过敞着")
    assert(unwind.head.reduceOnly, "平腿必须 reduceOnly")

  test("平腿成交后恢复 —— 能接受下一条信号"):
    val r = quoted(runner())
    feed(r, signal())
    feed(r, position(rich, -1.0))
    feed(r, terminal(rich, Side.Short, 1.0))
    feed(r, terminal(cheap, Side.Long, 0.0))
    // 平腿成交: 仓位回零
    feed(r, position(rich, 0.0))
    val afterUnwind = feed(r, terminal(rich, Side.Long, 1.0, ts = t0 + 200))
    assert(afterUnwind.isEmpty, "平掉之后不该再发单")
    assertEquals(feed(r, signal()).size, 2, "恢复后应能接受下一条信号")

  test("**平不掉就终止** —— 平腿回报到了但仓位还在"):
    // 从前"平不掉就停住"只存在于类文档里, 一行代码都没有: 平腿单被拒 (精度/reduceOnly 不符)
    // 只有柜台一条 warn, 策略随后每条信号一条 INFO, 一个持有裸敞口且永久瘫痪的实盘策略
    // 在日志里是 INFO 级别的。
    val r = quoted(runner())
    feed(r, signal())
    feed(r, position(rich, -1.0))
    feed(r, terminal(rich, Side.Short, 1.0))
    feed(r, terminal(cheap, Side.Long, 0.0))
    // 平腿单回来了, 但仓位没动 (被拒)
    val e = intercept[IllegalStateException](feed(r, terminal(rich, Side.Long, 0.0, ts = t0 + 200)))
    assert(e.getMessage.contains("平腿之后仍有裸敞口"), e.getMessage)

  test("在途期间不开新仓"):
    val r = quoted(runner())
    feed(r, signal())
    assert(feed(r, signal()).isEmpty, "上一轮两条 IOC 还没回来, 不该再开")

  test("未配平时不开新仓 —— 配平由**仓位**派生, 重启也成立"):
    val r = quoted(runner())
    feed(r, position(rich, -1.0)) // 只有一条腿有仓位 = 裸的
    assert(feed(r, signal()).isEmpty, "裸敞口上不该再叠一层")

  test("到单腿仓位上限 -> 不再加仓"):
    val r = quoted(runner(maxPos = 2.0))
    feed(r, position(rich, -2.0))
    feed(r, position(cheap, 2.0)) // 配平, 但已到上限
    assert(feed(r, signal()).isEmpty, "到上限就不该再开")

  test("enableOrders=false -> 一张单都不发"):
    val r = quoted(runner(enable = false))
    assert(feed(r, signal()).isEmpty)

  test("信号里有未声明的腿 -> 一张单都不发"):
    // 检测器按 ticker 的全部两两组合出信号, 三家所就有三对; 在没声明的标的上下单会拿不到回报。
    val hl = Instrument(Exchange.Hyperliquid, "AAPL")
    val r = quoted(runner())
    val foreign = Event.stamped(
      SpreadDislocations,
      SpreadDislocation("AAPL", hl, cheap, 20.0, 12.0, 1.0, 8.0, 8.0, 15.0, 100.5, 100.0, 500, 50, t0),
      t0, t0,
    )
    assert(feed(r, foreign).isEmpty, "未声明的腿不该下单")
