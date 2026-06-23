package strategy.strategies.breakouthedge.logic
import strategy.utils.hedge.MarketHedgeExecution

import hft.strategy.{OutcomeEvent, Strategy}

import hft.domain.*
import hft.messaging.{EventData, IncomeEvent, StateManager}

/** 突破式双阈值对冲单测：
  *   - 通道内 (未突破)：|净 delta| 达 **max** 阈值才对冲、未达则容忍；
  *   - 发生突破 (方向无关, 向上或向下)：降到 **min** 阈值即把 delta 中和到 0 (及时, 买/卖方通用)；
  *   - 通道预热不足：退化为 max 阈值。
  * 阈值 = pct·optionPositionEth；本测 optionPositionEth=100 -> max=3, min=1 (ETH)。
  */
class BreakoutHedgeStrategySpec extends munit.FunSuite:
  private val ex = Exchange.Binance
  private val sym = "ETHUSDT"
  private val ccy = "ETH"

  private def strat(windowBars: Int = 3, maxPct: Option[Double] = Some(0.03)) = BreakoutHedgeStrategy(
    ex, sym, ccy,
    execution = MarketHedgeExecution(ex, sym),
    optionPositionEth = 100.0, // -> max=3.0, min=1.0
    windowBars = windowBars,
    barIntervalMs = 1000,
    maxThresholdPct = maxPct,
    minThresholdPct = 0.01,
  )

  /** 暖机通道 (纯闸门模式 maxPct=None) */
  private def warmedGate: (BreakoutHedgeStrategy, StateManager) =
    val s = strat(maxPct = None)
    val sm = StateManager(Iterable(sym), orderTimeoutMs = 5000)
    Seq((0L, 100.0), (1000L, 100.0), (2000L, 99.0), (3000L, 101.0), (4000L, 100.0), (5000L, 100.0))
      .foreach((ts, p) => s.onEvent(tradeEv(p, ts), sm))
    (s, sm)

  private def tradeEv(price: Price, ts: Timestamp): IncomeEvent =
    IncomeEvent(ts, ts, EventData.MarketTradeUpdate(MarketTrade(ex, sym, price, 1.0, isBuyerMaker = false, ts)))

  /** 暖机通道 (此时 sm 无 greeks -> 不对冲, 仅推进通道)，建立窗口高/低=[99,101]。 */
  private def warmedChannel: (BreakoutHedgeStrategy, StateManager) =
    val s = strat()
    val sm = StateManager(Iterable(sym), orderTimeoutMs = 5000)
    // 收盘价序列 100,100,99,101,100 -> 近 3 根=99,101,100 -> 高101 低99; 末根(ts5000)开盘中 bucket5
    Seq((0L, 100.0), (1000L, 100.0), (2000L, 99.0), (3000L, 101.0), (4000L, 100.0), (5000L, 100.0))
      .foreach((ts, p) => s.onEvent(tradeEv(p, ts), sm))
    (s, sm)

  /** 设就绪 greeks/cashBal/perp 持仓 (净 delta = rawDelta + perpPos)。 */
  private def ready(sm: StateManager, rawDelta: Double, perpPos: Double = 0.0): Unit =
    sm.apply(IncomeEvent(0, 0, EventData.BalanceUpdate(Balance(ex, ccy, 0.0, 0))))
    sm.apply(IncomeEvent(0, 0, EventData.GreeksUpdate(Greeks(ex, ccy, rawDelta, 0.05, -0.5, 1.0, 0))))
    if perpPos != 0.0 then sm.apply(IncomeEvent(0, 0, EventData.PositionUpdate(Position(ex, sym, perpPos, 100.0, 0.0))))

  /** 在 bucket5 内 (ts=5500, 不收盘新 bar, 窗口不变) 触发评估。 */
  private def trigger(s: BreakoutHedgeStrategy, sm: StateManager, price: Price): Vector[OutcomeEvent] =
    s.onEvent(tradeEv(price, 5500L), sm)

  private def placed(out: Vector[OutcomeEvent]): Order = out match
    case Vector(OutcomeEvent.PlaceOrders(orders, _)) => orders.head
    case other                                       => fail(s"expected single PlaceOrders, got $other")

  test("通道内 + |净delta| 在 [min,max) 之间 -> 不对冲 (容忍区间漂移)"):
    val (s, sm) = warmedChannel
    ready(sm, rawDelta = 2.0) // |2| ∈ [1,3) , 价100 在通道内
    assertEquals(trigger(s, sm, 100.0), Vector.empty)

  test("通道内 + |净delta| 达 max 阈值 -> 对冲 (绝对上限)"):
    val (s, sm) = warmedChannel
    ready(sm, rawDelta = 4.0) // |4| ≥ 3
    val o = placed(trigger(s, sm, 100.0))
    assertEquals(o.side, Side.Short) // 偏多 -> 卖
    assertEquals(o.quantity, 4.0)

  test("向上突破 + 需买 + |净delta| 达 min(<max) -> 及时买对冲"):
    val (s, sm) = warmedChannel
    ready(sm, rawDelta = -2.0) // 净 delta=-2, 需买; |2| ∈ [1,3)
    val o = placed(trigger(s, sm, 102.0)) // 102 > 窗口高 101 -> 向上突破
    assertEquals(o.side, Side.Long)
    assertEquals(o.quantity, 2.0)

  test("向上突破 + 需卖 (买方/多 gamma 情形) -> 方向无关, 达 min 即卖对冲"):
    val (s, sm) = warmedChannel
    ready(sm, rawDelta = 2.0) // 净 delta=+2 需卖; 价102 向上突破 -> 方向无关 min=1 -> 对冲
    val o = placed(trigger(s, sm, 102.0))
    assertEquals(o.side, Side.Short)
    assertEquals(o.quantity, 2.0)

  test("向下突破 + 需卖 + |净delta| 达 min -> 及时卖对冲"):
    val (s, sm) = warmedChannel
    ready(sm, rawDelta = 2.0) // 需卖; |2| ∈ [1,3)
    val o = placed(trigger(s, sm, 98.0)) // 98 < 窗口低 99 -> 向下突破
    assertEquals(o.side, Side.Short)
    assertEquals(o.quantity, 2.0)

  test("通道预热不足 -> 无突破信号, 退化为 max 阈值"):
    val s = strat(windowBars = 50) // 远未暖机
    val sm = StateManager(Iterable(sym), orderTimeoutMs = 5000)
    Seq((0L, 100.0), (1000L, 105.0), (2000L, 100.0))
      .foreach((ts, p) => s.onEvent(tradeEv(p, ts), sm))
    ready(sm, rawDelta = -2.0) // |2| ∈ [1,3); 即便价"很高"也无窗口 -> 非突破 -> max=3 -> 不对冲
    assertEquals(s.onEvent(tradeEv(999.0, 2500L), sm), Vector.empty)

  // ==================== 纯突破闸门模式 (maxThresholdPct=None) ====================

  test("纯闸门: 通道内 + |净delta| 很大 -> 仍不对冲 (区间内完全不动作)"):
    val (s, sm) = warmedGate
    ready(sm, rawDelta = 10.0) // |10| 远超任何阈值, 但价100在通道内 -> 纯闸门不对冲
    assertEquals(trigger(s, sm, 100.0), Vector.empty)

  test("纯闸门: 突破 + 需买 + |净delta| 达 min -> 对冲 (卖方/空 gamma 情形)"):
    val (s, sm) = warmedGate
    ready(sm, rawDelta = -2.0) // 需买; |2| ≥ min=1
    val o = placed(trigger(s, sm, 102.0)) // 向上突破
    assertEquals(o.side, Side.Long)
    assertEquals(o.quantity, 2.0)

  test("纯闸门: 突破 + 需卖 -> 方向无关同样对冲 (买方/多 gamma 情形)"):
    val (s, sm) = warmedGate
    ready(sm, rawDelta = 5.0) // 净 delta=+5 需卖; 价102 向上突破 -> 方向无关 -> 卖对冲到 0
    val o = placed(trigger(s, sm, 102.0))
    assertEquals(o.side, Side.Short)
    assertEquals(o.quantity, 5.0)
