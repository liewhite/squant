package strategy.research

import hft.strategy.{OutcomeEvent, Strategy}

import hft.domain.*
import hft.engine.StrategyRunner
import hft.messaging.{EventData, IncomeEvent, StateManager}

/** MaMacdGridStrategy 单测：均线/MACD 方向分档下单的结构正确性。
  *
  * 直接调 strategy.onEvent (绕过 StrategyRunner，故挂单不进 StateManager —— present 恒空，每次都按期望补挂)。
  * 用加速上行/下行路径制造"价在均线上/下 + MACD 柱连续升/降"的预热状态，断言挂单方向/档距/reduceOnly。
  */
class MaMacdGridStrategySpec extends munit.FunSuite:
  private val ex = Exchange.Binance
  private val sym = "ETHUSDT"
  private val hour = 3_600_000L
  private val small = 0.003 // addSpacing (加仓)
  private val large = 0.012 // largeSpacing (顺势止盈, 宽)
  private val exitS = 0.001 // exitSpacing (MACD 反向后的移动止盈, 窄)

  private def strat = MaMacdGridStrategy(
    ex, sym, maPeriodBars = 60, barIntervalMs = hour, risingBars = 2,
    addSpacing = small, largeSpacing = large, exitSpacing = exitS, baseQty = 0.5, maxPositionCoin = 10.0,
  )

  private def tradeEv(ts: Long, px: Double): IncomeEvent =
    IncomeEvent(ts, ts, EventData.MarketTradeUpdate(MarketTrade(ex, sym, px, 1.0, isBuyerMaker = false, ts)))

  /** 喂 bars 根 (每小时一笔) 预热 K 线/指标，忽略中间输出。返回末根价格。 */
  private def warm(s: MaMacdGridStrategy, sm: StateManager, priceAt: Int => Double, bars: Int): Double =
    var last = 0.0
    (0 until bars).foreach { i =>
      val ts = i.toLong * hour + 60_000L
      val px = priceAt(i)
      last = px
      sm.apply(tradeEv(ts, px))
      s.onEvent(tradeEv(ts, px), sm) // 忽略
    }
    last

  /** 在末根之后再发一笔受控成交，返回 (输出, 该成交价) */
  private def step(s: MaMacdGridStrategy, sm: StateManager, bars: Int, px: Double): Vector[OutcomeEvent] =
    val ev = tradeEv(bars.toLong * hour + 60_000L, px)
    sm.apply(ev)
    s.onEvent(ev, sm)

  private def limitOf(o: Order): (Price, TimeInForce) = o.orderType match
    case OrderType.Limit(p, t) => (p, t)
    case x                     => fail(s"expected Limit, got $x")

  private def near(a: Double, b: Double): Unit = assert(math.abs(a - b) / b < 1e-6, s"expected≈$b got $a")

  test("预热不足 (均线未就绪) -> 不挂单"):
    val s = strat
    val sm = StateManager(Iterable(sym), 5000)
    // 仅 10 根，远不足 60 均线
    val out = (0 until 10).flatMap { i =>
      val ev = tradeEv(i.toLong * hour + 1, 1000.0 + i)
      sm.apply(ev); s.onEvent(ev, sm)
    }
    assertEquals(out, Seq.empty)

  test("价在均线上 + MACD 柱连续上升, 无持仓 -> 仅加仓买单 (小档距, 均线下方)"):
    val s = strat
    val sm = StateManager(Iterable(sym), 5000)
    val bars = 90
    // 加速上行: 价远高于 60 均线, MACD 柱持续走高
    warm(s, sm, i => 1000.0 + i.toDouble * i * 0.5, bars)
    val px = 1000.0 + bars.toDouble * bars * 0.5
    val out = step(s, sm, bars, px)
    val orders = out.collect { case OutcomeEvent.PlaceOrders(os, _) => os }.flatten
    // 无持仓 -> 只有加仓买 (BuyAccum)，无平仓卖
    assertEquals(orders.size, 1, s"got $out")
    val o = orders.head
    assertEquals(o.side, Side.Long)
    assertEquals(o.reduceOnly, false)
    val (p, tif) = limitOf(o)
    assertEquals(tif, TimeInForce.GTC)
    near(p, px * (1 - small))

  test("价在均线上 + MACD 柱连续上升, 有多仓 -> 加仓买(小) + 平仓卖(大)"):
    val s = strat
    val sm = StateManager(Iterable(sym), 5000)
    val bars = 90
    warm(s, sm, i => 1000.0 + i.toDouble * i * 0.5, bars)
    val px = 1000.0 + bars.toDouble * bars * 0.5
    sm.apply(IncomeEvent(0, 0, EventData.PositionUpdate(Position(ex, sym, 2.0, px, 0.0)))) // 注入多仓
    val orders = step(s, sm, bars, px).collect { case OutcomeEvent.PlaceOrders(os, _) => os }.flatten
    val buy = orders.find(o => o.side == Side.Long && !o.reduceOnly).getOrElse(fail(s"no BuyAccum: $orders"))
    val sell = orders.find(o => o.side == Side.Short && o.reduceOnly).getOrElse(fail(s"no SellClose: $orders"))
    near(limitOf(buy)._1, px * (1 - small))
    near(limitOf(sell)._1, px * (1 + large)) // 平仓卖用大档距
    near(sell.quantity, 0.5)                 // min(baseQty, pos=2) = 0.5

  test("价在均线下 + MACD 柱连续下降, 无持仓 -> 仅加仓卖单 (小档距, 均线上方)"):
    val s = strat
    val sm = StateManager(Iterable(sym), 5000)
    val bars = 90
    // 加速下行: 价远低于均线, MACD 柱持续走低
    warm(s, sm, i => 6000.0 - i.toDouble * i * 0.5, bars)
    val px = 6000.0 - bars.toDouble * bars * 0.5
    val orders = step(s, sm, bars, px).collect { case OutcomeEvent.PlaceOrders(os, _) => os }.flatten
    assertEquals(orders.size, 1, s"got $orders")
    val o = orders.head
    assertEquals(o.side, Side.Short)
    assertEquals(o.reduceOnly, false)
    near(limitOf(o)._1, px * (1 + small))

  test("价在均线下 + MACD 柱连续下降, 有空仓 -> 加仓卖(小) + 平仓买(大)"):
    val s = strat
    val sm = StateManager(Iterable(sym), 5000)
    val bars = 90
    warm(s, sm, i => 6000.0 - i.toDouble * i * 0.5, bars)
    val px = 6000.0 - bars.toDouble * bars * 0.5
    sm.apply(IncomeEvent(0, 0, EventData.PositionUpdate(Position(ex, sym, -2.0, px, 0.0)))) // 注入空仓
    val orders = step(s, sm, bars, px).collect { case OutcomeEvent.PlaceOrders(os, _) => os }.flatten
    val sell = orders.find(o => o.side == Side.Short && !o.reduceOnly).getOrElse(fail(s"no SellAccum: $orders"))
    val buy = orders.find(o => o.side == Side.Long && o.reduceOnly).getOrElse(fail(s"no BuyClose: $orders"))
    near(limitOf(sell)._1, px * (1 + small))
    near(limitOf(buy)._1, px * (1 - large))
    near(buy.quantity, 0.5)

  // ==================== 网格状态机 (present 去重 / 撤单) ====================

  private val metas = Map(
    (ex, sym) -> SymbolMeta(ex, sym, tickSize = 0.01, sizeStep = 0.001, minOrderSize = 0.001, contractSize = 1.0)
  )

  test("已有同档挂单时不重复下单 (present 去重, 经 StrategyRunner 登记 pending)"):
    val runner = StrategyRunner(strat, metas)
    val bars = 90
    // 经 runner 预热: 指标就绪后会下出并登记一张 BuyAccum, 后续步因 present 命中不再重复
    (0 until bars).foreach { i =>
      val ts = i.toLong * hour + 60_000L
      runner.onEvent(tradeEv(ts, 1000.0 + i.toDouble * i * 0.5), ts)
    }
    val px = 1000.0 + bars.toDouble * bars * 0.5
    // 已挂 BuyAccum (pos 仍为 0 -> 无 SellClose), 再来一笔不应再下加仓买
    val stepTs = bars.toLong * hour + 60_000L
    val out = runner.onEvent(tradeEv(stepTs, px), stepTs)
    val newAccumBuys = out.collect { case OutcomeEvent.PlaceOrders(os, _) => os }.flatten
      .count(o => o.side == Side.Long && !o.reduceOnly)
    assertEquals(newAccumBuys, 0, s"BuyAccum 已在 present 中, 不应重复下单: $out")

  test("档位不再期望且挂单已确认 -> 撤单; 仅 Created (未确认) -> 不撤"):
    def setupBullWithInjected(confirm: Boolean): Vector[OutcomeEvent] =
      val s = strat
      val sm = StateManager(Iterable(sym), 5000)
      val bars = 90
      warm(s, sm, i => 1000.0 + i.toDouble * i * 0.5, bars) // 价在均线上 (bull)
      val px = 1000.0 + bars.toDouble * bars * 0.5
      // 注入一张 bull 下不该存在的 SellAccum (Short, 非 reduceOnly) 挂单
      val injected = Order("oid1", ex, sym, Side.Short, OrderType.Limit(px * 1.001, TimeInForce.GTC), 0.5, reduceOnly = false, "cid1")
      sm.addPendingOrder(injected, 0L) // Created
      if confirm then // 推到 Pending (已确认) 才可撤
        sm.apply(IncomeEvent(0, 0, EventData.OrderUpdated(
          OrderUpdate("oid1", Some("cid1"), ex, sym, Side.Short, OrderStatus.Pending, px * 1.001, 0.5, 0.0, 0.0, 0)
        )))
      step(s, sm, bars, px)

    val confirmed = setupBullWithInjected(confirm = true)
    assert(
      confirmed.exists { case OutcomeEvent.CancelOrder(`ex`, `sym`, "oid1") => true; case _ => false },
      s"已确认的越界挂单应被撤: $confirmed",
    )
    val created = setupBullWithInjected(confirm = false)
    assert(
      !created.exists { case _: OutcomeEvent.CancelOrder => true; case _ => false },
      s"仅 Created (未确认) 的挂单不应被撤 (无 orderId 可撤): $created",
    )

  test("双确认离场: 价跌破均线 且 柱≥5根水下 -> 多头止盈切窄移动并随价重挂; 已贴窄档则不重挂"):
    // 先温和上行 (建多/暖指标), 再持续下行使价跌破均线 且 MACD 柱连续多根<0 (双确认)
    val up = 70; val down = 35; val bars = up + down
    def priceAt(i: Int): Double =
      if i < up then 1000.0 + i.toDouble * 15.0 // 温和上行
      else 1000.0 + (up - 1).toDouble * 15.0 - (i - up + 1).toDouble * 40.0 // 持续下行, 破均线 + 柱翻负
    val stepPx = priceAt(bars - 1) - 40.0 // 再跌一根

    def run(restingClosePx: Double): Vector[OutcomeEvent] =
      val s = strat
      val sm = StateManager(Iterable(sym), 5000)
      warm(s, sm, priceAt, bars)
      sm.apply(IncomeEvent(0, 0, EventData.PositionUpdate(Position(ex, sym, 2.0, stepPx, 0.0)))) // 多仓 (现已逆势)
      val o = Order("oidC", ex, sym, Side.Short, OrderType.Limit(restingClosePx, TimeInForce.GTC), 0.5, reduceOnly = true, "cidC")
      sm.addPendingOrder(o, 0L)
      sm.apply(IncomeEvent(0, 0, EventData.OrderUpdated(
        OrderUpdate("oidC", Some("cidC"), ex, sym, Side.Short, OrderStatus.Pending, restingClosePx, 0.5, 0.0, 0.0, 0)
      )))
      step(s, sm, bars, stepPx)

    def cancelled(out: Vector[OutcomeEvent]) =
      out.exists { case OutcomeEvent.CancelOrder(`ex`, `sym`, "oidC") => true; case _ => false }
    // 双确认成立 -> 遗留宽止盈单 (现价 +large) 被切窄移动止盈重挂
    assert(cancelled(run(stepPx * (1 + large))), "双确认(破均线+柱连续水下)后, 宽止盈单应被切窄移动止盈重挂")
    // 已贴合窄档 (现价 +exitS) -> 不重挂
    assert(!cancelled(run(stepPx * (1 + exitS))), "已在窄档位 -> 不应重挂")

  // ==================== 方案1: 平仓至少 max(占比×持仓, baseQty) ====================

  test("方案1 closeMinFraction=0.5: 大持仓单次平仓量 = max(半仓, baseQty)"):
    val s = MaMacdGridStrategy(
      ex, sym, maPeriodBars = 60, barIntervalMs = hour, risingBars = 2,
      addSpacing = small, largeSpacing = large, exitSpacing = exitS,
      closeMinFraction = 0.5, baseQty = 0.5, maxPositionCoin = 10.0,
    )
    val sm = StateManager(Iterable(sym), 5000)
    val bars = 90
    warm(s, sm, i => 1000.0 + i.toDouble * i * 0.5, bars)
    val px = 1000.0 + bars.toDouble * bars * 0.5
    sm.apply(IncomeEvent(0, 0, EventData.PositionUpdate(Position(ex, sym, 4.0, px, 0.0)))) // 多仓 4
    val orders = step(s, sm, bars, px).collect { case OutcomeEvent.PlaceOrders(os, _) => os }.flatten
    val sell = orders.find(o => o.side == Side.Short && o.reduceOnly).getOrElse(fail(s"no SellClose: $orders"))
    near(sell.quantity, 2.0) // min(4, max(4*0.5=2, 0.5)) = 2 (半仓), 而非逐笔 0.5

  // ==================== 方案2: 价偏离均线 N×ATR -> 移动止盈 + 停止加仓 ====================

  test("方案2 atrStretchN 触发: 价远超均线 -> 停止加多 且 多头止盈切窄移动 (exitSpacing)"):
    val s = MaMacdGridStrategy(
      ex, sym, maPeriodBars = 60, barIntervalMs = hour, risingBars = 2,
      addSpacing = small, largeSpacing = large, exitSpacing = exitS,
      atrStretchN = 0.01, atrPeriodBars = 14, baseQty = 0.5, maxPositionCoin = 10.0,
    )
    val sm = StateManager(Iterable(sym), 5000)
    val bars = 90
    warm(s, sm, i => 1000.0 + i.toDouble * i * 0.5, bars) // 加速上行: 价远在均线上, ATR>0
    val px = 1000.0 + bars.toDouble * bars * 0.5
    sm.apply(IncomeEvent(0, 0, EventData.PositionUpdate(Position(ex, sym, 2.0, px, 0.0)))) // 多仓
    val orders = step(s, sm, bars, px).collect { case OutcomeEvent.PlaceOrders(os, _) => os }.flatten
    // 拉伸过度 -> 不再加多
    assert(!orders.exists(o => o.side == Side.Long && !o.reduceOnly), s"拉伸过度应停止加多: $orders")
    // 止盈切窄 (exitSpacing) 而非宽静态 (largeSpacing)
    val sell = orders.find(o => o.side == Side.Short && o.reduceOnly).getOrElse(fail(s"no SellClose: $orders"))
    near(limitOf(sell)._1, px * (1 + exitS))
