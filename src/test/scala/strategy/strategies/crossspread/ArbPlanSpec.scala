package strategy.strategies.crossspread

import hft.domain.*
import hft.TestUnits.given
import strategy.strategies.crossspread.logic.*

/** 对敲决策与对账的契约。两层判据 (偏离 + 可执行的边)、成本门槛、以及**腿不平**的处理。 */
class ArbPlanSpec extends munit.FunSuite:
  private val rich = Instrument(Exchange.Binance, "AAPLUSDT")
  private val cheap = Instrument(Exchange.Okx, "AAPL")
  private val t0 = 1_700_000_000_000L

  private val cfg = ArbPlan.Config(
    minDeviationBps = 5.0,
    roundTripCostBps = 10.0,
    minProfitBps = 2.0,
    qtyPerLeg = Coin(1.0),
    maxQuoteAgeMs = 2000,
    enableOrders = true,
  ).validated

  private def signal(deviationBps: Double = 8.0, quoteAgeMs: Long = 100) = SpreadDislocation(
    ticker = "AAPL", rich = rich, cheap = cheap,
    spreadBps = 20.0, meanBps = 12.0, sigmaBps = 1.0, deviationBps = deviationBps, z = 8.0,
    crossEdgeBps = 15.0, richMid = 100.5, cheapMid = 100.0,
    samples = 500, quoteAgeMs = quoteAgeMs, timestamp = t0,
  )

  /** 卖腿买一 / 买腿卖一 定出可执行的边。 */
  private def quotes(sellBid: Double, buyAsk: Double) =
    (Some(ArbPlan.LegQuote(rich, sellBid, sellBid + 0.01)), Some(ArbPlan.LegQuote(cheap, buyAsk - 0.01, buyAsk)))

  private def plan(
      sellBid: Double = 100.5,
      buyAsk: Double = 100.0,
      sig: SpreadDislocation = signal(),
      unbalanced: Option[(Coin, Instrument)] = None,
      inFlight: Boolean = false,
      c: ArbPlan.Config = cfg,
  ) =
    val (rq, cq) = quotes(sellBid, buyAsk)
    ArbPlan.plan(c, sig, rq, cq, unbalanced, inFlight, _ => "cid")

  test("边用 bid/ask 算, 不是中价 —— 中价系统性高估两边价差的均值"):
    // 中价差在决策边界上恰好是最要紧的地方: 高估的那一截可能正好就是利润门槛。
    val sell = ArbPlan.LegQuote(rich, bid = 100.48, ask = 100.52) // mid 100.50
    val buy = ArbPlan.LegQuote(cheap, bid = 99.98, ask = 100.02)  // mid 100.00
    val edge = ArbPlan.edgeBps(sell, buy)
    val midEdge = CrossSpreadDetector.logRatioBps(Price(100.50), Price(100.00))
    assert(edge < midEdge, s"可执行的边 $edge 必须小于中价差 $midEdge")
    // 高估量 ≈ 两边价差的均值 (各 4bp -> 约 4bp)
    assertEqualsDouble(midEdge - edge, 4.0, 0.2)

  test("偏离没到门槛 -> 不动手, 且说出是哪一条不满足"):
    // 判据是"偏离中枢"而不是"价差大": 持续存在的价差是结构性的, 照着它开仓等来的是持仓成本。
    val Left(skip) = plan(sig = signal(deviationBps = 3.0)): @unchecked
    assertEquals(skip, ArbPlan.Skip.DeviationTooSmall(3.0, 5.0))
    assert(skip.describe.contains("未到门槛"), skip.describe)

  test("边盖不住来回四腿的成本 -> 不动手"):
    // 只看边不看成本, 会在"看起来有 8bp"的地方反复付掉 10bp 手续费。
    val Left(skip) = plan(sellBid = 100.05, buyAsk = 100.0): @unchecked // 边约 5bp < 10+2
    assert(skip.isInstanceOf[ArbPlan.Skip.EdgeBelowCost], skip.toString)
    assert(skip.describe.contains("盖不住"), skip.describe)

  test("两腿报价不同时刻 -> 不动手 (差出来的是时间不是价差)"):
    val Left(skip) = plan(sig = signal(quoteAgeMs = 30_000)): @unchecked
    assertEquals(skip, ArbPlan.Skip.QuotesTooOld(30_000, 2000))

  test("上一轮还没配平 -> 不在裸敞口上再叠一层"):
    val Left(skip) = plan(unbalanced = Some((Coin(0.5), rich))): @unchecked
    assertEquals(skip, ArbPlan.Skip.Unbalanced(Coin(0.5), rich))

  test("上一轮的 IOC 还在途 -> 等它"):
    assertEquals(plan(inFlight = true), Left(ArbPlan.Skip.InFlight))

  test("enableOrders=false -> 算得出来也不下单, 且日志说明本该下"):
    val Left(skip) = plan(c = cfg.copy(enableOrders = false)): @unchecked
    assert(skip.isInstanceOf[ArbPlan.Skip.OrdersDisabled], skip.toString)
    assert(skip.describe.contains("只记录不下单"), skip.describe)

  test("全部通过 -> 卖贵的买一、买便宜的卖一, 两条都是 IOC 限价"):
    val Right(legs) = plan(): @unchecked
    assertEquals(legs.sell.exchange, Exchange.Binance)
    assertEquals(legs.sell.side, Side.Short)
    assertEquals(legs.sell.orderType, OrderType.Limit(Price(100.5), TimeInForce.IOC))
    assertEquals(legs.buy.exchange, Exchange.Okx)
    assertEquals(legs.buy.side, Side.Long)
    assertEquals(legs.buy.orderType, OrderType.Limit(Price(100.0), TimeInForce.IOC))
    assert(legs.orders.forall(!_.reduceOnly), "开仓不是 reduceOnly")

  test("IOC 而不是市价 —— 限价把最差成交价钉死"):
    // 这笔交易全部利润只有几个 bp, 市价在薄盘上一次穿档就连本带利吃掉。
    val Right(legs) = plan(): @unchecked
    assert(legs.orders.forall(_.orderType match
      case OrderType.Limit(_, TimeInForce.IOC) => true
      case _                                   => false
    ))

  test("config: 危险方向的字段没有默认值, 且 minProfitBps=0 即拒"):
    // 0 意味着"刚好打平也做" —— 那是白担腿风险。
    intercept[IllegalArgumentException](cfg.copy(minProfitBps = 0.0).validated)
    intercept[IllegalArgumentException](cfg.copy(minDeviationBps = 0.0).validated)
    intercept[IllegalArgumentException](cfg.copy(qtyPerLeg = Coin.Zero).validated)
    assertEquals(cfg.requiredEdgeBps, 12.0, "门槛 = 来回成本 + 利润下限")

  // ==================== 对账: 腿不平是常态, 不是异常 ====================

  private def reconcile(sellFilled: Double, buyFilled: Double) =
    ArbPlan.reconcile(rich, Coin(sellFilled), cheap, Coin(buyFilled), tolerance = Coin(0.001))

  test("两腿都成 -> 配平"):
    assertEquals(reconcile(1.0, 1.0), ArbPlan.Outcome.Balanced(Coin(1.0)))

  test("两腿都没成 -> 没有敞口, 什么都不用做"):
    assertEquals(reconcile(0.0, 0.0), ArbPlan.Outcome.Missed)

  test("卖腿成得多 -> 净空, 买回多出的部分"):
    // 两条腿是两次独立的 REST 往返, 中间对手盘会动 —— IOC 一条全成一条部分成是常态。
    assertEquals(reconcile(1.0, 0.4), ArbPlan.Outcome.Naked(Coin(0.6), rich, Side.Long))

  test("买腿成得多 -> 净多, 卖掉多出的部分"):
    assertEquals(reconcile(0.3, 1.0), ArbPlan.Outcome.Naked(Coin(0.7), cheap, Side.Short))

  test("只成一条腿 -> 整条都是裸的"):
    assertEquals(reconcile(1.0, 0.0), ArbPlan.Outcome.Naked(Coin(1.0), rich, Side.Long))

  test("残量在容差内视为配平 —— 比最小步长还小的量在交易所也发不出去"):
    // 追着平会陷入"发不出去 -> 还是不平 -> 再发"的循环。
    assertEquals(reconcile(1.0, 0.9995), ArbPlan.Outcome.Balanced(Coin(0.9995)))

  test("成交量为负 / 容差非正即抛 —— 那不是一次可用的对账输入"):
    intercept[IllegalArgumentException](ArbPlan.reconcile(rich, Coin(-1.0), cheap, Coin(1.0), Coin(0.001)))
    intercept[IllegalArgumentException](ArbPlan.reconcile(rich, Coin(1.0), cheap, Coin(1.0), Coin.Zero))
