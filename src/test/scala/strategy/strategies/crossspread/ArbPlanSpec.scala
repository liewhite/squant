package strategy.strategies.crossspread

import hft.domain.*
import hft.TestUnits.given
import strategy.strategies.crossspread.logic.*

/** 对敲决策与对账的契约。两层判据 (偏离 + 可执行的边)、成本门槛、以及**腿不平**的处理。 */
class ArbPlanSpec extends munit.FunSuite:
  private val rich = Instrument.perp(Exchange.Binance, "AAPLUSDT")
  private val cheap = Instrument.perp(Exchange.Okx, "AAPL")
  private val t0 = 1_700_000_000_000L

  private val cfg = ArbPlan.Config(
    minDeviationBps = 5.0,
    roundTripCostBps = 10.0,
    minProfitBps = 2.0,
    qtyPerLeg = Coin(1.0),
    maxQuoteAgeMs = 2000,
    maxPositionPerLeg = Coin(5.0),
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

  private val minOrder: Instrument => Coin = _ => Coin(0.001)

  private def plan(
      sellBid: Double = 100.5,
      buyAsk: Double = 100.0,
      sig: SpreadDislocation = signal(),
      richPos: Coin = Coin.Zero,
      cheapPos: Coin = Coin.Zero,
      inFlight: Boolean = false,
      c: ArbPlan.Config = cfg,
      declared: Set[Instrument] = Set(rich, cheap),
  ) =
    val (rq, cq) = quotes(sellBid, buyAsk)
    val pos: Instrument => Coin = i => if i == rich then richPos else cheapPos
    ArbPlan.plan(c, sig, declared, rq, cq, pos, inFlight, minOrder)

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

  test("上一轮还没配平 -> 不在裸敞口上再叠一层 (配平由**仓位**派生, 不是内存里另记一本)"):
    // 内存那本账重启就没了, 而裸仓位还在交易所 —— 于是重启后会在裸敞口上继续开新仓。
    val Left(skip) = plan(richPos = Coin(-1.0), cheapPos = Coin(0.4)): @unchecked
    assert(skip.isInstanceOf[ArbPlan.Skip.Unbalanced], skip.toString)

  test("信号里的腿不属于本实例 -> 拒绝, 不在未声明的标的上下单"):
    // 检测器按 ticker 的全部两两组合出信号, 三家所就有三对。在没声明的标的上下单会拿不到回报
    // (own 的路由键只含已声明标的), 也没做过启动对齐。
    val Left(skip) = plan(declared = Set(cheap)): @unchecked
    assertEquals(skip, ArbPlan.Skip.ForeignLeg(rich))

  test("到单腿仓位上限 -> 不再加仓"):
    // 本策略只开仓不平仓, 而检测器冷却默认 60s —— 没有上限的话一次持续偏离能开出几十份。
    val Left(skip) = plan(richPos = Coin(-4.5), cheapPos = Coin(4.5), c = cfg.copy(maxPositionPerLeg = Coin(5.0))): @unchecked
    assert(skip.isInstanceOf[ArbPlan.Skip.AtPositionLimit], skip.toString)
    // 装得下就放行
    assert(plan(richPos = Coin(-1.0), cheapPos = Coin(1.0)).isRight)

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
    // clientOrderId 必须留空: StrategyRunner 会统一覆写。自己生成并拿它关联回报的后果是
    // 一条回报都匹配不上, 策略永久卡在"上一轮还在途"。
    assert(legs.orders.forall(_.clientOrderId.isEmpty), "clientOrderId 由框架分配, 策略留空")

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
    intercept[IllegalArgumentException](cfg.copy(maxPositionPerLeg = Coin(0.5)).validated) // 装不下一份
    assertEquals(cfg.requiredEdgeBps, 12.0, "门槛 = 来回成本 + 利润下限")

  // ==================== 裸敞口: 由仓位派生 ====================

  private def naked(richPos: Double, cheapPos: Double, minOrderSize: Double = 0.001) =
    ArbPlan.netExposure(rich, Coin(richPos), cheap, Coin(cheapPos), _ => Coin(minOrderSize))

  test("短 rich 与长 cheap 相互抵消 -> 配平"):
    assertEquals(naked(-1.0, 1.0), None)

  test("两腿都空 -> 配平 (没有仓位就没有敞口)"):
    assertEquals(naked(0.0, 0.0), None)

  test("卖腿成得多 -> 净空, 在 rich 上买回多出的部分"):
    // 两条腿是两次独立的 REST 往返, 中间对手盘会动 —— IOC 一条全成一条部分成是常态。
    assertEquals(naked(-1.0, 0.4), Some(ArbPlan.Naked(Coin(0.6), rich, Side.Long)))

  test("买腿成得多 -> 净多, 在 cheap 上卖掉多出的部分"):
    assertEquals(naked(-0.3, 1.0), Some(ArbPlan.Naked(Coin(0.7), cheap, Side.Short)))

  test("只成一条腿 -> 整条都是裸的"):
    assertEquals(naked(-1.0, 0.0), Some(ArbPlan.Naked(Coin(1.0), rich, Side.Long)))

  test("平的是**绝对仓位大的**那条 —— 平小的那条是在加敞口"):
    assertEquals(naked(-2.0, 0.5).map(_.leg), Some(rich))
    assertEquals(naked(-0.5, 2.0).map(_.leg), Some(cheap))

  test("残量小于**要平那条腿的最小可发量**时视为配平"):
    // 判据不是两腿步长的较小者 —— 那个方向是反的: 残量落在两者之间时会判成未配平,
    // 而平腿单随即被交易所按精度拒, 策略就卡死在那里。
    assertEquals(naked(-1.0, 0.9995, minOrderSize = 0.001), None)
    assert(naked(-1.0, 0.9985, minOrderSize = 0.001).isDefined, "超过最小可发量就该平")

  test("最小可发量非正即抛 —— 那不是一次可用的判据"):
    intercept[IllegalArgumentException](ArbPlan.netExposure(rich, Coin(1.0), cheap, Coin.Zero, _ => Coin.Zero))

  // ==================== 下出去的单必须属于自己声明的标的 ====================

  test("非永续的腿: 单带着腿的品种, 不是默认的 LinearPerp"):
    // 从前两个构造点把 Instrument 拆成 exchange + symbol 填进 Order, 第三维落在默认值上。
    // 症状不是"下错端点"这么直白: order.instrument 与声明的标的对不上, 挂单登记时
    // StateManager 以 "Instrument not found" 抛出 —— 而策略明明只交易自己声明的两条腿。
    val inverseRich = Instrument(Exchange.Binance, "AAPLUSDT", InstrumentKind.InversePerp)
    val optionCheap = Instrument.option(Exchange.Okx, "AAPL-250101-100-C")
    val sig = signal().copy(rich = inverseRich, cheap = optionCheap)
    val rq = Some(ArbPlan.LegQuote(inverseRich, 100.5, 100.51))
    val cq = Some(ArbPlan.LegQuote(optionCheap, 99.99, 100.0))
    val pos: Instrument => Coin = _ => Coin.Zero
    val Right(legs) = ArbPlan.plan(cfg, sig, Set(inverseRich, optionCheap), rq, cq, pos, false, minOrder): @unchecked

    assertEquals(legs.orders.map(_.instrument).toSet, Set(inverseRich, optionCheap))

  test("平腿单同样带着那条腿的品种"):
    val optionLeg = Instrument.option(Exchange.Okx, "AAPL-250101-100-C")
    val naked = ArbPlan.Naked(leg = optionLeg, closeSide = Side.Short, excess = Coin(1.0))
    assertEquals(ArbPlan.unwindOrder(naked).instrument, optionLeg)
