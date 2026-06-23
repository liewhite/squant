package strategy.volsell.logic
import strategy.utils.option.*

import scala.collection.mutable

/** VolSell 编排单测 (fake 交易所): 倍数→数量、宽跨选腿 (贴近现价两侧)、按价差定 taker/maker 卖价、
  * 两腿幂等 link、缺腿/数量不合规/数据不足→整体 Left (不裸下单)。 */
class VolSellSpec extends munit.FunSuite:
  private val d = 86_400_000L
  private val now = 1_700_000_000_000L

  /** closes 后半更波动 -> RV 升 -> mult=gridHigh; 末值=3060 -> spot=3060 */
  private val flat = Vector.fill(10)(3000.0)
  private val choppy = (0 until 10).map(i => if i % 2 == 0 then 3000.0 else 3060.0).toVector
  private val rvUpCloses = flat ++ choppy // spot = 3060

  private def inst(sym: String, strike: Double, right: OptionRight, min: Double = 0.1, step: Double = 0.1) =
    OptionInstrument(sym, now + 21 * d, strike, right, minQty = min, qtyStep = step, tickSize = 0.1) // 到期 ~21天后

  // spot=3060: 最近宽跨 = call 3100 (>spot 最小) + put 3000 (<spot 最大); 另含更远腿验证"最近"
  private val chain = Vector(
    inst("ETH-3100-C", 3100, OptionRight.Call),
    inst("ETH-3200-C", 3200, OptionRight.Call),
    inst("ETH-3000-P", 3000, OptionRight.Put),
    inst("ETH-2900-P", 2900, OptionRight.Put),
  )

  // call 价差 0.1 (<=0.3 -> taker@bid=49.9); put 价差 1.0 (>0.3 -> maker@mid-0.2=39.3)
  private val quotes = Map(
    "ETH-3100-C" -> Quote(49.9, 50.0),
    "ETH-3000-P" -> Quote(39.0, 40.0),
    "ETH-2900-P" -> Quote(28.0, 30.0), // RV 降测试 spot=3000 -> 选 put 2900
  )

  private final class FakeEx(
      closes: Vector[Double] = rvUpCloses,
      chainV: Vector[OptionInstrument] = chain,
      quoteV: Map[String, Quote] = quotes,
      failLeg: Set[String] = Set.empty,
  ) extends OptionsExchange:
    val placed: mutable.Buffer[(String, Double, Double, Boolean, String)] = mutable.Buffer.empty
    def underlyingCloses5m(symbol: String, bars: Int) = Right(closes)
    def underlyingSpot(symbol: String) = Right(closes.last)
    def optionChain(baseCoin: String) = Right(chainV)
    def optionQuote(symbol: String) = Right(quoteV.get(symbol))
    def sellOption(symbol: String, qty: Double, price: Double, postOnly: Boolean, orderLinkId: String) =
      placed += ((symbol, qty, price, postOnly, orderLinkId))
      if failLeg(symbol) then Left("simulated fail") else Right(s"oid-$orderLinkId")
    def optionAccountGreeks() = Right((0.0, 0.0))
    def linearKlines(symbol: String, interval: String, bars: Int) = Right(Vector.empty)

  private val cfg = VolSell.Config(symbol = "ETHUSDT", baseCoin = "ETH", targetDays = 21, gridHigh = 2.0, gridLow = 1.0, baseQty = 1.0, bars2w = 20)

  test("RV 升 -> 卖 2×, 宽跨 call3100+put3000; 价差≤0.3 取对手价(taker), >0.3 取中价-0.2(maker)"):
    val ex = FakeEx()
    val res = VolSell.plan(ex, cfg, now)
    assert(res.isRight, res.toString)
    val Right(dec) = res: @unchecked
    assertEquals(dec.mult, 2.0)
    assertEquals((dec.callStrike, dec.putStrike), (3100.0, 3000.0)) // 贴近 spot=3060 两侧
    assertEquals(dec.legs.map(_.qty).toSet, Set(2.0)) // baseQty 1 × 2
    val byRight = dec.legs.map(l => l.right -> l).toMap
    assertEquals((byRight(OptionRight.Call).price, byRight(OptionRight.Call).postOnly), (49.9, false)) // 价差0.1 -> 对手价 taker
    assertEquals((byRight(OptionRight.Put).price, byRight(OptionRight.Put).postOnly), (39.3, true))    // 价差1.0 -> 中价39.5-0.2 maker
    val period = SellVolPlan.currentDecisionTime(now)
    assertEquals(byRight(OptionRight.Call).orderLinkId, s"vs-$period-c")
    assertEquals(byRight(OptionRight.Put).orderLinkId, s"vs-$period-p")

  test("runNow=true -> 决策锚点用上周五 (orderLinkId 据此派生)"):
    val dec = VolSell.plan(FakeEx(), cfg, now, runNow = true).toOption.get
    val anchor = SellVolPlan.lastDecisionTime(now)
    assertEquals(dec.legs.map(_.orderLinkId).toSet, Set(s"vs-$anchor-c", s"vs-$anchor-p"))

  test("RV 降 -> 卖 1×"):
    assertEquals(VolSell.plan(FakeEx(choppy ++ flat), cfg, now).toOption.get.mult, 1.0)

  test("缺 put 侧价外腿 -> 整体 Left (绝不只下 call 裸腿)"):
    assert(VolSell.plan(FakeEx(chainV = chain.filter(_.right == OptionRight.Call)), cfg, now).isLeft)

  test("数量低于最小量 -> 整体 Left"):
    assert(VolSell.plan(FakeEx(chainV = chain.map(_.copy(minQty = 100.0))), cfg, now).isLeft)

  test("数量超 maxQty 硬上限 -> 整体 Left (防 scale bug 误下巨单)"):
    assert(VolSell.plan(FakeEx(), cfg.copy(maxQty = 1.5), now).isLeft) // qty=2 > 1.5
    assert(VolSell.plan(FakeEx(), cfg.copy(maxQty = 2.0), now).isRight) // 恰好不超

  test("无盘口报价 -> Left (无法定价)"):
    assert(VolSell.plan(FakeEx(quoteV = Map.empty), cfg, now).isLeft)

  test("K线不足 (<90%) -> Left"):
    assert(VolSell.plan(FakeEx(closes = Vector.fill(5)(3000.0)), cfg, now).isLeft)

  test("execute: 两腿都下单 (带 price/postOnly)"):
    val ex = FakeEx()
    val dec = VolSell.plan(ex, cfg, now).toOption.get
    val results = VolSell.execute(ex, dec)
    assertEquals(results.size, 2)
    assert(results.forall(_._2.isRight))
    assertEquals(ex.placed.map(p => (p._1, p._4)).toSet, Set(("ETH-3100-C", false), ("ETH-3000-P", true)))
