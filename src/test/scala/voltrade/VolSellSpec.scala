package voltrade

import scala.collection.mutable

/** VolSell 编排单测 (fake 交易所): 倍数→数量、两腿幂等 link、缺腿/数量不合规/数据不足→整体 Left (不裸下单)。 */
class VolSellSpec extends munit.FunSuite:
  private val d = 86_400_000L
  private val now = 1_700_000_000_000L

  /** 注: closes 后半更波动 -> RV 升 -> mult=gridHigh */
  private val flat = Vector.fill(10)(3000.0)
  private val choppy = (0 until 10).map(i => if i % 2 == 0 then 3000.0 else 3060.0).toVector
  private val rvUpCloses = flat ++ choppy // 本周(后半)更波动

  private def inst(sym: String, strike: Double, right: OptionRight, min: Double = 0.1, step: Double = 0.1) =
    OptionInstrument(sym, now + 21 * d, strike, right, minQty = min, qtyStep = step, tickSize = 0.1) // 到期 ~21天后

  private val chain = Vector(
    inst("ETH-B-3000-C-USDT", 3000, OptionRight.Call),
    inst("ETH-B-3000-P-USDT", 3000, OptionRight.Put),
  )

  private final class FakeEx(
      closes: Vector[Double] = rvUpCloses,
      chainV: Vector[OptionInstrument] = chain,
      asks: Map[String, Double] = Map("ETH-B-3000-C-USDT" -> 50.0, "ETH-B-3000-P-USDT" -> 40.0),
      failLeg: Set[String] = Set.empty,
  ) extends OptionsExchange:
    val placed: mutable.Buffer[(String, Double, Option[Double], String)] = mutable.Buffer.empty
    def underlyingCloses5m(symbol: String, bars: Int) = Right(closes)
    def underlyingSpot(symbol: String) = Right(closes.last)
    def optionChain(baseCoin: String) = Right(chainV)
    def optionBestAsk(symbol: String) = Right(asks.get(symbol))
    def sellOption(symbol: String, qty: Double, limitPrice: Option[Double], orderLinkId: String) =
      placed += ((symbol, qty, limitPrice, orderLinkId))
      if failLeg(symbol) then Left("simulated fail") else Right(s"oid-$orderLinkId")
    def optionAccountDelta() = Right(0.0)
    def linearKlines(symbol: String, interval: String, bars: Int) = Right(Vector.empty)

  private val cfg = VolSell.Config(symbol = "ETHUSDT", baseCoin = "ETH", targetDays = 21, gridHigh = 2.0, gridLow = 1.0, baseQty = 1.0, bars2w = 20)

  test("RV 升 -> 卖 2×, 两腿 call+put, PostOnly 价=ask, link 幂等且区分 c/p"):
    val ex = FakeEx()
    val res = VolSell.plan(ex, cfg, now)
    assert(res.isRight, res.toString)
    val Right(dec) = res: @unchecked
    assertEquals(dec.mult, 2.0)
    assertEquals(dec.legs.size, 2)
    assertEquals(dec.legs.map(_.qty).toSet, Set(2.0)) // baseQty 1 × 2
    val byRight = dec.legs.map(l => l.right -> l).toMap
    assertEquals(byRight(OptionRight.Call).price, 50.0)
    assertEquals(byRight(OptionRight.Put).price, 40.0)
    val period = SellVolPlan.currentDecisionTime(now)
    assertEquals(byRight(OptionRight.Call).orderLinkId, s"vs-$period-c")
    assertEquals(byRight(OptionRight.Put).orderLinkId, s"vs-$period-p")
    // 幂等: 同一周期 plan 两次得相同 link
    assertEquals(VolSell.plan(FakeEx(), cfg, now + 1000).toOption.get.legs.map(_.orderLinkId).toSet, dec.legs.map(_.orderLinkId).toSet)

  test("RV 降 -> 卖 1×"):
    assertEquals(VolSell.plan(FakeEx(choppy ++ flat), cfg, now).toOption.get.mult, 1.0)

  test("缺 put -> 整体 Left (绝不只下 call 裸腿)"):
    assert(VolSell.plan(FakeEx(chainV = chain.filter(_.right == OptionRight.Call)), cfg, now).isLeft)

  test("数量低于最小量 -> 整体 Left"):
    val bigMin = chain.map(_.copy(minQty = 100.0))
    assert(VolSell.plan(FakeEx(chainV = bigMin), cfg, now).isLeft)

  test("无卖一报价 -> Left (不市价砸盘)"):
    assert(VolSell.plan(FakeEx(asks = Map.empty), cfg, now).isLeft)

  test("K线不足 (<90%) -> Left"):
    assert(VolSell.plan(FakeEx(closes = Vector.fill(5)(3000.0)), cfg, now).isLeft)

  test("execute: 两腿都下单"):
    val ex = FakeEx()
    val dec = VolSell.plan(ex, cfg, now).toOption.get
    val results = VolSell.execute(ex, dec)
    assertEquals(results.size, 2)
    assert(results.forall(_._2.isRight))
    assertEquals(ex.placed.size, 2)
    assertEquals(ex.placed.map(_._1).toSet, Set("ETH-B-3000-C-USDT", "ETH-B-3000-P-USDT"))
