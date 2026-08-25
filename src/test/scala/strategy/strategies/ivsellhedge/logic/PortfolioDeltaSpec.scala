package strategy.strategies.ivsellhedge.logic

import strategy.utils.option.{OptionHolding, OptionInstrument, OptionMark, OptionRight}

import hft.option.BlackScholes

/** PortfolioDelta 单测：三份数据的内连接、缺 IV/缺链的显式暴露、ctVal 与符号的量纲、到期退化。 */
class PortfolioDeltaSpec extends munit.FunSuite:
  /** 连接后取可定价的腿 (多数用例只关心这一半) */
  private def rs(h: Seq[OptionHolding], c: Seq[OptionInstrument], m: Seq[OptionMark]) =
    PortfolioDelta.resolve(h, c, m).legs

  private val now = 1_700_000_000_000L
  private val day = 86_400_000L
  private val expiry = now + 30 * day

  private def inst(sym: String, k: Double, right: OptionRight, ctVal: Double = 0.1) =
    OptionInstrument(sym, expiry, k, right, ctVal = ctVal, minQty = 1, qtyStep = 1, tickSize = 0.0001)

  private val call = inst("C3000", 3000, OptionRight.Call)
  private val put = inst("P3000", 3000, OptionRight.Put)
  private val chain = Seq(call, put)
  private val marks = Seq(OptionMark("C3000", 0.6), OptionMark("P3000", 0.6))

  test("内连接: 持仓 × 期权链 × 标记 IV, 三者齐备才成腿"):
    val legs = PortfolioDelta.resolve(Seq(OptionHolding("C3000", -10)), chain, marks).legs
    assertEquals(legs.size, 1)
    assertEquals(legs.head.contracts, -10.0)
    assertEquals(legs.head.markVol, 0.6)

  test("零张持仓丢掉 (对敞口无贡献, 留着只会让缺 IV 的告警噪声化)"):
    assertEquals(PortfolioDelta.resolve(Seq(OptionHolding("C3000", 0)), chain, marks).legs, Vector.empty)
    assertEquals(PortfolioDelta.resolve(Seq(OptionHolding("C3000", 0)), chain, Nil).unpriceable, Vector.empty)

  test("缺标记 IV 的持仓被显式报出, 不按 0 计入 (半个敞口比没有敞口更危险)"):
    val holdings = Seq(OptionHolding("C3000", -10), OptionHolding("P3000", -10))
    assertEquals(PortfolioDelta.resolve(holdings, chain, Seq(OptionMark("C3000", 0.6))).legs.size, 1)
    assertEquals(PortfolioDelta.resolve(holdings, chain, Seq(OptionMark("C3000", 0.6))).unpriceable, Vector("P3000"))

  test("不在期权链里的持仓同样被报出 (算不出 ctVal/行权价)"):
    assertEquals(PortfolioDelta.resolve(Seq(OptionHolding("GHOST", -1)), chain, marks).unpriceable, Vector("GHOST"))

  test("空头 call 的 delta 为负, 空头 put 为正 (方向)"):
    val (dc, _) = PortfolioDelta.greeks(rs(Seq(OptionHolding("C3000", -10)), chain, marks), 3000, now)
    val (dp, _) = PortfolioDelta.greeks(rs(Seq(OptionHolding("P3000", -10)), chain, marks), 3000, now)
    assert(dc.value < 0, s"空头 call delta 应为负, 实为 ${dc.value}")
    assert(dp.value > 0, s"空头 put delta 应为正, 实为 ${dp.value}")

  test("空头跨式在平值附近近似 delta 中性, gamma 为负"):
    val holdings = Seq(OptionHolding("C3000", -10), OptionHolding("P3000", -10))
    val (d, g) = PortfolioDelta.greeks(rs(holdings, chain, marks), 3000, now)
    assert(math.abs(d.value) < 0.2, s"平值空头跨式 delta 应接近 0, 实为 ${d.value}")
    assert(g.value < 0, s"空头期权 gamma 应为负, 实为 ${g.value}")

  test("delta 随 ctVal 与张数线性缩放 (量纲: 张 × ctVal × BS delta)"):
    val one = PortfolioDelta.greeks(rs(Seq(OptionHolding("C3000", -1)), chain, marks), 3000, now)._1
    val ten = PortfolioDelta.greeks(rs(Seq(OptionHolding("C3000", -10)), chain, marks), 3000, now)._1
    assert(math.abs(ten.value - one.value * 10) < 1e-12, s"应线性: ${one.value} vs ${ten.value}")
    // ctVal 翻 10 倍 -> delta 翻 10 倍
    val fat = Seq(inst("C3000", 3000, OptionRight.Call, ctVal = 1.0))
    val fatD = PortfolioDelta.greeks(rs(Seq(OptionHolding("C3000", -1)), fat, marks), 3000, now)._1
    assert(math.abs(fatD.value - one.value * 10) < 1e-12, s"ctVal 应线性: ${one.value} vs ${fatD.value}")

  test("与 BlackScholes 逐腿对账 (delta 就是 Σ 张×ctVal×BS delta, 无额外修正)"):
    val holdings = Seq(OptionHolding("C3000", -7), OptionHolding("P3000", -3))
    val spot = 3200.0
    val (d, _) = PortfolioDelta.greeks(rs(holdings, chain, marks), spot, now)
    val t = (expiry - now).toDouble / BlackScholes.MillisPerYear
    val expected =
      -7 * 0.1 * BlackScholes.greeks(OptionRight.Call, spot, 3000, t, 0.6, 0.0).delta +
        -3 * 0.1 * BlackScholes.greeks(OptionRight.Put, spot, 3000, t, 0.6, 0.0).delta
    assert(math.abs(d.value - expected) < 1e-12, s"expected $expected got ${d.value}")

  test("已到期的腿退化为内在价值的敞口 (gamma=0), 不抛错"):
    val past = Seq(inst("C3000", 3000, OptionRight.Call).copy(expiryMs = now - day))
    val (d, g) = PortfolioDelta.greeks(rs(Seq(OptionHolding("C3000", -10)), past, marks), 3500, now)
    assertEquals(g.value, 0.0)
    assert(math.abs(d.value - (-10 * 0.1 * 1.0)) < 1e-12, s"实值到期 call delta=±1, 实为 ${d.value}")

  test("无持仓 -> (0,0)"):
    assertEquals(PortfolioDelta.greeks(Vector.empty, 3000, now), (hft.domain.Coin(0.0), hft.domain.Coin(0.0)))
