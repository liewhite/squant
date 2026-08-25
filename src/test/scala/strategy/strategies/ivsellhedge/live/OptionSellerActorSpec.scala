package strategy.strategies.ivsellhedge.live

import strategy.strategies.ivsellhedge.logic.SellPlan
import strategy.utils.option.*

import hft.domain.Exchange

import scala.collection.mutable

/** 卖出对账的**编排**单测：用 fake 交易所驱动 [[OptionSellerActor.reconcileOnce]]，
  * 覆盖两腿齐下、杠杆闸门短路、enableOpen 开关、缺 IV 不猜、取数失败不静默。 */
class OptionSellerActorSpec extends munit.FunSuite:

  private val day = 86_400_000L
  private val expiry3d = System.currentTimeMillis + 3 * day
  private val expiry10d = System.currentTimeMillis + 10 * day

  private def inst(sym: String, exp: Long, k: Double, right: OptionRight) =
    OptionInstrument(sym, exp, k, right, ctVal = 0.1, minQty = 1, qtyStep = 1, tickSize = 0.0001)

  /** 3 天与 10 天两个到期, 每档若干行权价 */
  private val chain: Vector[OptionInstrument] =
    (for
      (exp, tag) <- Vector((expiry3d, "A"), (expiry10d, "B"))
      k <- Vector(2800.0, 2900.0, 3050.0, 3100.0, 3200.0)
      r <- Vector(OptionRight.Call, OptionRight.Put)
    yield inst(s"ETH-USD-$tag-$k-${if r == OptionRight.Call then "C" else "P"}", exp, k, r)).toVector

  /** 一个可编排的假交易所：记录下过的单，其余读数由构造参数给定。 */
  private final class Fake(
      marks: Vector[OptionMark] = Vector.empty,
      holdings: Vector[OptionHolding] = Vector.empty,
      equity: Double = 100_000.0,
      coinBalance: Double = 0.0,
      spot: Double = 3000.0,
      quote: Quote = Quote(0.05, 0.052),
      chainOverride: Option[Vector[OptionInstrument]] = None,
      failWith: Option[String] = None,
      rejectOrders: Boolean = false,
  ) extends OptionsExchange
      with OptionAccountData:
    val orders: mutable.ArrayBuffer[(String, Double, Double, Boolean)] = mutable.ArrayBuffer.empty
    private def guard[A](a: A): Either[String, A] = failWith.toLeft(a)

    def optionChain(baseCoin: String) = guard(chainOverride.getOrElse(chain))
    def optionMarks(baseCoin: String) = guard(marks)
    def optionPositions(baseCoin: String) = guard(holdings)
    def underlyingLast(symbol: String) = guard(spot)
    def accountCash(ccy: String) = guard(OptionAccountCash(equity, coinBalance))
    def optionQuote(symbol: String) = guard(Some(quote))
    def sellOption(symbol: String, qty: Double, price: Double, postOnly: Boolean, orderLinkId: String) =
      if rejectOrders then Left("被拒")
      else { orders += ((symbol, qty, price, postOnly)); Right("ord-1") }
    // 本策略不用的读法
    def underlyingCloses5m(symbol: String, bars: Int) = guard(Vector.empty)
    def underlyingSpot(symbol: String) = guard(spot)
    def optionAccountGreeks() = guard((0.0, 0.0))
    def linearKlines(symbol: String, interval: String, bars: Int) = guard(Vector.empty)

  private def cfg(enableOpen: Boolean = true, maxLev: Double = 1.0) =
    OptionSellerActor
      .Config(symbol = "ETH", baseCoin = "ETH", ccy = "ETH",
        targetDays = 3, minTtlMs = day, minStrikeDistance = 0.02,
        ivQty = SellPlan.IvQty(ivStart = 0.2, qtyStart = 10, qtySlope = 1, qtyMax = 50),
        maxOptionLeverage = maxLev, enableOpen = enableOpen)
      .validated

  /** 3 天档、距离 2% 之外最近的两腿: call=3100, put=2900 */
  private val callSym = "ETH-USD-A-3100.0-C"
  private val putSym = "ETH-USD-A-2900.0-P"
  private def marksFor(iv: Double) = Vector(OptionMark(callSym, iv), OptionMark(putSym, iv))

  /** 默认 lastSubmitAt=0 表示"从未提交过", 不受结算静默期影响 */
  private def run(f: Fake, c: OptionSellerActor.Config = cfg(), lastSubmitAt: Long = 0L) =
    OptionSellerActor(f, Exchange.Okx, c).reconcileOnce(lastSubmitAt)

  test("两腿都按 IV 定量卖出, 走 IOC (postOnly=false), 价格取 bid"):
    val f = Fake(marks = marksFor(0.30))
    assert(run(f).exists(_.nonEmpty), "提交过卖单 -> 返回提交时刻 (用于进入结算静默期)")
    assertEquals(f.orders.map(_._1).toSet, Set(callSym, putSym))
    assertEquals(f.orders.map(_._2).toSet, Set(20.0)) // IV=30% -> 10 + 10 个点 = 20 张
    assertEquals(f.orders.map(_._3).toSet, Set(0.05)) // bid
    assertEquals(f.orders.map(_._4).toSet, Set(false), "必须是 IOC/taker, 不留挂单")

  test("已持空头张数从目标里扣掉 (声明式对账)"):
    val f = Fake(marks = marksFor(0.30), holdings = Vector(OptionHolding(callSym, -15), OptionHolding(putSym, -20)))
    run(f)
    assertEquals(f.orders.map(t => (t._1, t._2)).toSet, Set((callSym, 5.0)), "call 补 5 张, put 已达目标不动")

  test("IV 低于起卖点 -> 一张不卖"):
    val f = Fake(marks = marksFor(0.10))
    assertEquals(run(f), Right(None))
    assertEquals(f.orders.toVector, Vector.empty)

  test("杠杆闸门达上限 -> 完全不卖 (对冲不受影响, 由另一条腿负责)"):
    // Σ|张|=200, ctVal=0.1, spot=3000 -> 名义 60000; 净值 60000 -> 杠杆 1.0
    val f = Fake(marks = marksFor(0.50), holdings = Vector(OptionHolding(callSym, -200)), equity = 60_000.0)
    assertEquals(run(f), Right(None))
    assertEquals(f.orders.toVector, Vector.empty)

  test("净值 <= 0 -> 杠杆 +∞ -> 不卖"):
    val f = Fake(marks = marksFor(0.50), equity = 0.0)
    run(f)
    assertEquals(f.orders.toVector, Vector.empty)

  test("enableOpen=false -> 只算意图不下单"):
    val f = Fake(marks = marksFor(0.30))
    assertEquals(run(f, cfg(enableOpen = false)), Right(None), "没提交 -> 不进入结算静默期")
    assertEquals(f.orders.toVector, Vector.empty)

  test("缺标记 IV 的腿不卖, 也不回退到某个基础张数 (IV 是定量的唯一依据)"):
    val f = Fake(marks = Vector(OptionMark(callSym, 0.30))) // put 无 IV
    run(f)
    assertEquals(f.orders.map(_._1).toSet, Set(callSym))

  test("盘口点差过宽 -> 两腿都不卖"):
    val f = Fake(marks = marksFor(0.30), quote = Quote(0.05, 0.20))
    run(f)
    assertEquals(f.orders.toVector, Vector.empty)

  test("现价超出行权范围 -> 该侧无腿, 只卖得出的那一侧 (单腿裸卖, 已告警)"):
    // spot=3150: call 需 >= 3213 (最高挂牌 3200, 无); put 需 <= 3087 -> 取 3050
    val put3050 = "ETH-USD-A-3050.0-P"
    val f = Fake(marks = Vector(OptionMark(put3050, 0.30)), spot = 3150.0)
    run(f)
    assertEquals(f.orders.map(_._1).toSet, Set(put3050))

  test("无满足最小剩余期限的到期 -> Left, 不下单 (不静默)"):
    val soon = chain.map(_.copy(expiryMs = System.currentTimeMillis + 3600_000L)) // 1 小时后到期
    val f = Fake(marks = marksFor(0.30), chainOverride = Some(soon))
    assert(run(f).isLeft)
    assertEquals(f.orders.toVector, Vector.empty)

  test("取数失败 -> Left, 不下单 (错误向上传播, 不吞)"):
    val f = Fake(marks = marksFor(0.30), failWith = Some("网络错误"))
    assertEquals(run(f), Left("网络错误"))
    assertEquals(f.orders.toVector, Vector.empty)

  test("只选目标到期那一档的腿 (不跨到期拼宽跨)"):
    val f = Fake(marks = Vector(
      OptionMark(callSym, 0.30), OptionMark(putSym, 0.30),
      OptionMark("ETH-USD-B-3100.0-C", 0.9), OptionMark("ETH-USD-B-2900.0-P", 0.9), // 10 天档 IV 更高
    ))
    run(f)
    assertEquals(f.orders.map(_._1).toSet, Set(callSym, putSym), "targetDays=3 -> 只卖 3 天档")

  // ---------- 结算静默期：防同一缺口被连卖两轮 ----------

  test("刚提交过 -> 本轮完全不对账 (等持仓落地)"):
    val f = Fake(marks = marksFor(0.30))
    assertEquals(run(f, lastSubmitAt = System.currentTimeMillis), Right(None))
    assertEquals(f.orders.toVector, Vector.empty, "静默期内一个请求都不该发")

  test("静默期已过 -> 正常对账"):
    val f = Fake(marks = marksFor(0.30))
    // cfg 的 sellIntervalMs 默认 5000, settleRounds 默认 1 -> 静默期 5s
    assert(run(f, lastSubmitAt = System.currentTimeMillis - 6000).exists(_.nonEmpty))
    assertEquals(f.orders.size, 2)

  test("下单被拒不进入静默期 (那张单没成立, 下轮该重试)"):
    assertEquals(run(Fake(marks = marksFor(0.30), rejectOrders = true)), Right(None))

  test("持仓配不上期权链 -> 杠杆被低估, 本轮不卖 (与敞口侧同一道防线)"):
    val f = Fake(marks = marksFor(0.30), holdings = Vector(OptionHolding("GHOST-INST", -50)))
    assertEquals(run(f), Right(None))
    assertEquals(f.orders.toVector, Vector.empty)
