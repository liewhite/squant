package strategy.strategies.ivsellhedge.logic

import strategy.utils.hedge.{DeltaBand, DeltaCtx, QuotePolicy, QuoteStyle}

import hft.TestUnits.given
import hft.domain.*
import hft.engine.StrategyRunner
import hft.event.{AnyEvent, Event, Topics}
import hft.strategy.{OrderIntent, OutcomeEvent}

/** DeltaHedgeStrategy 机制单测。
  *
  * 重点验证：
  *   1. **判据与下单量是同一个真实净敞口** —— 信号只调阈值，不替换被测量的量；
  *   2. 净敞口 = 期权 delta + 现货余额 + 永续持仓，三者都以原值参与；
  *   3. ER 只影响阈值宽窄与报价方式，可用历史 K 线预热；
  *   4. 三道安全闸门 (读数缺失/陈旧、minHedgeQty、maxHedgeQty)。
  */
class DeltaHedgeStrategySpec extends munit.FunSuite:
  private val ex = Exchange.Okx
  private val sym = "ETH"
  private val ccy = "ETH"
  private val metas = Map((ex, sym) -> SymbolMeta(ex, sym, 0.01, 0.0001, 0.0001, 1.0))
  private val minute = 60_000L

  /** 固定阈值的死区 —— 把波动率/方向那两维隔离掉, 单独测触发与定量 */
  private final class Fixed(up: Coin, down: Coin) extends DeltaBand:
    def bands(ctx: DeltaCtx): (Coin, Coin) = (up, down)

  private def strat(
      band: DeltaBand,
      erBars: Int = 3,
      minQty: Coin = Coin(0.001),
      maxQty: Coin = Coin(Double.MaxValue),
      staleMs: Long = 0L,
      quotes: QuotePolicy = QuotePolicy.fixed(QuoteStyle.passive(0.01, 5000)),
  ) = DeltaHedgeStrategy(ex, sym, ccy, band,
    fastBarMs = minute, erPeriodBars = erBars, rvBars = 3, macdBarMs = 3_600_000L,
    quotes = quotes, minHedgeQty = minQty, maxHedgeQty = maxQty, maxExposureStaleMs = staleMs)

  private def feed(runner: StrategyRunner, ev: AnyEvent, localTs: Timestamp = -1): Vector[OutcomeEvent] =
    val lt = if localTs >= 0 then localTs else ev.localTs
    runner.onEvent(ev, lt).flatMap(_.as(OrderIntent)).map(_.outcome)

  private def bbo(px: Price, ts: Timestamp) = Event.stamped(Topics.Bbo, BBO(ex, sym, px, 1.0, px, 1.0, ts), ts, ts)

  /** 一条敞口读数。`spot` 是 actor 算这份 delta 时用的现价 —— gamma 修正以它为基准。 */
  private def exposure(
      optionDelta: Double,
      ts: Timestamp,
      coin: Double = 0.0,
      gamma: Double = 0.0,
      spot: Double = 3000.0,
      iv: Option[Double] = Some(0.6),
  ) = Event.stamped(OptionExposureTopic,
    OptionExposure(ex, ccy, Coin(optionDelta), Coin(gamma), Coin(coin), Price(spot), iv, 2, ts), ts, ts)

  private def position(size: Double, ts: Timestamp) =
    Event.stamped(Topics.Position, Position(AccountId.Live, ex, sym, Coin(size), 3000.0, 0.0), ts, ts)

  private def placed(out: Vector[OutcomeEvent]): Order = out match
    case Vector(OutcomeEvent.PlaceOrders(os, _)) => os.head
    case other                                   => fail(s"expected PlaceOrders, got $other")

  /** 起一个 runner 并把盘口喂上 (盘口是挂单价与 requote 时钟的来源) */
  private def runnerWith(s: DeltaHedgeStrategy): StrategyRunner =
    val r = StrategyRunner(s, metas, AccountId.Live)
    feed(r, bbo(3000.0, 0))
    r

  /** 用历史 K 线把 KAMA 预热到给定价位 (每根 h=l=c), 再喂上盘口 */
  private def runnerPrewarmed(s: DeltaHedgeStrategy, px: Double = 3000.0, bars: Int = 40): StrategyRunner =
    s.prewarmFast(Seq.fill(bars)((px, px, px)))
    runnerWith(s)

  test("敞口读数未到达 -> 不对冲 (不拿一个不存在的 delta 决策)"):
    val r = runnerWith(strat(Fixed(0.1, 0.1)))
    assertEquals(feed(r, bbo(3000.0, 1)), Vector.empty)

  test("判据与下单量是同一个真实净敞口 (信号只调阈值, 不替换被测量的量)"):
    val r = runnerPrewarmed(strat(Fixed(0.3, 0.3)))
    // 净敞口 0.5 > 阈值 0.3 -> 触发, 量就是 0.5
    val o = placed(feed(r, exposure(0.5, 10, gamma = -0.02, spot = 3100.0)))
    assertEquals(o.side, Side.Short)
    assertEquals(o.quantity, Coin(0.5), "量 = 判据用的那个量, 不存在第二个数")

  test("gamma 不参与判据 (它曾被用来把敞口折算到平滑价上, 那是把根本规则架空了)"):
    // 同一个净敞口, gamma 取任何值都必须给出同一个决定
    val a = runnerPrewarmed(strat(Fixed(0.3, 0.3)))
    val b2 = runnerPrewarmed(strat(Fixed(0.3, 0.3)))
    val oa = placed(feed(a, exposure(0.5, 10, gamma = 0.0, spot = 3000.0)))
    val ob = placed(feed(b2, exposure(0.5, 10, gamma = -5.0, spot = 3500.0)))
    assertEquals(oa.quantity, ob.quantity)
    assertEquals(oa.side, ob.side)

  test("死区内 -> 不动 (避免 delta 随价格漂移的抹布式调仓)"):
    val r = runnerPrewarmed(strat(Fixed(1.0, 1.0)))
    assertEquals(feed(r, exposure(0.5, 10)), Vector.empty)

  test("对冲后死区复位 (永续持仓抵扣掉敞口)"):
    val r = runnerPrewarmed(strat(Fixed(0.3, 0.3)))
    val o = placed(feed(r, exposure(2.0, 10)))
    assertEquals(o.side, Side.Short)
    feed(r, Event.stamped(Topics.OrderUpdate,
      OrderUpdate(AccountId.Live, "o1", Some("c1"), ex, sym, Side.Short, OrderStatus.Filled, 3000.0, 2.0, 2.0, 2.0, 20),
      20, 20))
    feed(r, position(-2.0, 20))
    assertEquals(feed(r, exposure(2.0, 30)), Vector.empty, "敞口已归零 -> 回到带内")

  test("净空敞口 -> 买入 (方向由真实敞口的符号定)"):
    val r = runnerPrewarmed(strat(Fixed(0.1, 0.1)))
    val o = placed(feed(r, exposure(-0.5, 10)))
    assertEquals(o.side, Side.Long)
    assertEquals(o.quantity, Coin(0.5))

  test("现货余额计入敞口 (币本位期权的保证金是真实裸多头)"):
    val r = runnerPrewarmed(strat(Fixed(0.1, 0.1)))
    // 期权 delta -0.3 + 现货 0.5 = 净多 0.2
    val o = placed(feed(r, exposure(-0.3, 10, coin = 0.5)))
    assertEquals(o.side, Side.Short)
    assert(math.abs(o.quantity.value - 0.2) < 1e-12, s"qty=${o.quantity.value}")

  test("永续持仓抵扣敞口 (净敞口 = 期权+现货+永续)"):
    val r = runnerPrewarmed(strat(Fixed(0.1, 0.1)))
    feed(r, position(-0.4, 5))
    val o = placed(feed(r, exposure(1.0, 10))) // 1.0 - 0.4 = 0.6
    assert(math.abs(o.quantity.value - 0.6) < 1e-12, s"qty=${o.quantity.value}")

  test("对冲量超 maxHedgeQty 硬上限 -> 不下单 (疑似 delta 计算 bug)"):
    val r = runnerWith(strat(Fixed(0.1, 0.1), erBars = 50, maxQty = Coin(1.0)))
    assertEquals(feed(r, exposure(5.0, 10)), Vector.empty)

  test("敞口读数陈旧 -> 暂停对冲 (宁可不动也不按过期 delta 乱挂)"):
    val r = runnerWith(strat(Fixed(0.1, 0.1), erBars = 50, staleMs = 1000))
    // 读数时间戳 10, 本地处理时刻 5000 -> 陈旧 4990ms > 1000ms
    assertEquals(feed(r, exposure(0.5, 10), localTs = 5000), Vector.empty)

  test("挂单未成交超 requoteMs -> 撤单 (下一 tick 按新价重挂)"):
    val r = runnerWith(strat(Fixed(0.1, 0.1), erBars = 50))
    feed(r, exposure(0.5, 10))
    feed(r, Event.stamped(Topics.OrderUpdate,
      OrderUpdate(AccountId.Live, "o1", Some("c1"), ex, sym, Side.Short, OrderStatus.Pending, 3030.0, 0.5, 0.0, 0.0, 1000),
      1000, 1000))
    assertEquals(feed(r, bbo(3000.0, 3000)), Vector.empty, "2s < requote 5s -> 不撤")
    feed(r, bbo(3000.0, 7000)) match
      case Vector(OutcomeEvent.CancelOrder(_, _, ref)) => assertEquals(ref, OrderRef.ByExchangeId("o1"))
      case other                                       => fail(s"expected CancelOrder, got $other")

  test("下单到确认之间不重复下单"):
    val r = runnerWith(strat(Fixed(0.1, 0.1), erBars = 50))
    assert(feed(r, exposure(0.5, 10)).nonEmpty)
    assertEquals(feed(r, exposure(0.5, 11)), Vector.empty)

  test("被动挂单在盘口外侧 (PostOnly 不吃单)"):
    val r = runnerWith(strat(Fixed(0.1, 0.1), erBars = 50))
    placed(feed(r, exposure(0.5, 10))).orderType match
      case OrderType.Limit(px, tif) =>
        assertEquals(tif, TimeInForce.PostOnly)
        assert(math.abs(px.value - 3000.0 * 1.01) < 1e-6, s"卖单应挂在 ask 外 1%, 实为 $px")
      case other => fail(s"expected Limit, got $other")

  test("别的币种的敞口读数不参与决策"):
    val r = runnerWith(strat(Fixed(0.1, 0.1), erBars = 50))
    val other = Event.stamped(OptionExposureTopic,
      OptionExposure(ex, "BTC", Coin(5.0), Coin(0.0), Coin(0.0), Price(60000), Some(0.6), 1, 10), 10, 10)
    assertEquals(feed(r, other), Vector.empty)
    assertEquals(feed(r, bbo(3000.0, 11)), Vector.empty, "BTC 那条不该被存下来当本策略的敞口")

  // ---------- 报价方式随体制切换 (KAMA 的效率比驱动, 现在看的是**价格**路径) ----------

  private val calmStyle = QuoteStyle.passive(0.01, 60_000)
  private val trendStyle = QuoteStyle.crossing(0.001, 1000)
  private val byEr = QuotePolicy.byEfficiency(0.5, calmStyle, trendStyle)

  /** 按 1 分钟节奏喂一段**价格**序列，每根之后给一条恒定的越界敞口，收集全部下单意图。
    *
    * gamma 取 0，使信号等于真实敞口、恒定越界 —— 这样变量只剩报价方式。每收到一张单就撤掉，
    * 让后面的 bar 能继续触发，最后看 ER 预热之后那张单用的是哪一种。
    */
  private def ordersOverPrices(r: StrategyRunner, prices: Seq[Double]): Vector[Order] =
    val out = Vector.newBuilder[Order]
    prices.zipWithIndex.foreach { case (px, i) =>
      val ts = (i + 1) * minute
      val evs = feed(r, bbo(px, ts)) ++ feed(r, exposure(2.0, ts, gamma = 0.0, spot = px))
      evs.foreach {
        case OutcomeEvent.PlaceOrders(os, _) =>
          out += os.head
          feed(r, Event.stamped(Topics.OrderUpdate,
            OrderUpdate(AccountId.Live, "o1", Some("c1"), ex, sym, os.head.side, OrderStatus.Cancelled,
              Price(px), os.head.quantity, 0.0, 0.0, ts),
            ts, ts))
        case _ => ()
      }
    }
    out.result()

  private def tifOf(o: Order): TimeInForce = o.orderType match
    case OrderType.Limit(_, tif) => tif
    case other                   => fail(s"expected Limit, got $other")

  private def pxOf(o: Order): Double = o.orderType match
    case OrderType.Limit(px, _) => px.value
    case other                  => fail(s"expected Limit, got $other")

  test("价格来回折返 (ER 低) -> 被动挂在 ask 之上, PostOnly"):
    val r = runnerWith(strat(Fixed(0.1, 0.1), erBars = 3, quotes = byEr))
    val prices = (1 to 16).map(i => if i % 2 == 0 then 3010.0 else 2990.0)
    val orders = ordersOverPrices(r, prices)
    assert(orders.sizeIs >= 2, s"应触发多次, 实为 ${orders.size}")
    assertEquals(tifOf(orders.last), TimeInForce.PostOnly, "ER 预热后判为平缓 -> 被动挂")
    val lastPx = prices.last
    assert(math.abs(pxOf(orders.last) - lastPx * 1.01) < 1e-6, s"被动卖价应在 ask 之上, 实为 ${pxOf(orders.last)}")

  test("价格单边走 (ER 高) -> 跨价挂在 bid 之下, GTC"):
    val r = runnerWith(strat(Fixed(0.1, 0.1), erBars = 3, quotes = byEr))
    val prices = (1 to 16).map(i => 3000.0 + i * 5.0)
    val orders = ordersOverPrices(r, prices)
    assert(orders.sizeIs >= 2, s"应触发多次, 实为 ${orders.size}")
    assertEquals(tifOf(orders.last), TimeInForce.GTC, "ER 预热后仍判为单边 -> 跨价")
    assert(math.abs(pxOf(orders.last) - prices.last * 0.999) < 1e-6, s"跨价卖价应在 bid 之下, 实为 ${pxOf(orders.last)}")

  test("ER 未预热 -> 按单边处理 (裸着敞口比多付手续费贵)"):
    val r = runnerWith(strat(Fixed(0.1, 0.1), erBars = 50, quotes = byEr))
    assertEquals(tifOf(placed(feed(r, exposure(0.5, 10)))), TimeInForce.GTC)

  test("KAMA 用历史 K 线预热 -> 开机即就绪, 不再有头几十分钟的盲区"):
    // 预热成一段单边上行的历史 -> ER 高 -> 首单就按单边处理; 且平滑价已可用
    val s1 = strat(Fixed(0.1, 0.1), erBars = 3, quotes = byEr)
    s1.prewarmFast((1 to 20).map(i => { val p = 3000.0 + i * 5.0; (p, p, p) }))
    val trending = runnerWith(s1)
    assertEquals(tifOf(placed(feed(trending, exposure(2.0, 10, gamma = 0.0)))), TimeInForce.GTC)
    // 预热成来回折返的历史 -> ER 低 -> 首单就按平缓处理 (未预热时这里会是 GTC)
    val s2 = strat(Fixed(0.1, 0.1), erBars = 3, quotes = byEr)
    s2.prewarmFast((1 to 20).map(i => { val p = if i % 2 == 0 then 3010.0 else 2990.0; (p, p, p) }))
    val chopping = runnerWith(s2)
    assertEquals(tifOf(placed(feed(chopping, exposure(2.0, 10, gamma = 0.0)))), TimeInForce.PostOnly,
      "预热带来的就绪状态直接决定首单的报价方式")

  test("订单超时宽于最长存活时间, 否则框架会把正常挂单当丢单清理"):
    val s = strat(Fixed(0.1, 0.1), quotes = byEr)
    assert(s.orderTimeoutMs > calmStyle.ttlMs, s"orderTimeoutMs=${s.orderTimeoutMs} 须 > ${calmStyle.ttlMs}")

  // ---------- 阈值随预测波动范围走 (ER 已退出死区) ----------

  private def volBand = DeltaBand.volScaled(
    tightMult = 0.5, looseMult = 2.0, horizonMs = 30 * minute, minTheta = Coin(0.001), maxTheta = Coin(100.0))

  private def volStrat(prewarmPrices: Seq[Double], src: SigmaSource = SigmaSource.Realized) =
    val st = DeltaHedgeStrategy(ex, sym, ccy, volBand,
      fastBarMs = minute, erPeriodBars = 3, rvBars = 5, sigmaSource = src, macdBarMs = 3_600_000L,
      quotes = QuotePolicy.fixed(QuoteStyle.passive(0.01, 5000)))
    st.prewarmFast(prewarmPrices.map(p => (p, p, p)))
    st

  /** 低波动/高波动两段历史 (同一均值, 只有幅度不同) */
  private val calmHistory = (1 to 30).map(i => 3000.0 + (if i % 2 == 0 then 1.0 else -1.0))
  private val wildHistory = (1 to 30).map(i => 3000.0 + (if i % 2 == 0 then 60.0 else -60.0))

  test("同一敞口: 低波动下越界对冲, 高波动下阈值被放宽而不动"):
    // 唯一的差别是历史波动 —— 敞口、gamma、现价、方向全都一样
    val fired = feed(runnerWith(volStrat(calmHistory)), exposure(0.5, 10, gamma = -0.01))
    val held = feed(runnerWith(volStrat(wildHistory)), exposure(0.5, 10, gamma = -0.01))
    assert(fired.nonEmpty, "低波动 -> 阈值小 -> 0.5 越界, 对冲")
    assertEquals(held, Vector.empty, "高波动 -> 阈值大 -> 0.5 在带内, 不动")

  test("gamma 越大阈值越宽 (敞口扩散更快, 按比例放宽以控成本)"):
    val small = feed(runnerWith(volStrat(wildHistory)), exposure(0.5, 10, gamma = -0.0001))
    val large = feed(runnerWith(volStrat(wildHistory)), exposure(0.5, 10, gamma = -0.05))
    assert(small.nonEmpty, "gamma 极小 -> 阈值趋下限 -> 对冲")
    assertEquals(large, Vector.empty, "gamma 大 -> 阈值宽 -> 不动")

  test("maxTheta 咬住: 极端波动下敞口仍有硬上界"):
    val capped = DeltaBand.volScaled(0.5, 2.0, 30 * minute, Coin(0.001), Coin(0.4))
    val st = DeltaHedgeStrategy(ex, sym, ccy, capped,
      fastBarMs = minute, erPeriodBars = 3, rvBars = 5, macdBarMs = 3_600_000L,
      quotes = QuotePolicy.fixed(QuoteStyle.passive(0.01, 5000)))
    st.prewarmFast(wildHistory.map(p => (p, p, p)))
    val o = placed(feed(runnerWith(st), exposure(0.41, 10, gamma = -0.05)))
    assertEquals(o.quantity, Coin(0.41), "超过上界 0.4 -> 必然对冲, 量是真实敞口")

  test("σ 来源可切换: 取期权 IV 时不看历史波动"):
    // 历史是高波动的, 但 IV 给一个很小的值 -> 阈值小 -> 对冲
    val st = volStrat(wildHistory, SigmaSource.ImpliedVol)
    assert(feed(runnerWith(st), exposure(0.5, 10, gamma = -0.01, iv = Some(0.01))).nonEmpty)
    // 同一历史, IV 给大值 -> 阈值大 -> 不动
    val st2 = volStrat(wildHistory, SigmaSource.ImpliedVol)
    assertEquals(feed(runnerWith(st2), exposure(0.5, 10, gamma = -0.01, iv = Some(5.0))), Vector.empty)

  test("σ 未就绪 -> 阈值取下限 (宁可对冲频繁, 不按不存在的波动率放宽敞口)"):
    val st = DeltaHedgeStrategy(ex, sym, ccy, volBand,
      fastBarMs = minute, erPeriodBars = 3, rvBars = 100, macdBarMs = 3_600_000L, // RV 远未就绪
      quotes = QuotePolicy.fixed(QuoteStyle.passive(0.01, 5000)))
    assert(feed(runnerWith(st), exposure(0.5, 10, gamma = -0.05)).nonEmpty, "下限 0.001 -> 必然对冲")

  test("订单超时宽于最长存活时间, 否则框架会把正常挂单当丢单清理"):
    val s = strat(Fixed(0.1, 0.1), quotes = byEr)
    assert(s.orderTimeoutMs > calmStyle.ttlMs, s"orderTimeoutMs=${s.orderTimeoutMs} 须 > ${calmStyle.ttlMs}")
