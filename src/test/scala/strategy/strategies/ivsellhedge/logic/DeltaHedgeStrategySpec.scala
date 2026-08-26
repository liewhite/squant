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

  /** 固定阈值的死区 —— 把 MACD 那一维隔离掉, 单独测触发/定量 */
  private final class Fixed(up: Coin, down: Coin) extends DeltaBand:
    def bands(ctx: DeltaCtx): (Coin, Coin) = (up, down)

  private def strat(
      band: DeltaBand,
      kamaEr: Int = 3,
      minQty: Coin = Coin(0.001),
      maxQty: Coin = Coin(Double.MaxValue),
      staleMs: Long = 0L,
      quotes: QuotePolicy = QuotePolicy.fixed(QuoteStyle.passive(0.01, 5000)),
  ) = DeltaHedgeStrategy(ex, sym, ccy, band,
    kamaBarMs = minute, kamaErBars = kamaEr, macdBarMs = 3_600_000L,
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
  ) = Event.stamped(OptionExposureTopic,
    OptionExposure(ex, ccy, Coin(optionDelta), Coin(gamma), Coin(coin), Price(spot), 2, ts), ts, ts)

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
    s.prewarmKama(Seq.fill(bars)((px, px, px)))
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
    val r = runnerWith(strat(Fixed(0.1, 0.1), kamaEr = 50, maxQty = Coin(1.0)))
    assertEquals(feed(r, exposure(5.0, 10)), Vector.empty)

  test("敞口读数陈旧 -> 暂停对冲 (宁可不动也不按过期 delta 乱挂)"):
    val r = runnerWith(strat(Fixed(0.1, 0.1), kamaEr = 50, staleMs = 1000))
    // 读数时间戳 10, 本地处理时刻 5000 -> 陈旧 4990ms > 1000ms
    assertEquals(feed(r, exposure(0.5, 10), localTs = 5000), Vector.empty)

  test("挂单未成交超 requoteMs -> 撤单 (下一 tick 按新价重挂)"):
    val r = runnerWith(strat(Fixed(0.1, 0.1), kamaEr = 50))
    feed(r, exposure(0.5, 10))
    feed(r, Event.stamped(Topics.OrderUpdate,
      OrderUpdate(AccountId.Live, "o1", Some("c1"), ex, sym, Side.Short, OrderStatus.Pending, 3030.0, 0.5, 0.0, 0.0, 1000),
      1000, 1000))
    assertEquals(feed(r, bbo(3000.0, 3000)), Vector.empty, "2s < requote 5s -> 不撤")
    feed(r, bbo(3000.0, 7000)) match
      case Vector(OutcomeEvent.CancelOrder(_, _, ref)) => assertEquals(ref, OrderRef.ByExchangeId("o1"))
      case other                                       => fail(s"expected CancelOrder, got $other")

  test("下单到确认之间不重复下单"):
    val r = runnerWith(strat(Fixed(0.1, 0.1), kamaEr = 50))
    assert(feed(r, exposure(0.5, 10)).nonEmpty)
    assertEquals(feed(r, exposure(0.5, 11)), Vector.empty)

  test("被动挂单在盘口外侧 (PostOnly 不吃单)"):
    val r = runnerWith(strat(Fixed(0.1, 0.1), kamaEr = 50))
    placed(feed(r, exposure(0.5, 10))).orderType match
      case OrderType.Limit(px, tif) =>
        assertEquals(tif, TimeInForce.PostOnly)
        assert(math.abs(px.value - 3000.0 * 1.01) < 1e-6, s"卖单应挂在 ask 外 1%, 实为 $px")
      case other => fail(s"expected Limit, got $other")

  test("别的币种的敞口读数不参与决策"):
    val r = runnerWith(strat(Fixed(0.1, 0.1), kamaEr = 50))
    val other = Event.stamped(OptionExposureTopic,
      OptionExposure(ex, "BTC", Coin(5.0), Coin(0.0), Coin(0.0), Price(60000), 1, 10), 10, 10)
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
    val r = runnerWith(strat(Fixed(0.1, 0.1), kamaEr = 3, quotes = byEr))
    val prices = (1 to 16).map(i => if i % 2 == 0 then 3010.0 else 2990.0)
    val orders = ordersOverPrices(r, prices)
    assert(orders.sizeIs >= 2, s"应触发多次, 实为 ${orders.size}")
    assertEquals(tifOf(orders.last), TimeInForce.PostOnly, "ER 预热后判为平缓 -> 被动挂")
    val lastPx = prices.last
    assert(math.abs(pxOf(orders.last) - lastPx * 1.01) < 1e-6, s"被动卖价应在 ask 之上, 实为 ${pxOf(orders.last)}")

  test("价格单边走 (ER 高) -> 跨价挂在 bid 之下, GTC"):
    val r = runnerWith(strat(Fixed(0.1, 0.1), kamaEr = 3, quotes = byEr))
    val prices = (1 to 16).map(i => 3000.0 + i * 5.0)
    val orders = ordersOverPrices(r, prices)
    assert(orders.sizeIs >= 2, s"应触发多次, 实为 ${orders.size}")
    assertEquals(tifOf(orders.last), TimeInForce.GTC, "ER 预热后仍判为单边 -> 跨价")
    assert(math.abs(pxOf(orders.last) - prices.last * 0.999) < 1e-6, s"跨价卖价应在 bid 之下, 实为 ${pxOf(orders.last)}")

  test("ER 未预热 -> 按单边处理 (裸着敞口比多付手续费贵)"):
    val r = runnerWith(strat(Fixed(0.1, 0.1), kamaEr = 50, quotes = byEr))
    assertEquals(tifOf(placed(feed(r, exposure(0.5, 10)))), TimeInForce.GTC)

  test("KAMA 用历史 K 线预热 -> 开机即就绪, 不再有头几十分钟的盲区"):
    // 预热成一段单边上行的历史 -> ER 高 -> 首单就按单边处理; 且平滑价已可用
    val s1 = strat(Fixed(0.1, 0.1), kamaEr = 3, quotes = byEr)
    s1.prewarmKama((1 to 20).map(i => { val p = 3000.0 + i * 5.0; (p, p, p) }))
    val trending = runnerWith(s1)
    assertEquals(tifOf(placed(feed(trending, exposure(2.0, 10, gamma = 0.0)))), TimeInForce.GTC)
    // 预热成来回折返的历史 -> ER 低 -> 首单就按平缓处理 (未预热时这里会是 GTC)
    val s2 = strat(Fixed(0.1, 0.1), kamaEr = 3, quotes = byEr)
    s2.prewarmKama((1 to 20).map(i => { val p = if i % 2 == 0 then 3010.0 else 2990.0; (p, p, p) }))
    val chopping = runnerWith(s2)
    assertEquals(tifOf(placed(feed(chopping, exposure(2.0, 10, gamma = 0.0)))), TimeInForce.PostOnly,
      "预热带来的就绪状态直接决定首单的报价方式")

  test("订单超时宽于最长存活时间, 否则框架会把正常挂单当丢单清理"):
    val s = strat(Fixed(0.1, 0.1), quotes = byEr)
    assert(s.orderTimeoutMs > calmStyle.ttlMs, s"orderTimeoutMs=${s.orderTimeoutMs} 须 > ${calmStyle.ttlMs}")

  // ---------- ER 只调阈值宽窄, 不替换判据 ----------

  test("同一个净敞口: 震荡里被放宽的阈值挡住, 趋势里被收紧的阈值放行"):
    val band = DeltaBand.adaptive(Coin(0.4), macdTighten = 1.0, chopWiden = 2.0, trendTighten = 0.5)
    def run(prewarmPrices: Seq[Double], exposureEth: Double): Vector[OutcomeEvent] =
      val st = DeltaHedgeStrategy(ex, sym, ccy, band,
        kamaBarMs = minute, kamaErBars = 3, macdBarMs = 3_600_000L,
        quotes = QuotePolicy.fixed(QuoteStyle.passive(0.01, 5000)))
      st.prewarmKama(prewarmPrices.map(p => (p, p, p)))
      feed(runnerWith(st), exposure(exposureEth, 10))
    val chop = (1 to 20).map(i => if i % 2 == 0 then 3010.0 else 2990.0)  // ER≈0 -> 阈值 0.8
    val trend = (1 to 20).map(i => 3000.0 + i * 5.0)                      // ER≈1 -> 阈值 0.2
    // 敞口 0.5: 落在 0.2 与 0.8 之间 —— 唯一的区别就是阈值
    assertEquals(run(chop, 0.5), Vector.empty, "震荡: 阈值放宽到 0.8 -> 0.5 在带内, 不对冲")
    assert(run(trend, 0.5).nonEmpty, "趋势: 阈值收紧到 0.2 -> 0.5 越界, 对冲")

  test("真实敞口有上界: 无论 ER 怎么走, 超过 base×chopWiden 必然对冲"):
    val band = DeltaBand.adaptive(Coin(0.4), macdTighten = 1.0, chopWiden = 2.0, trendTighten = 0.5)
    val st = DeltaHedgeStrategy(ex, sym, ccy, band,
      kamaBarMs = minute, kamaErBars = 3, macdBarMs = 3_600_000L,
      quotes = QuotePolicy.fixed(QuoteStyle.passive(0.01, 5000)))
    st.prewarmKama((1 to 20).map(i => { val p = if i % 2 == 0 then 3010.0 else 2990.0; (p, p, p) })) // 最放宽的体制
    val o = placed(feed(runnerWith(st), exposure(0.81, 10)))
    assertEquals(o.quantity, Coin(0.81), "超过上界 0.8 -> 必然对冲, 且量是真实敞口")
