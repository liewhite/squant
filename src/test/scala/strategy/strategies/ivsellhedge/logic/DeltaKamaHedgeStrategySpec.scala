package strategy.strategies.ivsellhedge.logic

import strategy.utils.hedge.{DeltaBand, DeltaCtx, QuotePolicy, QuoteStyle}

import hft.TestUnits.given
import hft.domain.*
import hft.engine.StrategyRunner
import hft.event.{AnyEvent, Event, Topics}
import hft.state.StateManager
import hft.strategy.{OrderIntent, OutcomeEvent}

/** DeltaKamaHedgeStrategy 机制单测。
  *
  * 重点验证三件与设计直接相关的事：
  *   1. **判据用平滑信号、下单量用真实敞口** —— 两者刻意不同；
  *   2. **只平滑外生敞口** —— 对冲后信号立刻回落, 死区自动复位 (不会连着重复触发);
  *   3. KAMA 预热不足**回退真实敞口**, 而不是放着裸敞口不管。
  */
class DeltaKamaHedgeStrategySpec extends munit.FunSuite:
  private val ex = Exchange.Okx
  private val sym = "ETH"
  private val ccy = "ETH"
  private val metas = Map((ex, sym) -> SymbolMeta(ex, sym, 0.01, 0.0001, 0.0001, 1.0))
  private val bucket = 1000L

  /** 固定阈值的死区 —— 把 MACD 那一维隔离掉, 单独测触发/定量 */
  private final class Fixed(up: Coin, down: Coin) extends DeltaBand:
    def bands(ctx: DeltaCtx): (Coin, Coin) = (up, down)

  private def strat(
      band: DeltaBand,
      kamaEr: Int = 2,
      minQty: Coin = Coin(0.001),
      maxQty: Coin = Coin(Double.MaxValue),
      staleMs: Long = 0L,
      quotes: QuotePolicy = QuotePolicy.fixed(QuoteStyle.passive(0.01, 5000)),
  ) = DeltaKamaHedgeStrategy(ex, sym, ccy, band,
    kamaBucketMs = bucket, kamaErPeriod = kamaEr, macdBarMs = 3_600_000L,
    quotes = quotes, minHedgeQty = minQty, maxHedgeQty = maxQty, maxExposureStaleMs = staleMs)

  private def feed(runner: StrategyRunner, ev: AnyEvent, localTs: Timestamp = -1): Vector[OutcomeEvent] =
    val lt = if localTs >= 0 then localTs else ev.localTs
    runner.onEvent(ev, lt).flatMap(_.as(OrderIntent)).map(_.outcome)

  private def bbo(px: Price, ts: Timestamp) = Event.stamped(Topics.Bbo, BBO(ex, sym, px, 1.0, px, 1.0, ts), ts, ts)

  /** 一条敞口读数：optionDelta + coinBalance 就是外生敞口 O */
  private def exposure(optionDelta: Double, ts: Timestamp, coin: Double = 0.0) =
    Event.stamped(OptionExposureTopic,
      OptionExposure(ex, ccy, Coin(optionDelta), Coin(-0.01), Coin(coin), Price(3000), 2, ts), ts, ts)

  private def position(size: Double, ts: Timestamp) =
    Event.stamped(Topics.Position, Position(AccountId.Live, ex, sym, Coin(size), 3000.0, 0.0), ts, ts)

  private def placed(out: Vector[OutcomeEvent]): Order = out match
    case Vector(OutcomeEvent.PlaceOrders(os, _)) => os.head
    case other                                   => fail(s"expected PlaceOrders, got $other")

  /** 起一个 runner 并把盘口喂上 (盘口是挂单价与 requote 时钟的来源) */
  private def runnerWith(s: DeltaKamaHedgeStrategy): StrategyRunner =
    val r = StrategyRunner(s, metas, AccountId.Live)
    feed(r, bbo(3000.0, 0))
    r

  test("敞口读数未到达 -> 不对冲 (不拿一个不存在的 delta 决策)"):
    val r = runnerWith(strat(Fixed(0.1, 0.1)))
    assertEquals(feed(r, bbo(3000.0, 1)), Vector.empty)

  test("KAMA 预热不足 -> 回退真实敞口判越界 (降级但不裸奔)"):
    val r = runnerWith(strat(Fixed(0.1, 0.1), kamaEr = 50)) // 预热远未完成
    val o = placed(feed(r, exposure(0.5, 10)))
    assertEquals(o.side, Side.Short)                        // 净多 0.5 -> 卖
    assertEquals(o.quantity, Coin(0.5))

  test("死区内 -> 不动 (避免 delta 随价格漂移的抹布式调仓)"):
    val r = runnerWith(strat(Fixed(1.0, 1.0), kamaEr = 50))
    assertEquals(feed(r, exposure(0.5, 10)), Vector.empty)

  test("判据用平滑信号, 下单量用真实敞口 —— 两者不同"):
    // 桶宽 1s, erPeriod=2 -> 3 个已结束桶即就绪。先用小敞口把 KAMA 喂到 0 附近,
    // 再突然给一个大敞口: 真实值立刻越界, 而 KAMA 被历史压着, 要几个桶才跟上。
    val r = runnerWith(strat(Fixed(0.3, 0.3), kamaEr = 2))
    (1 to 5).foreach(i => feed(r, exposure(0.0, i * bucket)))
    assertEquals(feed(r, exposure(2.0, 6 * bucket)), Vector.empty, "真实敞口 2.0 已越界, 但平滑信号还在带内 -> 不该动")
    // 持续保持大敞口: KAMA 跟上后才触发一次 (触发后进入等确认, 故只会有一张单)
    val orders = (7 to 20).flatMap(i => feed(r, exposure(2.0, i * bucket)))
    assertEquals(orders.size, 1, s"应恰好触发一次, 实为 ${orders.size}")
    val o = placed(orders.toVector)
    assertEquals(o.side, Side.Short)
    assertEquals(o.quantity, Coin(2.0), "数量按真实敞口, 不是平滑值")

  test("只平滑外生敞口: 对冲仓位以原值入信号 -> 对冲后死区立刻复位"):
    // 让 KAMA 稳定在 2.0 (强越界), 触发一次对冲; 随后把永续持仓补到 -2.0 (敞口归零)。
    // 若把 O+P 整体平滑, 信号会继续停在越界值上 -> 会再次触发;
    // 只平滑 O 的话, P 立刻生效 -> 信号 ≈ 0 -> 回到带内。
    val r = runnerWith(strat(Fixed(0.3, 0.3), kamaEr = 2))
    val orders = (1 to 20).flatMap(i => feed(r, exposure(2.0, i * bucket)))
    assertEquals(orders.size, 1, s"等确认期间不该重复下单, 实为 ${orders.size}")
    assertEquals(placed(orders.toVector).side, Side.Short)
    // 挂单成交 -> 永续持仓 -2.0, 挂单腿回到空闲
    feed(r, Event.stamped(Topics.OrderUpdate,
      OrderUpdate(AccountId.Live, "o1", Some("c1"), ex, sym, Side.Short, OrderStatus.Filled, 3000.0, 2.0, 2.0, 2.0, 22 * bucket),
      22 * bucket, 22 * bucket))
    feed(r, position(-2.0, 22 * bucket))
    val after = (23 to 30).flatMap(i => feed(r, exposure(2.0, i * bucket)))
    assertEquals(after, Seq.empty, "敞口已被对冲平掉 -> 信号回到带内, 不该再下单")

  test("净空敞口 -> 买入 (方向由真实敞口的符号定)"):
    val r = runnerWith(strat(Fixed(0.1, 0.1), kamaEr = 50))
    val o = placed(feed(r, exposure(-0.5, 10)))
    assertEquals(o.side, Side.Long)
    assertEquals(o.quantity, Coin(0.5))

  test("现货余额计入敞口 (币本位期权的保证金是真实裸多头)"):
    val r = runnerWith(strat(Fixed(0.1, 0.1), kamaEr = 50))
    // 期权 delta -0.3 + 现货 0.5 = 净多 0.2
    val o = placed(feed(r, exposure(-0.3, 10, coin = 0.5)))
    assertEquals(o.side, Side.Short)
    assert(math.abs(o.quantity.value - 0.2) < 1e-12, s"qty=${o.quantity.value}")

  test("永续持仓抵扣敞口 (净敞口 = 期权+现货+永续)"):
    val r = runnerWith(strat(Fixed(0.1, 0.1), kamaEr = 50))
    feed(r, position(-0.4, 5))
    val o = placed(feed(r, exposure(1.0, 10))) // 1.0 - 0.4 = 0.6
    assert(math.abs(o.quantity.value - 0.6) < 1e-12, s"qty=${o.quantity.value}")

  test("触发后真实敞口低于 minHedgeQty -> 不下单 (吸收 KAMA 滞后带来的微量触发)"):
    // 构造"信号仍越界、真实敞口已很小"这个 KAMA 滞后的典型局面:
    // 先让敞口在 2.0 上稳住 (KAMA=2.0), 再骤降到 0.05 —— KAMA 会停在中途 (约 0.6),
    // 信号仍在带外, 而真实敞口只有 0.05。
    def lagged(minQty: Coin): Seq[OutcomeEvent] =
      val r = runnerWith(strat(Fixed(0.3, 0.3), kamaEr = 2, minQty = minQty))
      (1 to 5).foreach(i => feed(r, exposure(2.0, i * bucket)))  // 这一阶段会挂一张单
      feed(r, Event.stamped(Topics.OrderUpdate,                   // 撤掉它, 让挂单腿回到空闲
        OrderUpdate(AccountId.Live, "o1", Some("c1"), ex, sym, Side.Short, OrderStatus.Cancelled, 3030.0, 2.0, 0.0, 0.0, 5 * bucket),
        5 * bucket, 5 * bucket))
      (6 to 15).flatMap(i => feed(r, exposure(0.05, i * bucket)))
    assertEquals(lagged(Coin(0.5)), Seq.empty, "真实敞口 0.05 < minHedgeQty 0.5 -> 不下单")
    // 同一序列换成极小的闸门就会下单 —— 证明上面拦住它的是闸门, 而不是压根没触发
    val fired = lagged(Coin(0.0001))
    assertEquals(fired.size, 1, "同一序列在闸门放宽后应触发, 否则上面的用例是空转")
    assert(math.abs(placed(fired.toVector).quantity.value - 0.05) < 1e-12, "量按真实敞口, 不是平滑值")

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

  // ---------- 报价方式随体制切换 (KAMA 的效率比驱动) ----------

  private val calmStyle = QuoteStyle.passive(0.01, 60_000)
  private val trendStyle = QuoteStyle.crossing(0.001, 1000)
  private val byEr = QuotePolicy.byEfficiency(0.5, calmStyle, trendStyle)

  /** 喂一段敞口序列，收集全部下单意图。
    *
    * 头几个桶 ER 还没预热 (此时按单边处理)，第一张单必然是跨价的 —— 所以每收到一张单就撤掉，
    * 让后续的桶能继续触发，最后看**ER 预热之后**那张单用的是哪种方式。
    */
  private def ordersOver(r: StrategyRunner, series: Seq[(Int, Double)]): Vector[Order] =
    val out = Vector.newBuilder[Order]
    series.foreach { case (i, o) =>
      val evs = feed(r, exposure(o, i * bucket))
      evs.foreach {
        case OutcomeEvent.PlaceOrders(os, _) =>
          out += os.head
          // 撤掉它, 让挂单腿回到空闲 (否则等确认会挡住后面所有桶)
          feed(r, Event.stamped(Topics.OrderUpdate,
            OrderUpdate(AccountId.Live, "o1", Some("c1"), ex, sym, os.head.side, OrderStatus.Cancelled,
              3000.0, os.head.quantity, 0.0, 0.0, i * bucket),
            i * bucket, i * bucket))
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

  test("敞口来回折返 (ER 低) -> 被动挂在 ask 之上, PostOnly"):
    // 敞口在 ±2 之间折返: 净位移≈0、路径很长 -> ER≈0 -> 平缓; KAMA≈-2 远超死区 0.1 故持续触发
    val r = runnerWith(strat(Fixed(0.1, 0.1), kamaEr = 2, quotes = byEr))
    val orders = ordersOver(r, (1 to 14).map(i => (i, if i % 2 == 0 then 2.0 else -2.0)))
    assert(orders.sizeIs >= 2, s"应触发多次, 实为 ${orders.size}")
    assertEquals(tifOf(orders.head), TimeInForce.GTC, "第一张在 ER 预热前 -> 按单边处理")
    assertEquals(tifOf(orders.last), TimeInForce.PostOnly, "ER 预热后判为平缓 -> 被动挂")
    assert(math.abs(pxOf(orders.last) - 3000.0 * 1.01) < 1e-6 || math.abs(pxOf(orders.last) - 3000.0 * 0.99) < 1e-6,
      s"被动价应在盘口之外, 实为 ${pxOf(orders.last)}")

  test("敞口单边走 (ER 高) -> 跨价挂在 bid 之下, GTC"):
    // 敞口单边递增: 净位移≈路径长度 -> ER≈1 -> 单边 -> 跨价追单
    val r = runnerWith(strat(Fixed(0.1, 0.1), kamaEr = 2, quotes = byEr))
    val orders = ordersOver(r, (1 to 14).map(i => (i, i * 0.5)))
    assert(orders.sizeIs >= 2, s"应触发多次, 实为 ${orders.size}")
    assertEquals(tifOf(orders.last), TimeInForce.GTC, "ER 预热后仍判为单边 -> 跨价")
    assertEquals(orders.last.side, Side.Short, "净多 -> 卖")
    assert(math.abs(pxOf(orders.last) - 3000.0 * 0.999) < 1e-6, s"卖单应挂在 bid 之下, 实为 ${pxOf(orders.last)}")

  test("ER 预热不足 -> 按单边处理 (裸着敞口比多付手续费贵)"):
    val r = runnerWith(strat(Fixed(0.1, 0.1), kamaEr = 50, quotes = byEr))
    placed(feed(r, exposure(0.5, 10))).orderType match
      case OrderType.Limit(_, tif) => assertEquals(tif, TimeInForce.GTC)
      case other                   => fail(s"expected Limit, got $other")

  test("订单超时宽于最长存活时间, 否则框架会把正常挂单当丢单清理"):
    val s = strat(Fixed(0.1, 0.1), quotes = byEr)
    assert(s.orderTimeoutMs > calmStyle.ttlMs, s"orderTimeoutMs=${s.orderTimeoutMs} 须 > ${calmStyle.ttlMs}")
