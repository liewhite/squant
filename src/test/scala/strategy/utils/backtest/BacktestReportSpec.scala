package strategy.utils.backtest

import hft.domain.*
import hft.event.{AnyEvent, Event, Topics}

/** BacktestRecorder 旁路观察 + BacktestReport 出图 的纯行为单测：
  * 预热过滤、基准/收益/回撤/buy&hold 计算、成交点采集与买卖向、以及 markers 渲染。 */
class BacktestReportSpec extends munit.FunSuite:
  private val ex = Exchange.Binance
  private val sym: Symbol = "ETHUSDT"
  private val H = 3_600_000L

  private def trade(ts: Long, px: Double): AnyEvent =
    Event.stamped(Topics.Trade, MarketTrade(ex, sym, px, 1.0, isBuyerMaker = false, ts), ts, ts)
  private def fill(ts: Long, side: Side, px: Double, qty: Double): AnyEvent =
    Event.stamped(Topics.Fill, Fill(AccountId.Live, ex, sym, side, px, qty, ts), ts, ts)
  private def acct(ts: Long, equity: Double): AnyEvent =
    Event.stamped(Topics.AccountInfo, AccountInfo(AccountId.Live, ex, equity = equity, notional = 0.0), ts, ts)

  test("预热区间 (startMs 之前) 的成交/采样全部忽略"):
    val rec = BacktestRecorder(ex, sym, startMs = 1000L, initialBalance = 100.0)
    Seq(
      trade(500, 10.0), fill(500, Side.Long, 10.0, 1.0), acct(500, 100.0), // 预热, 忽略
      trade(1000, 20.0), acct(1000, 100.0),                                // 正式起点: 基准=100, 首价=20
      fill(1100, Side.Long, 20.0, 2.0),
    ).foreach(rec.observe)
    assertEquals(rec.fillCount, 1)               // 预热那笔不计
    assertEquals(rec.fills.head._3, 20.0)        // 只剩正式区间那笔
    assertEquals(rec.baseEquityOr(0.0), 100.0)   // 基准 = 正式区间首个 equity

  test("buy&hold / 最大回撤 / 收益 计算正确"):
    val t0 = 1_700_000_000_000L // 采样按 (ts - lastSampleTs >= 间隔) 节流, 用真实 epoch-ms (首样必采)
    val rec = BacktestRecorder(ex, sym, startMs = t0, initialBalance = 100.0, sampleIntervalMs = H)
    Seq(
      trade(t0, 100.0), acct(t0, 1000.0),                  // 基准净值 1000, 首价 100
      trade(t0 + H, 120.0), acct(t0 + H, 1200.0),          // 峰值 1200
      trade(t0 + 2 * H, 80.0), acct(t0 + 2 * H, 600.0),    // 回撤 = (1200-600)/1200 = 50%
    ).foreach(rec.observe)
    assertEqualsDouble(rec.buyHoldRetPct, -20.0, 1e-9)  // 末价80 vs 首价100 = -20%
    assertEqualsDouble(rec.maxDrawdown, 0.5, 1e-9)
    assertEquals(rec.equitySamples.size, 3)

  test("成交点买卖向: Long=买 Short=卖"):
    val rec = BacktestRecorder(ex, sym, startMs = 0L, initialBalance = 100.0)
    Seq(fill(10, Side.Long, 50.0, 1.0), fill(20, Side.Short, 60.0, 1.0)).foreach(rec.observe)
    val fs = rec.fills
    assertEquals(fs.map(_._2), Vector(Side.Long, Side.Short))
    assertEquals(fs.map(_._3), Vector(50.0, 60.0))

  test("EquityChartHtml markers 渲染: 买=绿↑多边形 卖=红↓多边形, 含图例"):
    import strategy.utils.viz.EquityChartHtml
    val html = EquityChartHtml.render(
      title = "t",
      ts = Vector(0L, H, 2 * H),
      lines = Seq(EquityChartHtml.Line("p", "#000", Vector(1.0, 1.2, 0.8))),
      posDir = Vector(1, 1, -1),
      stats = Nil,
      markers = Seq(EquityChartHtml.Marker(0L, buy = true, 1.0), EquityChartHtml.Marker(H, buy = false, 1.2)),
    )
    assert(html.contains("<polygon"), "应渲染成交点多边形")
    assert(html.contains("#16a34a"), "买点绿色")
    assert(html.contains("#dc2626"), "卖点红色")
    assert(html.contains("▲买"), "副标题含买卖图例")
