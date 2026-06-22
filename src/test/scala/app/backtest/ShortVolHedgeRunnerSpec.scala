package app.backtest

import hft.backtest.MarketDataSource
import hft.domain.*
import hft.messaging.{EventData, IncomeEvent}
import strategy.research.{MacdBiasOverlay, TargetDeltaHedgeStrategy}

import java.time.LocalDate

/** [[ShortVolHedgeRunner]] 核心不变量单测 (喂合成价路径, 不触网/不读盘)：
  *   - **期权腿 MTM 与对冲规则无关**：同一价路径下, 窄带 (频繁对冲) 与宽带 (几乎不对冲) 的 optionPnl 必相等。
  *     这是 [[StructuralEdgeExperiment]] 全部分析的前提 (对冲规则只改对冲腿)。
  *   - **拆解恒等式**：totalPnl == optionPnl + hedgePnl。
  *   - 窄带确实比宽带对冲更频繁 (fills 更多)。
  */
class ShortVolHedgeRunnerSpec extends munit.FunSuite:

  private val ex = Exchange.Binance
  private val sym = ShortVolHedgeRunner.Symbol

  private val metas: Map[(Exchange, Symbol), SymbolMeta] =
    Map((ex, sym) -> SymbolMeta(ex, sym, tickSize = 0.01, sizeStep = 0.001, minOrderSize = 0.001, contractSize = 1.0))

  /** 合成一条来回震荡的价路径 (2000 附近 ±3% 正弦), 每秒一笔, 跨数小时 -> 足以多次越带触发对冲。 */
  private val day0 = LocalDate.parse("2026-03-02")
  private val path: Vector[IncomeEvent] =
    val t0 = day0.atStartOfDay(java.time.ZoneOffset.UTC).toInstant.toEpochMilli
    (0 until 4 * 3600).map { i =>
      val ts = t0 + i.toLong * 1000L
      val px = 2000.0 * (1.0 + 0.03 * math.sin(i / 600.0))
      IncomeEvent(ts, ts, EventData.MarketTradeUpdate(MarketTrade(ex, sym, px, 1.0, isBuyerMaker = false, ts)))
    }.toVector

  private def source(): MarketDataSource = new MarketDataSource:
    def events(): Iterator[IncomeEvent] = path.iterator

  private def run(band: Double, useMarket: Boolean = true): ShortVolOutcome =
    val params = ShortVolParams(
      start = day0,
      end = day0,
      // 纯中性 (target=0) 固定带策略
      strategyFactory = (ex, sym, c, exec) => TargetDeltaHedgeStrategy(ex, sym, c, exec, overlay = MacdBiasOverlay(tiltMoveRatio = 0.0), bandMoveRatio = band),
      feeRate = 0.0005,
      useMarket = useMarket,
    )
    ShortVolHedgeRunner.run(metas, () => source(), params)

  test("期权腿 MTM 与对冲规则无关: 窄带 vs 宽带 optionPnl 相等"):
    val tight = run(0.001) // 频繁对冲
    val wide = run(0.5)    // 几乎不对冲
    assertEqualsDouble(tight.optionPnl, wide.optionPnl, 1e-6, "期权腿只取决于价路径与 IV, 不应随带宽变")
    assert(clue(tight.fills) > clue(wide.fills), "窄带应比宽带对冲更频繁")

  test("拆解恒等式: totalPnl == optionPnl + hedgePnl"):
    val r = run(0.01)
    assertEqualsDouble(r.totalPnl, r.optionPnl + r.hedgePnl, 1e-9)

  test("宽带几乎不对冲 -> 完整 PnL ≈ 纯空跨式 (期权腿)"):
    val wide = run(0.5)
    assertEquals(wide.fills, 0, "带宽 50% 时不应触发对冲")
    assertEqualsDouble(wide.totalPnl, wide.optionPnl, 1e-9, "无对冲 -> 完整=期权腿")

  test("限价(maker)分支: 拆解恒等式同样成立 + 期权腿与 market 分支相等"):
    val limit = run(0.01, useMarket = false)
    assertEqualsDouble(limit.totalPnl, limit.optionPnl + limit.hedgePnl, 1e-9)
    // 期权腿只取决于价路径与 IV, 与执行方式 (市价/限价) 也无关
    assertEqualsDouble(limit.optionPnl, run(0.01, useMarket = true).optionPnl, 1e-6, "期权腿与执行方式无关")
