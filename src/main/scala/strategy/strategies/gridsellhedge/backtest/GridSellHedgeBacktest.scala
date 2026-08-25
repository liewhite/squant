package strategy.strategies.gridsellhedge.backtest

import hft.backtest.MarketDataKind
import hft.backtest.binance.BinanceMarketDataProvider
import hft.domain.{Exchange, Side}
import hft.event.Topics
import strategy.strategies.gridsellhedge.logic.DynamicHedgeBand
import strategy.utils.viz.EquityChartHtml
import sttp.client4.DefaultSyncBackend

import java.time.LocalDate

/** **ETH 期权 网格卖方 + 动态阈值备兑对冲** 回测启动器。
  *
  * 规则 1 — 网格卖出 ([[strategy.strategies.gridsellhedge.logic.OptionGrid]]):
  *   卖 3 天到期期权; 预热后种下当前价上下最近 100 整数档的跨式 (上 call/下 put); 价每越过一条 100 网格线,
  *   顺势在前方一格卖出下一档 (上行卖 call、下行卖 put), 且该档无持仓才卖。到期按内在价值结算、释放该档。
  *
  * 规则 2 — 动态阈值行权价备兑对冲 ([[strategy.strategies.gridsellhedge.logic.DynamicHedgeBand]]):
  *   每档独立, maker 挂单执行 (BBO 外 0.01%/撤单重挂)。**价越过行权价即 1:1 备兑** (短 call→买标的/短 put→卖标的),
  *   **价回落超过离场阈值 (d ≤ −threshold) 即平掉对冲**; 每次越过行权价把离场阈值 ×1.2, 未触发时每小时 ×0.9 直到回到初值 0.4%。
  *
  * IV = 过去 [[GridConfig.rvWindowHours]] 小时的已实现波动率 (滞后, 无前视), 卖出时固定给该期权定价。
  *
  * 运行: sbt "runMain strategy.strategies.gridsellhedge.backtest.GridSellHedgeBacktest"
  * env: GSH_START / GSH_END (yyyy-MM-dd) / GSH_SPACING / GSH_TENOR_DAYS / GSH_NODE_CONTRACTS / GSH_RV_HOURS /
  *      GSH_POLL_MS / GSH_MAKER_OFFSET / GSH_OPT_FEE / GSH_PERP_FEE / GSH_SLIP / GSH_INIT_THRESH / GSH_EXPAND /
  *      GSH_DECAY / DATA_CACHE
  */
@main def GridSellHedgeBacktest(args: String*): Unit =
  System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn")

  val symbol = "ETHUSDT"
  val cacheDir = sys.env.getOrElse("DATA_CACHE", "data-cache")
  val start = LocalDate.parse(sys.env.getOrElse("GSH_START", "2025-04-10"))
  val end = LocalDate.parse(sys.env.getOrElse("GSH_END", "2026-06-16"))

  def envD(k: String, d: Double): Double = sys.env.get(k).map(_.toDouble).getOrElse(d)
  def envI(k: String, d: Int): Int = sys.env.get(k).map(_.toInt).getOrElse(d)

  val rvHours = envI("GSH_RV_HOURS", 72)
  val band = DynamicHedgeBand.Params(
    initialThreshold = envD("GSH_INIT_THRESH", 0.004),
    expandFactor = envD("GSH_EXPAND", 1.2),
    decayFactor = envD("GSH_DECAY", 0.9),
  )
  val config = GridConfig(
    spacing = envD("GSH_SPACING", 100.0),
    tenorDays = envD("GSH_TENOR_DAYS", 30.0),
    nodeContracts = envD("GSH_NODE_CONTRACTS", 1.0),
    rvWindowHours = rvHours,
    warmupHours = rvHours,
    pollIntervalMs = sys.env.get("GSH_POLL_MS").map(_.toLong).getOrElse(3000L),
    makerOffsetPct = envD("GSH_MAKER_OFFSET", 0.0001),
    optFeeRate = envD("GSH_OPT_FEE", 0.0003),
    perpFeeRate = envD("GSH_PERP_FEE", 0.0002),
    slippagePct = envD("GSH_SLIP", 0.0),
    band = band,
  )

  println("============ ETH 网格卖方 + 动态阈值备兑对冲 回测 (3s 轮询 / maker 对冲) ============")
  println(f"symbol=$symbol  区间=$start..$end  网格=${config.spacing}%.0f  tenor=${config.tenorDays}%.1fd  每档=${config.nodeContracts}%.2f张  轮询=${config.pollIntervalMs}ms")
  println(f"IV=过去${config.rvWindowHours}h已实现波动  对冲: 越过行权价即开, 回落超过离场阈值即平  离场阈值初始=${band.initialThreshold * 100}%.2f%%×${band.expandFactor}%.2f/次 每小时衰减×${band.decayFactor}%.2f")
  println(f"对冲: BBO外${config.makerOffsetPct * 100}%.3f%%挂maker/撤单重挂  费率: 期权=${config.optFeeRate * 100}%.3f%%(名义) 对冲=${config.perpFeeRate * 100}%.3f%%(maker) 滑点=${config.slippagePct * 100}%.3f%%")

  val sim = GridSellHedgeSim(config)
  val backend = DefaultSyncBackend()
  val t0 = System.nanoTime()
  var n = 0L
  try
    val it = BinanceMarketDataProvider(backend, cacheDir)
      .source(Seq(symbol), start, end, Set(MarketDataKind.Trades))
      .events()
    while it.hasNext do
      it.next().as(Topics.Trade).filter(_.symbol == symbol).foreach { t =>
        sim.onPrice(t.price, t.timestamp)
        n += 1
      }
  finally backend.close()

  report(symbol, config, sim, start, end, n, (System.nanoTime() - t0) / 1e9)

/** 控制台摘要 + 三件套 (equity CSV / fills CSV / 净值曲线网页)。 */
private def report(
    symbol: String,
    config: GridConfig,
    sim: GridSellHedgeSim,
    start: LocalDate,
    end: LocalDate,
    trades: Long,
    secs: Double,
): Unit =
  val st = sim.stats
  val samples = sim.equitySamples
  val base = config.initialBalance
  val finalEq = sim.finalEquity
  val retPct = (finalEq - base) / base * 100.0
  val firstPx = sim.firstPrice
  val lastPx = sim.lastPrice
  val bhRetPct = if firstPx > 0 then (lastPx - firstPx) / firstPx * 100.0 else 0.0

  var peak = base; var maxDd = 0.0
  samples.foreach { case (_, eq, _, _) => peak = math.max(peak, eq); if peak > 0 then maxDd = math.max(maxDd, (peak - eq) / peak) }

  println("\n============================ 结果 ============================")
  println(f"处理成交=$trades  净值采样=${samples.size}  用时 $secs%.1fs")
  println(f"期权: 卖出=${st.sold} 到期=${st.expired} 期末未平=${st.openPositions}   对冲: 挂单=${st.ordersPlaced} 成交开=${st.hedgeOpens} 成交平=${st.hedgeCloses}")
  println(f"期权腿已实现=${st.optRealized}%+.1f   对冲腿已实现=${st.hedgeRealized}%+.1f   费用=${st.feesPaid}%.1f   合计已实现=${st.totalRealized}%+.1f")
  println(f"末权益=$finalEq%.1f (含未实现)   策略收益=$retPct%+.2f%%   buy&hold=$bhRetPct%+.2f%%   超额=${retPct - bhRetPct}%+.2f%%   最大回撤=${maxDd * 100}%.2f%%")

  // ---- 落盘 ----
  val outDir = "docs/gridsellhedge"
  java.nio.file.Files.createDirectories(java.nio.file.Path.of(outDir))
  val label = s"gridsell_eth_${start}_${end}"

  writeCsv(s"$outDir/${label}_equity.csv", "ts,equity,price,netPos", samples.map { case (ts, eq, px, p) => f"$ts,$eq%.2f,$px%.2f,$p%.4f" })
  val fills = sim.fills
  writeCsv(s"$outDir/${label}_fills.csv", "ts,side,price,qty", fills.map { case (ts, side, px, q) => f"$ts,${side},$px%.2f,$q%.4f" })

  if samples.sizeIs >= 2 && firstPx > 0 then
    val ts = samples.map(_._1)
    val eqNorm = samples.map(_._2 / base)
    val bhNorm = samples.map(_._3 / firstPx)
    val posDir = samples.map(s => math.signum(s._4).toInt)
    // 对冲成交标记 (过多则等距抽样, 控制网页体积)
    val markStep = math.max(1, fills.size / 3000)
    val markers = fills.iterator.zipWithIndex.collect { case ((t, side, px, _), i) if i % markStep == 0 => EquityChartHtml.Marker(t, buy = side == Side.Long, value = px / firstPx) }.toVector
    EquityChartHtml.write(
      s"$outDir/$label.html",
      title = s"ETH 网格卖方+动态备兑对冲  $start..$end",
      ts = ts,
      lines = Seq(EquityChartHtml.Line("策略净值", "#2563eb", eqNorm), EquityChartHtml.Line("Buy&Hold", "#9ca3af", bhNorm)),
      posDir = posDir,
      stats = Seq(
        "标的" -> symbol,
        "策略收益" -> f"$retPct%+.2f%%",
        "Buy&Hold" -> f"$bhRetPct%+.2f%%",
        "最大回撤" -> f"${maxDd * 100}%.2f%%",
        "期权卖出/到期" -> s"${st.sold}/${st.expired}",
        "对冲开/平" -> s"${st.hedgeOpens}/${st.hedgeCloses}",
        "末权益" -> f"$finalEq%.0f",
      ),
      markers = markers,
    )
    println(s"\n网页=$outDir/$label.html  equity=$outDir/${label}_equity.csv  fills=$outDir/${label}_fills.csv")
  else println("\n(采样不足, 未出网页)")

private def writeCsv(path: String, header: String, rows: Seq[String]): Unit =
  val pw = java.io.PrintWriter(path)
  try { pw.println(header); rows.foreach(pw.println) }
  finally pw.close()
