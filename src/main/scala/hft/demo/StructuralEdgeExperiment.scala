package hft.demo

import hft.domain.{Exchange, Symbol}
import hft.strategy.Strategy
import strategy.research.{BreakoutHedgeStrategy, HedgeExecution, KamaTrendOverlay, MacdBiasOverlay, TargetDeltaHedgeStrategy}
import sttp.client4.DefaultSyncBackend

import java.time.LocalDate

/** **结构性优势探索**：在一个趋势周与一个震荡周上，对照若干**中性**(target=0) 对冲规则，
  * 隔离唯一真正的结构性杠杆——**对冲带宽如何随区制 (趋势/震荡) 伸缩**。
  *
  * 理论：连续 delta 对冲 short gamma 的损益 ≈ ½·Γ·S²·(σ_implied² − σ_realized²)·dt = VRP。
  * 期权腿 MTM 只取决于价路径与 IV，**与对冲规则无关** ([[ShortVolOutcome]] 的不变量)，故对冲规则之间
  * 的全部差异都落在**对冲腿** (gamma 滑点 + 手续费)。能改变对冲腿期望的只有两条结构性杠杆：
  *   1. **少付已实现方差**——震荡 (会均值回复) 时放宽带、趋势 (会延续) 时收紧带；
  *   2. **少付手续费**——对冲更少 (Leland/Whalley-Wilmott 最优带 ∝ (cost/Γ)^⅓)。
  * 方向性 overlay (顺势超量) 是**方向 beta 而非波动率 edge**，本实验一律 target=0 排除之。
  *
  * 输出每个 (周 × 规则) 的两腿拆解表。判据：**区制自适应带**能否在两个区制都不劣于固定带
  * (趋势里护住、震荡里少漏血)——若是则为真结构性优势，否则只是方差搬运/调参。
  *
  * 运行: sbt "runMain hft.demo.StructuralEdgeExperiment [fee] [market|limit]"
  */
@main def StructuralEdgeExperiment(args: String*): Unit =
  System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn")

  val feeRate = args.lift(0).map(_.toDouble).getOrElse(0.0005)
  val useMarket = args.lift(1).map(_.toLowerCase).forall(_ != "limit")

  // 数据驱动挑选的两个代表周 (相邻、同 vol 量级 ~50-70% 年化, 仅区制不同)：
  val weeks = Seq(
    ("震荡", LocalDate.parse("2026-03-02"), LocalDate.parse("2026-03-08")), // net≈0, ER 0.01, vol 71%: 低效来回
    ("趋势", LocalDate.parse("2026-03-10"), LocalDate.parse("2026-03-16")), // net +18%, ER 1.00, vol 50%: 干净单边
    ("急扭", LocalDate.parse("2026-02-21"), LocalDate.parse("2026-02-27")), // net -2%, ER 0.09, vol 100%, range 20%: 大幅反转 (宽带最坏情形)
  )

  type StratFactory = (Exchange, Symbol, String, HedgeExecution) => Strategy
  val Straddles = -10.0                              // 空 10 份跨式
  val OptionPositionEth = math.abs(Straddles)        // 突破阈值基数 (期权头寸 ETH 名义)
  val BaseBand = 0.010                               // 区制自适应规则的基准带宽

  // 一组**中性** (target=0) 对冲规则。固定带/ER 自适应带均走 TargetDelta (band=|gamma|·S·ratio·bandMult)；
  // 突破带走 BreakoutHedgeStrategy (5h Donchian 突破 -> 双 delta 阈值, 阈值=pct·期权头寸ETH)。
  def neutralFixed(band: Double): StratFactory =
    (ex, sym, c, exec) => TargetDeltaHedgeStrategy(ex, sym, c, exec, overlay = MacdBiasOverlay(tiltMoveRatio = 0.0), bandMoveRatio = band)
  val erAdaptive: StratFactory =
    (ex, sym, c, exec) => TargetDeltaHedgeStrategy(ex, sym, c, exec, overlay = KamaTrendOverlay(tiltMoveRatio = 0.0, bandChopMult = 3.0, bandTrendMult = 1.0), bandMoveRatio = BaseBand)
  def breakout(maxPct: Double, minPct: Double): StratFactory =
    (ex, sym, c, exec) => BreakoutHedgeStrategy(ex, sym, c, exec, optionPositionEth = OptionPositionEth, maxThresholdPct = Some(maxPct), minThresholdPct = minPct)

  // 固定带宽扫描 (映射"对冲频率↘ -> 成本"曲线) + ER 自适应参照 + 5h 突破双阈值。
  // 突破阈值按"占期权头寸 ETH 名义"计；因 ATM 美元 gamma(|gamma|·S)≈12×头寸, 故 pct 须放大到 ~10-40%
  // 才对应 ~1-3% 价带 (用户原始 3%/1% 等价 ~0.24% 价带、严重过对冲)。此处给三档由密到疏。
  val bandSweep = Seq(0.004, BaseBand, 0.020, 0.030, 0.050, 0.080)
  val fixedAndEr: Seq[(String, StratFactory)] =
    bandSweep.map(b => f"固定带 ${b * 100}%.1f%%" -> neutralFixed(b)) :+ ("区制带·ER自适应" -> erAdaptive)
  val breakoutRules: Seq[(String, StratFactory)] = Seq(
    "5h突破 max3%/min1%(原始)" -> breakout(maxPct = 0.03, minPct = 0.01),
    "5h突破 max15%/min6%" -> breakout(maxPct = 0.15, minPct = 0.06),
    "5h突破 max25%/min10%" -> breakout(maxPct = 0.25, minPct = 0.10),
    "5h突破 max40%/min20%" -> breakout(maxPct = 0.40, minPct = 0.20),
  )
  // 第 3 参 onlyBreakout=on -> 只跑突破规则 (固定带/ER 为确定性结果, 已知时可跳过省时)
  val onlyBreakout = args.lift(2).map(_.toLowerCase).contains("breakout")
  val rules: Seq[(String, StratFactory)] = if onlyBreakout then breakoutRules else fixedAndEr ++ breakoutRules

  val backend = DefaultSyncBackend()
  val symbolMetas = ShortVolHedgeRunner.fetchSymbolMetas(backend)

  // 收集 (周, 规则, outcome)
  val results =
    for (regime, start, end) <- weeks; (ruleName, factory) <- rules
    yield
      val params = ShortVolParams(
        start = start,
        end = end,
        strategyFactory = factory,
        feeRate = feeRate,
        useMarket = useMarket,
        straddles = Straddles,
      )
      val src = ShortVolHedgeRunner.tradeSourceFactory(backend, start, end)
      val out = ShortVolHedgeRunner.run(symbolMetas, src, params)
      (regime, start, end, ruleName, out)

  // ==================== 报表 ====================
  println()
  println("==================== 结构性优势实验: 中性对冲规则对照 ====================")
  println(f"fee=${feeRate * 100}%.3f%%  执行=${if useMarket then "市价(taker)" else "限价(maker)"}  | 期权腿 MTM 与对冲规则无关 (同周应相等, 可作一致性校验)")
  println()
  // 按 weeks 的规范顺序逐周出表；周身份统一用 (start,end) 匹配 (避免仅按 start 匹配的脆弱耦合)
  for (regime, start, end) <- weeks do
    val weekResults = results.filter(r => r._2 == start && r._3 == end)
    val sample = weekResults.head._5 // 同周 atmStrike/lastPx/optionPnl 为常量, 取首行即可
    println(f"【$regime%s 周】$start..$end  起点 ${sample.atmStrike}%.2f -> 末 ${sample.lastPx}%.2f (${sample.netMovePct}%+.2f%%)  期权腿(不变)≈${sample.optionPnl}%+.1f")
    println(f"  ${"规则"}%-22s${"对冲腿"}%12s${"完整PnL"}%12s${"fills"}%8s")
    for (_, _, _, ruleName, out) <- weekResults do
      println(f"  $ruleName%-20s${out.hedgePnl}%12.1f${out.totalPnl}%12.1f${out.fills}%8d")
    println()
  println("说明: '不对冲'即只持空跨式, 完整PnL=期权腿. 对冲腿恒为负=对冲成本 (gamma 滑点+手续费);")
  println("      区制带若两周对冲腿都不比最优固定带差 => 结构性优势; 否则只是在固定带之间插值.")
  println("=========================================================================")
