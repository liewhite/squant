package strategy.research

import hft.backtest.{BacktestEngine, BsGreeksConfig, BsGreeksSource, MarketDataSource}
import hft.domain.*
import hft.engine.StrategyRunner
import hft.messaging.{EventData, IncomeEvent}
import hft.option.{BlackScholes, OptionRight}
import hft.sim.SimConfig
import strategy.gammascalp.logic.GammaScalpStrategy
import strategy.utils.hedge.MacdBiasMode

/** 回测引擎 + 期权回测的**正确性验证** (非策略盈利性)。
  *
  * 三层验证, 全程零依赖 (合成 GBM 路径, 不联网)、手续费=0、延迟=0:
  *
  *   1. BlackScholes 希腊字母 = 价格的有限差分导数 (delta/gamma/vega/theta)，且满足 put-call 平价。
  *      —— 证明定价/希腊字母公式正确。
  *   2. BsGreeksSource 注入的账户级 Greeks = 直接 BS 计算 (含 theta/365、vega/100 单位换算)。
  *      —— 证明回测期权数据源聚合正确。
  *   3. 期权理论核心: 对 delta 对冲的跨式, **买方总盈亏关于 IV 单调递减、在 IV≈RV 处穿零**
  *      (实现波动 > 隐含波动时多 gamma 才赚)，且**卖方为买方的镜像** (符号相反)。
  *      用已知实现波动 RV 的合成路径, 在 IV ∈ {0.5,0.75,1,1.5,2}×RV 上跑买方/卖方,
  *      多种子蒙特卡洛取均值。—— 这是定价×希腊字母×撮合×账本×P&L 端到端联立正确的充要特征,
  *      任一环节有 bug (theta 符号、delta、成交价、盈亏记账) 都会破坏该单调穿零结构。
  *
  * 运行: sbt "runMain strategy.research.BacktestValidation [seeds] [days]"
  */
@main def BacktestValidation(args: String*): Unit =
  System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "error")
  val seeds = args.lift(0).map(_.toInt).getOrElse(24)
  val days = args.lift(1).map(_.toDouble).getOrElse(1.0)

  var pass = 0
  var fail = 0
  def check(name: String, ok: Boolean, detail: String = ""): Unit =
    if ok then { pass += 1; println(f"  [PASS] $name%-52s $detail") }
    else { fail += 1; println(f"  [FAIL] $name%-52s $detail") }

  // ==================================================================
  println("==================== 1. BlackScholes: 希腊字母=价格的有限差分导数 ====================")
  blackScholesFiniteDiff(check)

  println("\n==================== 2. BsGreeksSource: 注入 Greeks = 直接 BS ====================")
  greeksSourceConsistency(check)

  println("\n==================== 3. 期权回测端到端: 买方盈亏关于 IV 单调穿零 (IV≈RV) ====================")
  optionTheoryMonteCarlo(seeds, days, check)

  println("\n==================== 汇总 ====================")
  println(s"PASS=$pass  FAIL=$fail")
  if fail > 0 then sys.exit(1)

// ---------------------------------------------------------------------------
// 1. BS 希腊字母 = 价格的有限差分导数 + put-call 平价
// ---------------------------------------------------------------------------
private def blackScholesFiniteDiff(check: (String, Boolean, String) => Unit): Unit =
  import OptionRight.*
  val s = 2000.0; val k = 2000.0; val t = 0.5; val sig = 0.6; val r = 0.0
  def price(right: OptionRight, ss: Double = s, kk: Double = k, tt: Double = t, vv: Double = sig) =
    BlackScholes.greeks(right, ss, kk, tt, vv, r).price
  val g = BlackScholes.greeks(Call, s, k, t, sig, r)

  val hS = s * 1e-4
  val deltaFd = (price(Call, ss = s + hS) - price(Call, ss = s - hS)) / (2 * hS)
  val gammaFd = (price(Call, ss = s + hS) - 2 * g.price + price(Call, ss = s - hS)) / (hS * hS)
  val hV = 1e-5
  val vegaFd = (price(Call, vv = sig + hV) - price(Call, vv = sig - hV)) / (2 * hV)
  val hT = 1e-5
  val thetaFd = (price(Call, tt = t - hT) - price(Call, tt = t)) / hT // dP/d(calendar t) = -dP/dTau

  def rel(a: Double, b: Double): Double = math.abs(a - b) / math.max(math.abs(b), 1e-9)
  check("delta = ∂Price/∂S", rel(deltaFd, g.delta) < 1e-4, f"fd=$deltaFd%.6f bs=${g.delta}%.6f")
  check("gamma = ∂²Price/∂S²", rel(gammaFd, g.gamma) < 1e-3, f"fd=$gammaFd%.8f bs=${g.gamma}%.8f")
  check("vega  = ∂Price/∂σ", rel(vegaFd, g.vega) < 1e-4, f"fd=$vegaFd%.4f bs=${g.vega}%.4f")
  check("theta = ∂Price/∂t (每年)", rel(thetaFd, g.theta) < 1e-3, f"fd=$thetaFd%.4f bs=${g.theta}%.4f")

  val c = BlackScholes.greeks(Call, s, k, t, sig, 0.03)
  val p = BlackScholes.greeks(Put, s, k, t, sig, 0.03)
  check("put-call 平价 C−P = S−K·e^(−rT)", math.abs((c.price - p.price) - (s - k * math.exp(-0.03 * t))) < 1e-6, "")
  check("delta 平价 Δc−Δp = 1", math.abs((c.delta - p.delta) - 1.0) < 1e-9, "")

// ---------------------------------------------------------------------------
// 2. BsGreeksSource 注入的账户级 Greeks 必须等于直接 BS 计算 (含单位换算)
// ---------------------------------------------------------------------------
private def greeksSourceConsistency(check: (String, Boolean, String) => Unit): Unit =
  val ex = Exchange.Binance; val sym = "ETHUSDT"; val ccy = "ETH"
  val s0 = 2000.0; val iv = 0.6; val n = 3.0
  val t0 = 1_700_000_000_000L
  val expiry = t0 + (30L * 86_400_000L) // 30 天后到期
  // 两笔成交: 首笔设行权=s0, 第二笔价格变动以检验 greeks 随 S 变化
  val trades = Vector(t0 -> s0, (t0 + 5000L) -> (s0 * 1.01))
  val src = BsGreeksSource(
    fixedTradeSource(ex, sym, trades),
    BsGreeksConfig(ex, ccy, sym, straddles = n, impliedVol = iv, expiry = expiry, emitIntervalMs = 1L),
  )
  val emitted = src.events().collect { case IncomeEvent(_, _, EventData.GreeksUpdate(g)) => g }.toVector
  check("源发出 Greeks 事件", emitted.nonEmpty, s"count=${emitted.size}")

  // 取第二笔 (S=s0·1.01) 对应的 greeks, 与直接 BS (ATM 跨式, 行权=s0) 对照
  val g = emitted.last
  val s = s0 * 1.01
  val tY = (expiry - (t0 + 5000L)) / BlackScholes.MillisPerYear
  val call = BlackScholes.greeks(OptionRight.Call, s, s0, tY, iv, 0.0)
  val put = BlackScholes.greeks(OptionRight.Put, s, s0, tY, iv, 0.0)
  def near(a: Double, b: Double, eps: Double) = math.abs(a - b) < eps
  check("源 delta = N·(Δcall+Δput)", near(g.delta, n * (call.delta + put.delta), 1e-9), f"${g.delta}%.6f")
  check("源 gamma = N·(Γcall+Γput)", near(g.gamma, n * (call.gamma + put.gamma), 1e-12), f"${g.gamma}%.8f")
  check("源 theta = N·θ/365 (每日)", near(g.theta, n * (call.theta + put.theta) / 365.0, 1e-9), f"${g.theta}%.6f")
  check("源 vega = N·vega/100 (对1%)", near(g.vega, n * (call.vega + put.vega) / 100.0, 1e-9), f"${g.vega}%.6f")

// ---------------------------------------------------------------------------
// 3. 期权理论端到端: 合成已知 RV 的路径, 买方/卖方在不同 IV 上 delta 对冲, 看总盈亏单调穿零
// ---------------------------------------------------------------------------
private def optionTheoryMonteCarlo(seeds: Int, days: Double, check: (String, Boolean, String) => Unit): Unit =
  val ex = Exchange.Binance; val sym = "ETHUSDT"; val ccy = "ETH"
  val s0 = 2000.0; val sigma = 0.8 // GBM 年化波动
  val stepMs = 5000L
  val t0 = 1_700_000_000_000L
  val ivMults = Vector(0.5, 0.75, 1.0, 1.5, 2.0)
  val straddleN = 1.0
  val expiry = t0 + (14L * 86_400_000L) // 14 天后到期 (回测仅取其中 days 天)

  // 对每个 IV 倍数累计买方/卖方总盈亏的均值 (跨种子)
  val buyerByMult = Array.fill(ivMults.size)(0.0)
  val sellerByMult = Array.fill(ivMults.size)(0.0)
  var rvSum = 0.0
  var fillsSum = 0L

  for seed <- 0 until seeds do
    val path = gbmPath(seed, s0, sigma, days, stepMs, t0)
    rvSum += realizedVolAnnualized(path)
    val rv = realizedVolAnnualized(path)
    val trades = path
    ivMults.zipWithIndex.foreach { (mult, i) =>
      val iv = rv * mult
      val (buyerTotal, fills) = runHedgedStraddle(ex, sym, ccy, trades, expiry, iv, straddleN)
      val (sellerTotal, _) = runHedgedStraddle(ex, sym, ccy, trades, expiry, iv, -straddleN)
      buyerByMult(i) += buyerTotal
      sellerByMult(i) += sellerTotal
      if i == 2 then fillsSum += fills // IV=RV 档记录成交量, 确认确有对冲发生
    }

  val rvAvg = rvSum / seeds
  ivMults.indices.foreach(i => { buyerByMult(i) /= seeds; sellerByMult(i) /= seeds })

  // 参考权利金 (IV=RV 时一份 ATM 跨式入场价值, tenor=14d), 用以衡量 "IV=RV 处盈亏≈0" 的相对大小
  val tY0 = 14.0 / 365.0
  val premium = math.abs(straddleN) *
    (BlackScholes.greeks(OptionRight.Call, s0, s0, tY0, rvAvg, 0.0).price
      + BlackScholes.greeks(OptionRight.Put, s0, s0, tY0, rvAvg, 0.0).price)

  println(f"  合成 GBM: σ=$sigma  实测 RV(年化)≈$rvAvg%.3f  种子=$seeds  天=$days  参考权利金≈$premium%.1f  IV=RV档总成交≈${fillsSum / seeds}")
  println(f"  ${"IV/RV"}%8s ${"IV"}%8s ${"买方总盈亏"}%14s ${"占权利金"}%9s ${"卖方总盈亏"}%14s")
  ivMults.zipWithIndex.foreach { (m, i) =>
    println(f"  ${m}%8.2f ${rvAvg * m}%8.3f ${buyerByMult(i)}%+14.2f ${buyerByMult(i) / premium * 100}%+8.1f%% ${sellerByMult(i)}%+14.2f")
  }

  // --- 理论判据 ---
  val buyer = buyerByMult.toVector
  val seller = sellerByMult.toVector
  // (a) 买方总盈亏关于 IV 单调递减 (IV 越高、买方越亏)
  val monotone = buyer.sliding(2).forall(w => w(0) > w(1))
  check("买方总盈亏关于 IV 单调递减", monotone, buyer.map(v => f"$v%+.1f").mkString(" > "))
  // (b) 低 IV (0.5×RV) 买方盈利、高 IV (2×RV) 买方亏损 -> 在 IV≈RV 穿零
  check("IV=0.5·RV 买方盈利 (RV>IV, 多 gamma 赚)", buyer.head > 0, f"${buyer.head}%+.2f")
  check("IV=2·RV 买方亏损 (RV<IV, 多 gamma 亏)", buyer.last < 0, f"${buyer.last}%+.2f")
  // (c) 穿零点最接近 IV=RV (|买方盈亏| 在 mult=1 处最小)
  val minAbsIdx = buyer.indices.minBy(i => math.abs(buyer(i)))
  check("|买方盈亏| 最小处 ≈ IV=RV (mult=1.0)", ivMults(minAbsIdx) == 1.0, f"最小处 mult=${ivMults(minAbsIdx)}%.2f")
  // (d) IV=RV 处总盈亏 ≈ 0 (远小于权利金, 残差为离散对冲摩擦)
  val atRv = math.abs(buyer(2))
  check("IV=RV 处买方盈亏 ≈ 0 (<10% 权利金)", atRv < 0.10 * premium, f"|${buyer(2)}%.2f| = ${atRv / premium * 100}%.1f%% 权利金")
  // (e) 极端 IV 处买卖符号相反 (波动错价主导, 摩擦为二阶) -> 镜像
  val extremesOpposite = buyer.head * seller.head < 0 && buyer.last * seller.last < 0
  check("极端 IV 处买卖方向相反 (0.5×/2×RV 镜像)", extremesOpposite,
    f"0.5×: ${buyer.head}%+.1f/${seller.head}%+.1f  2×: ${buyer.last}%+.1f/${seller.last}%+.1f")
  // (f) 卖方关于 IV 单调递增 (与买方相反)
  check("卖方总盈亏关于 IV 单调递增", seller.sliding(2).forall(w => w(0) < w(1)), seller.map(v => f"$v%+.1f").mkString(" < "))

// 跑一次 delta 对冲跨式回测, 返回 (期权腿+对冲腿总盈亏, 成交数)。straddleN<0 即卖方。fee=0,delay=0。
private def runHedgedStraddle(
    ex: Exchange, sym: Symbol, ccy: String,
    trades: Vector[(Timestamp, Double)], expiry: Timestamp, iv: Double, straddleN: Double,
): (Double, Int) =
  val src = BsGreeksSource(
    fixedTradeSource(ex, sym, trades),
    BsGreeksConfig(ex, ccy, sym, straddles = straddleN, impliedVol = iv, expiry = expiry,
      emitIntervalMs = 5000L, minTenorDays = 1.0),
  )
  val strategy = GammaScalpStrategy(
    exchange = ex, symbol = sym, ccy = ccy,
    deltaBand = 0.02, baseOffsetRatio = 0.003, dirSkewRatio = 0.0, // 纯对称对冲 (无方向偏移)
    minHedgeQty = 0.001, biasMode = MacdBiasMode.Sign,
  )
  val meta = SymbolMeta(ex, sym, tickSize = 1e-4, sizeStep = 1e-4, minOrderSize = 1e-4, contractSize = 1.0)
  val runner = StrategyRunner.backtest(strategy, Map((ex, sym) -> meta))
  var lastPx = 0.0; var lastTs = 0L
  val obs: IncomeEvent => Unit = ev =>
    ev.data match { case EventData.MarketTradeUpdate(t) => lastPx = t.price; lastTs = t.timestamp; case _ => () }
  val engine = BacktestEngine(
    ex, src, Seq(runner),
    SimConfig(exchangeToStrategyDelayMs = 0, orderToExchangeDelayMs = 0, initialBalanceUsdt = 1_000_000.0,
      makerFeeRate = 0.0, takerFeeRate = 0.0),
    observers = Seq(obs),
  )
  val r = engine.run()
  val optionPnl = src.optionPnl(lastPx, lastTs)
  val hedgePnl = r.finalEquity - r.initialBalance
  (optionPnl + hedgePnl, r.fills)

// ---------------------------------------------------------------------------
// 合成数据辅助
// ---------------------------------------------------------------------------

/** 内存固定 trade 源 */
private def fixedTradeSource(ex: Exchange, sym: Symbol, trades: Vector[(Timestamp, Double)]): MarketDataSource =
  new MarketDataSource:
    def events(): Iterator[IncomeEvent] =
      trades.iterator.map { (ts, px) =>
        IncomeEvent(ts, ts, EventData.MarketTradeUpdate(MarketTrade(ex, sym, px, 1.0, isBuyerMaker = false, ts)))
      }

/** 零漂移几何布朗运动价格路径 (固定种子 -> 确定性)。 */
private def gbmPath(seed: Long, s0: Double, sigmaAnnual: Double, days: Double, stepMs: Long, t0: Timestamp): Vector[(Timestamp, Double)] =
  val rng = new java.util.Random(seed)
  val steps = (days * 86_400_000L / stepMs).toInt
  val dt = stepMs.toDouble / BlackScholes.MillisPerYear
  val drift = -0.5 * sigmaAnnual * sigmaAnnual * dt // 零漂移: E[S]=S0 (对数空间补偿)
  val diff = sigmaAnnual * math.sqrt(dt)
  val out = Array.ofDim[(Timestamp, Double)](steps + 1)
  var s = s0
  out(0) = (t0, s)
  var i = 1
  while i <= steps do
    s *= math.exp(drift + diff * rng.nextGaussian())
    out(i) = (t0 + i.toLong * stepMs, s)
    i += 1
  out.toVector

/** 路径年化实现波动 (对数收益): σ = sqrt(Σ r² / T_years)。 */
private def realizedVolAnnualized(path: Vector[(Timestamp, Double)]): Double =
  val rets = path.sliding(2).map { case Vector((_, a), (_, b)) => math.log(b / a) }.toVector
  val sumSq = rets.map(r => r * r).sum
  val tYears = (path.last._1 - path.head._1).toDouble / BlackScholes.MillisPerYear
  math.sqrt(sumSq / tYears)
