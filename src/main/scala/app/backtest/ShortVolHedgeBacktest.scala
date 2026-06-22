package app.backtest

import hft.backtest.BsGreeksConfig
import strategy.research.{AdaptiveBandScaler, DeltaKamaFilter, HedgeExecution, HedgeOverlay, KamaTrendOverlay, MacdBiasOverlay, RatchetSwitch, TargetDeltaHedgeStrategy}
import sttp.client4.DefaultSyncBackend

import java.nio.file.Path
import java.time.LocalDate

/** 卖方 (short-vol) + 方向性 **目标 delta** 对冲回测 ([[TargetDeltaHedgeStrategy]])。
  *
  * 场景：**卖出**一份 IV=50 的 ATM 跨式 ([[BsGreeksConfig.straddles]] 取负 = short call + short put)，
  * 收权利金、背 short gamma；用永续做**方向性 delta 对冲**——MACD 水上目标正 delta、水下目标负 delta，
  * 对冲阈值与目标偏移均按 |gamma|·S 动态计算 (随头寸/临期自适应)，限价单 3 秒未成交撤单重挂 (追价)。
  *
  * 数据：Binance ETHUSDT 永续历史 trades (trade-native，限价单走 [[hft.sim.SimState.matchTrade]] 撮合，
  * 无需 BBO)。期权希腊字母为 BS 合成 ([[BsGreeksSource]])，与实盘 OKX 同一 Greeks 通道。
  *
  * P&L = 期权腿 (short 跨式: 进场权利金 − 现价值，含 theta 收入) + 永续对冲腿 (含方向 overlay 损益 + 手续费)。
  * 限价对冲单成交为 **maker**，按 makerFeeRate 计费。
  *
  * 运行: sbt "runMain app.backtest.ShortVolHedgeBacktest [start] [end] [fee] [tilt] [band] [market|limit] [iv]
  *          [ratchet on|off] [adaptive on|off] [switch on|off] [straddles] [kama on|off]"
  * 缺省回测最近一周 (end = 今天-2, 留出 Binance Vision 数据上架延迟)。
  */
@main def ShortVolHedgeBacktest(args: String*): Unit =
  System.setProperty("org.slf4j.simpleLogger.showDateTime", "true")
  System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "HH:mm:ss.SSS")
  System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn") // 成交量大，压到 WARN 防刷屏

  // ==================== 配置 ====================
  val symbol = ShortVolHedgeRunner.Symbol
  // 跨式份数, 第 11 参：负=卖方(short), 正=买方(long)。默认 -10 (卖方)。用于买/卖零和对照
  val straddles = args.lift(10).map(_.toDouble).getOrElse(-10.0)

  val end = args.lift(1).map(LocalDate.parse).getOrElse(LocalDate.now().minusDays(2))
  val start = args.lift(0).map(LocalDate.parse).getOrElse(end.minusDays(6))
  // 手续费率, 第 3 参 (市价=taker 通常 0.0004~0.0005; 限价=maker 0.0002)。默认 0.0005 (taker)
  val feeRate = args.lift(2).map(_.toDouble).getOrElse(0.0005)
  // 目标 delta 偏移幅度, 第 4 参：overlay 的 tilt 系数。MACD: 0=纯中性。KAMA-趋势: 顺势超量幅度。默认 0
  val tilt = args.lift(3).map(_.toDouble).getOrElse(0.0)
  // 对冲带宽, 第 5 参 = |gamma|·S·band (价格约走该比例才对冲)。默认 0.01 (1%, 加大对冲间隔)
  val bandMoveRatio = args.lift(4).map(_.toDouble).getOrElse(0.01)
  // 执行方式, 第 6 参：market=市价(taker, 立即成交) / limit=限价 3s 追价(maker)。默认 market
  val useMarket = args.lift(5).map(_.toLowerCase).forall(_ != "limit")
  // 卖出 IV (年化), 第 7 参。卖方按此报价收权利金; 盈亏取决于它与事后实现波动 RV 的差 (VRP)。默认 0.5
  val impliedVol = args.lift(6).map(_.toDouble).getOrElse(0.5)
  // 价位棘轮, 第 8 参 (on|off)：同向对冲须越过上次对冲价才执行 (阈值逐渐加大)。默认 on
  val useRatchet = args.lift(7).map(_.toLowerCase).forall(_ != "off")
  // 方向自适应带, 第 9 参 (on|off)：对冲收紧(最小半带)、静默每分钟回升10%至初始。默认 off
  val useAdaptive = args.lift(8).map(_.toLowerCase).contains("on")
  // 棘轮开关, 第 10 参 (on|off)：大反弹(≥4%)临时关棘轮、再下杀(≥4%)重启并重置锚 (防高空坠落)。默认 off
  val useSwitch = args.lift(9).map(_.toLowerCase).contains("on")
  // netDelta 1min KAMA 滤波, 第 12 参 (on|off)：对冲触发改用平滑后的净 delta (震荡少来回、趋势照常)。默认 off
  val useKama = args.lift(11).map(_.toLowerCase).contains("on")
  // 方向 overlay, 第 13 参：macd=MACD 方向偏移 / kama=KAMA-趋势(ER 自适应带 + price-vs-KAMA 顺势超量)。默认 macd
  val useKamaTrend = args.lift(12).map(_.toLowerCase).contains("kama")
  // 对冲策略工厂 (每次产新实例)：TargetDeltaHedgeStrategy + 注入的 overlay/棘轮/自适应/滤波。
  // KAMA-趋势模式下 tilt=0 时用内置默认幅度 0.01 (顺势超量系数)。
  val strategyFactory: (hft.domain.Exchange, String, String, HedgeExecution) => hft.strategy.Strategy =
    (ex, sym, c, exec) =>
      val overlay: HedgeOverlay =
        if useKamaTrend then KamaTrendOverlay(tiltMoveRatio = if tilt > 0 then tilt else 0.01)
        else MacdBiasOverlay(tiltMoveRatio = tilt)
      TargetDeltaHedgeStrategy(
        exchange = ex,
        symbol = sym,
        ccy = c,
        execution = exec,
        overlay = overlay,
        bandMoveRatio = bandMoveRatio,
        ratchet = useRatchet,
        bandScaler = Option.when(useAdaptive)(AdaptiveBandScaler()), // floor0.5 / 收窄10%每次 / 回升10%每分
        ratchetSwitch = Option.when(useSwitch)(RatchetSwitch()),     // 反弹4%关 / 下杀4%重启
        deltaFilter = Option.when(useKama)(DeltaKamaFilter()),       // 1min bar / ER=10 / fast2 / slow30
      )

  val backend = DefaultSyncBackend()
  val symbolMetas = ShortVolHedgeRunner.fetchSymbolMetas(backend)

  val stamp = LocalDate.now().toString
  val params = ShortVolParams(
    start = start,
    end = end,
    strategyFactory = strategyFactory,
    feeRate = feeRate,
    useMarket = useMarket,
    impliedVol = impliedVol,
    straddles = straddles,
    recorderPath = Some(Path.of(s"backtest-fills-shortvol-$symbol-$stamp.csv")),
  )
  val r = ShortVolHedgeRunner.run(symbolMetas, ShortVolHedgeRunner.tradeSourceFactory(backend, start, end), params)

  val days = java.time.temporal.ChronoUnit.DAYS.between(start, end) + 1
  println("==================== ShortVol Hedge Backtest Result ====================")
  println(s"symbol         : $symbol  [$start .. $end] ($days days)")
  println(f"期权 (卖方)     : 起点ATM ${r.atmStrike}%.2f | IV=$impliedVol straddles=$straddles (short) tenor=回测周期(持有到期)")
  val execLabel = if useMarket then "市价(taker)" else "限价3s追价(maker)"
  val overlayLabel =
    if useKamaTrend then f"KAMA趋势(ER自适应带+price/KAMA顺势超量, tilt=${(if tilt > 0 then tilt else 0.01) * 100}%.2f%%)"
    else if tilt == 0.0 then "关(纯中性对冲)"
    else f"MACD bias·|gamma|·S·${tilt * 100}%.2f%%/级"
  println(f"对冲           : band=|gamma|·S·${bandMoveRatio * 100}%.2f%% | 方向overlay=$overlayLabel | 执行=$execLabel | 棘轮=${if useRatchet then "on" else "off"} | 自适应带=${if useAdaptive then "on" else "off"} | 棘轮开关=${if useSwitch then "on" else "off"} | KAMA滤波=${if useKama then "on" else "off"}")
  println(f"fee            : ${feeRate * 100}%.3f%%")
  println(f"start/end px    : ${r.atmStrike}%.2f -> ${r.lastPx}%.2f  (${r.netMovePct}%+.2f%%)")
  println(s"market events  : ${r.marketEvents}")
  println(s"fills          : ${r.fills}")
  println("-------------------- P&L 拆解 (USDT) --------------------")
  println(f"  期权腿 MTM   : ${r.optionPnl}%+.2f   (short 跨式 BS 估值变化, 含 theta 收入)")
  println(f"  永续对冲腿   : ${r.hedgePnl}%+.2f   (方向 overlay + 未实现 + 手续费)")
  println(f"  完整        : ${r.totalPnl}%+.2f   (期权腿 + 对冲腿)")
  println("---------------------------------------------------------")
  println(f"对冲腿 realized : ${r.realizedPnl}%.2f USDT (已扣手续费) | 末仓 ${r.endPositions}")
  println("========================================================================")
