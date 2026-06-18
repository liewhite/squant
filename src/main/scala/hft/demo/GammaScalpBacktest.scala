package hft.demo

import hft.backtest.{BacktestEngine, BinanceHistory, BsGreeksConfig, BsGreeksSource}
import hft.domain.Exchange
import hft.engine.StrategyRunner
import hft.messaging.{EventData, IncomeEvent}
import hft.sim.{FillRecorder, SimConfig}
import hft.strategy.GammaScalpStrategy
import sttp.client4.DefaultSyncBackend

import java.nio.file.Path
import java.time.LocalDate

/** 纯 gamma scalping 回测入口 ([[GammaScalpStrategy]])。
  *
  * 数据：Binance ETHUSDT 永续历史 trades (2024 年中后无 bookTicker，用 [[TradePrintBboSource]]
  * 合成零价差 L1)。期权希腊字母为 **mock**：用 [[BsGreeksSource]] 以 Black-Scholes 合成一个
  * **长 ATM 跨式** (N 个 call + N 个 put，行权价取回测起点价) 的账户级 greeks，喂入与实盘 OKX
  * 同一 Greeks 通道——策略代码与实盘完全相同。
  *
  * 撮合按 **maker 手续费** 计 (对冲单全为 PostOnly)。回测 P&L = 永续对冲的 maker scalp 净收益
  * (毛 gamma 收益 - 手续费)；期权 theta/权利金为已知结构性成本，不在永续 P&L 内建模。
  *
  * 运行: sbt "runMain hft.demo.GammaScalpBacktest [START yyyy-MM-dd] [END yyyy-MM-dd]"
  * 缺省回测最近一周 (end = 今天-2, 留出 Binance Vision 数据上架延迟)。
  */
@main def GammaScalpBacktest(args: String*): Unit =
  System.setProperty("org.slf4j.simpleLogger.showDateTime", "true")
  System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "HH:mm:ss.SSS")
  System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn") // 成交量大，压到 WARN 防刷屏

  // ==================== 配置 ====================
  val symbol = "ETHUSDT"
  val ccy = "ETH"
  val impliedVol = 0.6        // 年化 IV (ETH 典型)
  val riskFreeRate = 0.0
  val tenorDays = 30.0        // 期权期限：临近到期滚动到新 ATM (gamma 全程存活)
  val straddles = 10.0        // 长跨式份数 (N 个 call + N 个 put)
  val deltaBand = 0.1         // 净 delta 对称容忍带 (ETH)
  val baseOffsetRatio = 0.002  // 基础对冲间距 0.2% (PostOnly 距最新成交价)
  val takerFeeRate = 0.0005
  val initialBalanceUsdt = 100_000.0

  val end = args.lift(1).map(LocalDate.parse).getOrElse(LocalDate.now().minusDays(2))
  val start = args.lift(0).map(LocalDate.parse).getOrElse(end.minusDays(6))
  // maker 手续费率 (gamma scalp 的核心成本)，第 3 参覆盖以做"毛/净"对照。0.0002 = 0.02%
  val makerFeeRate = args.lift(2).map(_.toDouble).getOrElse(0.0002)
  // 方向性间距偏移幅度，第 4 参覆盖以做 A/B：0=关(纯对称), 0.0005=开
  val dirSkewRatio = args.lift(3).map(_.toDouble).getOrElse(0.0005)
  // 方向模式，第 5 参：sign=只看柱符号(±1) / graded=颜色×趋势分级(±2，默认)
  val biasMode = args.lift(4).map(_.toLowerCase) match
    case Some("sign") => hft.strategy.MacdBiasMode.Sign
    case _            => hft.strategy.MacdBiasMode.Graded

  val backend = DefaultSyncBackend()
  val publicClient = hft.exchange.binance.BinanceClient(backend, credentials = None)
  val symbolMetas = publicClient
    .fetchAllSymbolMetas()
    .fold(e => sys.error(s"fetch symbol metas failed: ${e.message}"), _.map(m => (m.exchange, m.symbol) -> m).toMap)

  // 回测层负责原始数据下载/缓存/组装 (trade-native，不合成盘口)
  def tradeSource() = BinanceHistory.source(backend, Seq(symbol), start, end)

  // 以回测起点首个真实成交价作为 ATM 行权价 (peek 仅加载首日)
  val atmStrike = tradeSource()
    .events()
    .collectFirst { case IncomeEvent(_, _, EventData.MarketTradeUpdate(t)) => t.price }
    .getOrElse(sys.error(s"no market data for $symbol [$start .. $end] (数据未上架或日期无效?)"))

  // 滚动 ATM 长跨式：临近到期滚到新 ATM，gamma 全程存活；行权价由源在首笔成交时自设为当时价
  val greeksConfig = BsGreeksConfig(
    exchange = Exchange.Binance,
    ccy = ccy,
    underlyingSymbol = symbol,
    straddles = straddles,
    impliedVol = impliedVol,
    tenorDays = tenorDays,
    riskFreeRate = riskFreeRate,
    spotHolding = 0.0,        // 纯期权 + 永续对冲，无现货
    emitIntervalMs = 1000,    // 对齐实盘 OKX greeks 轮询节奏
  )
  val source = BsGreeksSource(tradeSource(), greeksConfig)

  val strategy = GammaScalpStrategy(
    exchange = Exchange.Binance,
    symbol = symbol,
    ccy = ccy,
    deltaBand = deltaBand,
    baseOffsetRatio = baseOffsetRatio,
    dirSkewRatio = dirSkewRatio,
    biasMode = biasMode,
  )
  val runner = StrategyRunner(strategy, symbolMetas)

  val stamp = LocalDate.now().toString
  val recorder = FillRecorder(Path.of(s"backtest-fills-gammascalp-$symbol-$stamp.csv"))
  recorder.open()
  // 旁路观察者: 跟踪最新标的中间价与时间，用于回测末期期权腿 MTM 估值
  var lastMid = atmStrike
  var lastTs = 0L
  val priceObserver: IncomeEvent => Unit = ev =>
    ev.data match
      case EventData.MarketTradeUpdate(t) => lastMid = t.price; lastTs = t.timestamp
      case _                              => ()
  try
    val engine = BacktestEngine(
      exchange = Exchange.Binance,
      source = source,
      runners = Seq(runner),
      config = SimConfig(
        exchangeToStrategyDelayMs = 100,
        orderToExchangeDelayMs = 50,
        initialBalanceUsdt = initialBalanceUsdt,
        makerFeeRate = makerFeeRate,
        takerFeeRate = takerFeeRate,
      ),
      observers = Seq(recorder.onEvent, priceObserver),
    )
    val result = engine.run()

    // 完整 gamma scalp P&L = 期权腿 (滚动跨式: 历次滚动已实现 + 末期未实现) + 永续对冲腿。
    // 仅看对冲腿在趋势行情里会严重误导 (对冲腿亏损由期权腿盈利对冲)。
    val optionPnl = source.optionRealizedPnl + source.optionUnrealized(lastMid, lastTs)
    val hedgePnl = result.finalEquity - result.initialBalance // 永续对冲腿 (含未实现 + 手续费)
    val totalPnl = hedgePnl + optionPnl // 完整: 期权腿 + 对冲腿 (含 theta/滚动, 已扣手续费)
    val days = java.time.temporal.ChronoUnit.DAYS.between(start, end) + 1
    println("==================== GammaScalp Backtest Result ====================")
    println(s"symbol         : $symbol  [$start .. $end] ($days days)")
    println(f"期权            : 起点ATM $atmStrike%.2f | IV=$impliedVol straddles=$straddles tenor=${tenorDays}%.0fd(滚动) band=$deltaBand ETH")
    println(f"对冲间距       : ${baseOffsetRatio * 100}%.2f%% ± ${dirSkewRatio * 100}%.2f%% (MACD 方向偏移, mode=$biasMode)")
    println(f"maker fee      : ${makerFeeRate * 100}%.3f%%  (PostOnly 对冲单)")
    println(f"start/end px    : $atmStrike%.2f -> $lastMid%.2f  (${(lastMid / atmStrike - 1) * 100}%+.2f%%)")
    println(s"market events  : ${result.marketEvents}")
    println(s"fills          : ${result.fills}")
    println("-------------------- P&L 拆解 (USDT) --------------------")
    println(f"  期权腿 MTM   : $optionPnl%+.2f   (长跨式 BS 估值变化, 含 theta)")
    println(f"  永续对冲腿   : $hedgePnl%+.2f   (含未实现 + 手续费)")
    println(f"  完整 gamma   : $totalPnl%+.2f   (期权腿 + 对冲腿)")
    println("---------------------------------------------------------")
    println(f"对冲腿 realized : ${result.realizedPnl}%.2f USDT (已扣手续费) | 末仓 ${result.positions}")
    println("====================================================================")
  finally recorder.close()
