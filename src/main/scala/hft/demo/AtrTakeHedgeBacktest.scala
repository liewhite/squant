package hft.demo

import hft.backtest.{BacktestEngine, BinanceDataKind, BinanceHistory, BsGreeksConfig, BsGreeksSource, TradePrintBboSource}
import hft.domain.*
import hft.engine.StrategyRunner
import hft.messaging.{EventData, IncomeEvent}
import hft.option.BlackScholes
import hft.sim.SimConfig
import hft.strategy.AtrTakeHedgeStrategy
import sttp.client4.DefaultSyncBackend

import java.time.{LocalDate, ZoneOffset}
import scala.collection.mutable.ArrayBuffer

/** 买方 (long-gamma) ATR 通道 take 式对冲回测 ([[AtrTakeHedgeStrategy]])。
  *
  * 价格主导 (越 atrMult×ATR 即对冲)、delta 定量 (take 当前净 delta)、市价成交、成交后中心移到成交价。
  * 期权腿用 [[BsGreeksSource]] 合成长 ATM 跨式 (BS 估值, 含 theta)；对冲腿走撮合引擎。
  * 手续费=0、延迟=0 (简化, 聚焦对冲机制本身)。
  *
  * 数据链: 原始 trades -> BsGreeksSource(注入 greeks, 透传 trades) -> TradePrintBboSource(trades 合成零价差盘口),
  * 故期权源见 trades (定 greeks/行权)、策略见盘口 (market take)。
  *
  * 运行: sbt "runMain hft.demo.AtrTakeHedgeBacktest [START] [END] [IV] [atrMult] [straddles]"
  */
@main def AtrTakeHedgeBacktest(args: String*): Unit =
  System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn")

  val symbol = "ETHUSDT"
  val ccy = "ETH"
  val start = LocalDate.parse(args.lift(0).getOrElse("2025-05-12"))
  val end = LocalDate.parse(args.lift(1).getOrElse("2025-06-12"))
  val impliedVol = args.lift(2).map(_.toDouble).getOrElse(0.6)
  val atrMult = args.lift(3).map(_.toDouble).getOrElse(2.0)
  val straddles = args.lift(4).map(_.toDouble).getOrElse(10.0)
  val initialBalanceUsdt = 1_000_000.0

  val backend = DefaultSyncBackend()
  // 合约元数据手工构造 (避免联网取交易所信息；ETHUSDT 永续精度)
  val meta = SymbolMeta(Exchange.Binance, symbol, tickSize = 0.01, sizeStep = 0.001, minOrderSize = 0.001, contractSize = 1.0)
  val symbolMetas = Map((Exchange.Binance, symbol) -> meta)

  def tradeSource() = BinanceHistory.source(backend, Seq(symbol), start, end, kinds = Seq(BinanceDataKind.Trades))

  // 起点 ATM 行权 (仅用于展示；BsGreeksSource 内部首笔成交自设行权)
  val atmStrike = tradeSource()
    .events()
    .collectFirst { case IncomeEvent(_, _, EventData.MarketTradeUpdate(t)) => t.price }
    .getOrElse(sys.error(s"no market data for $symbol [$start .. $end]"))

  // 单只 ATM 长跨式, 到期 = 回测结束日 (全程持有不滚动)
  val expiryMs = end.plusDays(1).atStartOfDay.toInstant(ZoneOffset.UTC).toEpochMilli
  val greeksConfig = BsGreeksConfig(
    exchange = Exchange.Binance, ccy = ccy, underlyingSymbol = symbol,
    straddles = straddles, impliedVol = impliedVol, expiry = expiryMs,
    riskFreeRate = 0.0, spotHolding = 0.0, emitIntervalMs = 1000,
  )
  // 期权源见 trades; 再用 TradePrintBboSource 合成零价差盘口给策略 (market take)
  val withGreeks = BsGreeksSource(tradeSource(), greeksConfig)
  val source = TradePrintBboSource(withGreeks)

  val strategy = AtrTakeHedgeStrategy(Exchange.Binance, symbol, ccy, atrMult = atrMult)
  val runner = StrategyRunner.backtest(strategy, symbolMetas)

  // 观察者: 跟踪最新价 (期权腿 MTM) + 小时采样算实现波动 (与 IV 对照, 解读盈亏方向)
  var lastMid = atmStrike; var lastTs = 0L
  val hourlySamples = ArrayBuffer.empty[Double]
  var lastSampleTs = 0L
  val obs: IncomeEvent => Unit = ev =>
    ev.data match
      case EventData.BboUpdate(b) =>
        lastMid = b.midPrice; lastTs = b.timestamp
        if b.timestamp - lastSampleTs >= 3_600_000L then { lastSampleTs = b.timestamp; hourlySamples += b.midPrice }
      case _ => ()

  val engine = BacktestEngine(
    exchange = Exchange.Binance, source = source, runners = Seq(runner),
    config = SimConfig(
      exchangeToStrategyDelayMs = 0, orderToExchangeDelayMs = 0,
      initialBalanceUsdt = initialBalanceUsdt, makerFeeRate = 0.0, takerFeeRate = 0.0,
    ),
    observers = Seq(obs),
  )
  val result = engine.run()

  val optionPnl = withGreeks.optionPnl(lastMid, lastTs)
  val hedgePnl = result.finalEquity - result.initialBalance
  val totalPnl = optionPnl + hedgePnl
  val days = java.time.temporal.ChronoUnit.DAYS.between(start, end) + 1
  val rv = realizedVolHourly(hourlySamples.toVector)
  // 参考: 入场跨式权利金 (起点 ATM, IV, tenor=回测周期)
  val tY = days.toDouble / 365.0
  val call = BlackScholes.greeks(hft.option.OptionRight.Call, atmStrike, atmStrike, tY, impliedVol, 0.0).price
  val put = BlackScholes.greeks(hft.option.OptionRight.Put, atmStrike, atmStrike, tY, impliedVol, 0.0).price
  val premium = straddles * (call + put)

  println("==================== ATR-Take Hedge (买方 long-gamma) Backtest ====================")
  println(s"symbol         : $symbol  [$start .. $end] ($days days)  fee=0 delay=0")
  println(f"期权            : 起点ATM $atmStrike%.2f | IV=$impliedVol straddles=$straddles tenor=持有到期 | 入场权利金≈$premium%.0f")
  println(f"对冲            : 价格越 ${atrMult}%.1f×ATR(14,1h) 即 take 当前净 delta (市价), 成交后中心重置")
  println(f"start/end px    : $atmStrike%.2f -> $lastMid%.2f  (${(lastMid / atmStrike - 1) * 100}%+.2f%%)")
  println(f"实现波动 RV(年化): $rv%.3f   vs IV=$impliedVol%.3f   -> ${if rv > impliedVol then "RV>IV (买方应占优)" else "RV<IV (买方应吃亏)"}")
  println(s"market events  : ${result.marketEvents}    对冲成交 fills: ${result.fills}")
  println("-------------------- P&L 拆解 (USDT) --------------------")
  println(f"  期权腿 MTM   : $optionPnl%+.2f   (长跨式 BS 估值变化, 含 theta)")
  println(f"  永续对冲腿   : $hedgePnl%+.2f   (含未实现, fee=0)")
  println(f"  完整 (期权+对冲): $totalPnl%+.2f")
  println(f"对冲腿 realized : ${result.realizedPnl}%+.2f | 末仓 ${result.positions}")
  println("==================================================================================")
  backend.close()

/** 小时采样价格的年化实现波动 (对数收益) */
private def realizedVolHourly(samples: Vector[Double]): Double =
  if samples.sizeIs < 3 then 0.0
  else
    val rets = samples.sliding(2).map { case Vector(a, b) => math.log(b / a) }.toVector
    val sumSq = rets.map(r => r * r).sum
    val tYears = rets.size.toDouble / (365.0 * 24.0) // 每步 1 小时
    math.sqrt(sumSq / tYears)
