package strategy.strategies.atrtakehedge.backtest

import hft.backtest.{BacktestEngine, BinanceDataKind, BinanceHistory, BsGreeksConfig, BsGreeksSource, TradePrintBboSource}
import hft.domain.*
import hft.engine.StrategyRunner
import hft.indicator.RealizedVol
import hft.messaging.{EventData, IncomeEvent}
import hft.option.BlackScholes
import hft.sim.SimConfig
import strategy.strategies.atrtakehedge.logic.AtrTakeHedgeStrategy
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
  * 运行: sbt "runMain strategy.strategies.atrtakehedge.backtest.AtrTakeHedgeBacktest [START] [END] [IV] [atrMult] [straddles]"
  */
@main def AtrTakeHedgeBacktest(args: String*): Unit =
  System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn")

  val symbol = "ETHUSDT"
  val ccy = "ETH"
  val start = LocalDate.parse(args.lift(0).getOrElse("2026-03-23"))
  val end = LocalDate.parse(args.lift(1).getOrElse("2026-05-23"))
  val atrMult = args.lift(3).map(_.toDouble).getOrElse(2.0)
  val straddles = args.lift(4).map(_.toDouble).getOrElse(10.0)
  val initialBalanceUsdt = 1_000_000.0
  val curveCsv = sys.env.getOrElse("PNL_CSV", "/tmp/atr_hedge_pnl.csv")

  val backend = DefaultSyncBackend()
  // 合约元数据手工构造 (避免联网取交易所信息；ETHUSDT 永续精度)
  val meta = SymbolMeta(Exchange.Binance, symbol, tickSize = 0.01, sizeStep = 0.001, minOrderSize = 0.001, contractSize = 1.0)
  val symbolMetas = Map((Exchange.Binance, symbol) -> meta)

  def tradeSource() = BinanceHistory.source(backend, Seq(symbol), start, end, kinds = Seq(BinanceDataKind.Trades))

  // ① 预扫一遍, 算本段实现波动 RV (小时采样, 年化), 并取首笔成交价为起点 ATM。
  println(s"computing realized vol over [$start .. $end] ...")
  val hourSamples = ArrayBuffer.empty[Double]
  var rvLastTs = 0L
  var atmStrike = 0.0
  locally {
    val it = tradeSource().events()
    while it.hasNext do
      it.next().data match
        case EventData.MarketTradeUpdate(t) =>
          if atmStrike == 0.0 then atmStrike = t.price
          if t.timestamp - rvLastTs >= 3_600_000L then { rvLastTs = t.timestamp; hourSamples += t.price }
        case _ => ()
  }
  if atmStrike == 0.0 then sys.error(s"no market data for $symbol [$start .. $end]")
  val rv = realizedVolHourly(hourSamples.toVector)
  // ② IV = RV (买入即按本段实现波动定价)；可由第 3 参显式覆盖
  val impliedVol = args.lift(2).map(_.toDouble).getOrElse(rv)
  println(f"realized vol (年化, 小时采样) = $rv%.4f  ->  IV = $impliedVol%.4f")

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

  // 观察者: 跟踪最新价 (期权腿 MTM) + 按小时记 PnL 曲线 (期权腿 / 对冲腿 / 完整)
  var lastMid = atmStrike; var lastTs = 0L
  var curveLastTs = 0L
  // 曲线点: (ts, price, optionPnl, hedgePnl, totalPnl)
  val curve = ArrayBuffer.empty[(Long, Double, Double, Double, Double)]
  val obs: IncomeEvent => Unit = ev =>
    ev.data match
      case EventData.BboUpdate(b) =>
        lastMid = b.midPrice; lastTs = b.timestamp
      case EventData.AccountInfoUpdate(_, info) =>
        // 账户净值 = 对冲腿; 期权腿用 BS 现值; 仅在已见行情 (源已初始化) 后按小时记点
        if lastTs > 0 && ev.exchangeTs - curveLastTs >= 3_600_000L then
          curveLastTs = ev.exchangeTs
          val optPnl = withGreeks.optionPnl(lastMid, lastTs)
          val hedge = info.equity - initialBalanceUsdt
          curve += ((ev.exchangeTs, lastMid, optPnl, hedge, optPnl + hedge))
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
  println(f"实现波动 RV(年化): $rv%.3f   IV=$impliedVol%.3f   ${if math.abs(rv - impliedVol) < 1e-9 then "(IV=RV -> 理论完整盈亏≈0)" else f"(IV≠RV)"}")
  println(s"market events  : ${result.marketEvents}    对冲成交 fills: ${result.fills}")
  println("-------------------- P&L 拆解 (USDT) --------------------")
  println(f"  期权腿 MTM   : $optionPnl%+.2f   (长跨式 BS 估值变化, 含 theta)")
  println(f"  永续对冲腿   : $hedgePnl%+.2f   (含未实现, fee=0)")
  println(f"  完整 (期权+对冲): $totalPnl%+.2f   (IV=RV 时应接近 0)")
  println(f"对冲腿 realized : ${result.realizedPnl}%+.2f | 末仓 ${result.positions}")

  // PnL 曲线落 CSV (小时采样): ts,date,price,optionPnl,hedgePnl,totalPnl
  val pw = java.io.PrintWriter(curveCsv)
  try
    pw.println("ts,date,price,optionPnl,hedgePnl,totalPnl")
    curve.foreach { case (ts, px, opt, hed, tot) =>
      val d = java.time.Instant.ofEpochMilli(ts).toString
      pw.println(f"$ts,$d,$px%.2f,$opt%.4f,$hed%.4f,$tot%.4f")
    }
  finally pw.close()
  println(s"PnL 曲线 (${curve.size} 点) -> $curveCsv")
  println("==================================================================================")
  backend.close()

/** 小时采样价格的年化实现波动 (对数收益, 对零求和口径) */
private def realizedVolHourly(samples: Vector[Double]): Double =
  RealizedVol.annualizedFromPrices(samples, BlackScholes.HoursPerYear)
