package hft.demo

import hft.backtest.{BacktestEngine, BinanceDataKind, BinanceHistory}
import hft.domain.Exchange
import hft.engine.StrategyRunner
import hft.messaging.{EventData, IncomeEvent}
import hft.sim.SimConfig
import hft.strategy.MaMacdGridStrategy
import sttp.client4.DefaultSyncBackend

import java.time.LocalDate
import java.util.concurrent.Executors
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

/** 均线 + MACD 方向性网格策略回测 (含系数扫描)。
  *
  * 对 [[MaMacdGridStrategy]] 在给定 symbol / 区间上扫描 (smallSpacing × largeSpacing) 网格档距，
  * 输出每组系数的成交数 / 已实现盈亏 / 末净值 / 收益率，并与买入持有 (buy&hold) 对比。
  *
  * 运行:
  *   sbt "runMain hft.demo.MaMacdGridBacktest [SYMBOL] [START] [END] [baseQty] [maxPos]"
  * 例:
  *   sbt "runMain hft.demo.MaMacdGridBacktest ETHUSDT 2026-01-10 2026-06-16 0.5 10"
  */
@main def MaMacdGridBacktest(args: String*): Unit =
  System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn")

  val symbol = args.lift(0).getOrElse("ETHUSDT")
  val start = LocalDate.parse(args.lift(1).getOrElse("2026-01-10"))
  val end = LocalDate.parse(args.lift(2).getOrElse("2026-06-16"))
  val baseQty = args.lift(3).map(_.toDouble).getOrElse(0.5)
  val maxPos = args.lift(4).map(_.toDouble).getOrElse(10.0)

  val initBalance = 100_000.0
  val makerFee = 0.0002
  val takerFee = 0.0005

  val backend = DefaultSyncBackend()
  val publicClient = hft.exchange.binance.BinanceClient(backend, credentials = None)
  val symbolMetas = publicClient
    .fetchAllSymbolMetas()
    .fold(e => sys.error(s"fetch symbol metas failed: ${e.message}"), _.map(m => (m.exchange, m.symbol) -> m).toMap)

  // 扫描网格：顺势加仓档距 (小) × 平仓档距 (大)
  val smallGrid = Seq(0.002, 0.003, 0.004)
  val largeGrid = Seq(0.025, 0.040, 0.060)

  final case class Row(small: Double, large: Double, fills: Int, realized: Double, equity: Double, posCoin: Double)

  def runOne(small: Double, large: Double): (Row, (Double, Double)) =
    // 每组系数重建数据源 (命中本地缓存)；b&h 价格用观察者抓首/末成交价
    val source = BinanceHistory.source(backend, Seq(symbol), start, end, kinds = Seq(BinanceDataKind.Trades))
    val strategy = MaMacdGridStrategy(
      exchange = Exchange.Binance,
      symbol = symbol,
      smallSpacing = small,
      largeSpacing = large,
      baseQty = baseQty,
      maxPositionCoin = maxPos,
    )
    val runner = StrategyRunner(strategy, symbolMetas)
    var firstPx = 0.0
    var lastPx = 0.0
    val obs: IncomeEvent => Unit = ev =>
      ev.data match
        case EventData.MarketTradeUpdate(t) =>
          if firstPx == 0.0 then firstPx = t.price
          lastPx = t.price
        case _ => ()
    val engine = BacktestEngine(
      exchange = Exchange.Binance,
      source = source,
      runners = Seq(runner),
      config = SimConfig(
        exchangeToStrategyDelayMs = 100,
        orderToExchangeDelayMs = 50,
        initialBalanceUsdt = initBalance,
        makerFeeRate = makerFee,
        takerFeeRate = takerFee,
      ),
      observers = Seq(obs),
    )
    val r = engine.run()
    val posCoin = r.positions.map(_.size).sum
    (Row(small, large, r.fills, r.realizedPnl, r.finalEquity, posCoin), (firstPx, lastPx))

  println("==================== MA+MACD Grid Sweep ====================")
  println(s"symbol=$symbol  range=[$start .. $end]  baseQty=$baseQty maxPos=$maxPos")
  println(f"capital=$initBalance%.0f  fee maker=${makerFee * 100}%.3f%% taker=${takerFee * 100}%.3f%%")

  // 并行扫描各系数组合 (数据已缓存, 各线程独立读 zip)。需大堆: 每个 run 物化一日成交为 List。
  val combos = for small <- smallGrid; large <- largeGrid yield (small, large)
  val pool = Executors.newFixedThreadPool(math.min(combos.size, Runtime.getRuntime.availableProcessors))
  given ec: ExecutionContext = ExecutionContext.fromExecutor(pool)
  val results =
    try Await.result(Future.sequence(combos.map((s, l) => Future(runOne(s, l)))), Duration.Inf)
    finally pool.shutdown()
  val rows = results.map(_._1)
  val (firstPx, lastPx) = results.head._2

  println("-" * 78)
  println(f"${"small"}%7s ${"large"}%7s ${"fills"}%7s ${"realizedPnL"}%14s ${"finalEquity"}%14s ${"return%"}%9s ${"posCoin"}%9s")
  rows.sortBy(-_.equity).foreach { row =>
    val ret = (row.equity - initBalance) / initBalance * 100
    println(
      f"${row.small}%7.3f ${row.large}%7.3f ${row.fills}%7d ${row.realized}%+14.2f ${row.equity}%14.2f ${ret}%+8.2f%% ${row.posCoin}%+9.3f"
    )
  }
  println("-" * 78)
  val bhRet = if firstPx > 0 then (lastPx - firstPx) / firstPx * 100 else 0.0
  println(f"buy&hold: $symbol $firstPx%.2f -> $lastPx%.2f  (${bhRet}%+.2f%%)")
  val best = rows.maxBy(_.equity)
  println(f"best by finalEquity: small=${best.small}%.3f large=${best.large}%.3f -> equity=${best.equity}%.2f (${(best.equity - initBalance) / initBalance * 100}%+.2f%%)")
  println("============================================================")
