package strategy.research

import hft.backtest.{BacktestEngine, BinanceDataKind, BinanceHistory}
import hft.domain.*
import hft.engine.StrategyRunner
import hft.exchange.SubscriptionKind
import hft.messaging.{EventData, IncomeEvent, StateManager}
import hft.sim.SimConfig
import hft.strategy.{OutcomeEvent, Strategy}
import strategy.mamacdgrid.logic.MaMacdGridStrategy
import sttp.client4.DefaultSyncBackend

import java.time.LocalDate

/** 回测性能基准：把总耗时拆成 解析/IO、引擎、策略 三段，定位瓶颈。
  *
  * 用本地缓存的真实 trades 数据 (不联网)，单线程跑，分三阶段计时：
  *   A. 仅遍历数据源 (解析 zip+CSV)            -> 解析/IO 成本
  *   B. 引擎 + 空策略 (订阅 trades, 不下单)     -> 引擎成本 (撮合/调度/状态)
  *   C. 引擎 + 真实 MaMacd 策略                 -> 总成本 (B 之上即策略成本)
  *
  * 运行: sbt "runMain strategy.research.BacktestPerf [SYMBOL] [START] [END] [CACHE_DIR]"
  */
@main def BacktestPerf(args: String*): Unit =
  System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn")
  val symbol = args.lift(0).getOrElse("ETHUSDT")
  val start = LocalDate.parse(args.lift(1).getOrElse("2025-04-10"))
  val end = LocalDate.parse(args.lift(2).getOrElse("2025-04-13"))
  val cacheDir = args.lift(3).getOrElse(sys.env.getOrElse("DATA_CACHE", "data-cache"))
  val backend = DefaultSyncBackend()

  val meta = SymbolMeta(Exchange.Binance, symbol, tickSize = 1e-5, sizeStep = 1.0, minOrderSize = 1.0, contractSize = 1.0)
  val metas = Map((Exchange.Binance, symbol) -> meta)

  def src() = BinanceHistory.source(backend, Seq(symbol), start, end, kinds = Seq(BinanceDataKind.Trades), cacheDir = cacheDir)
  def time[A](label: String)(body: => A): A =
    val t0 = System.nanoTime()
    val a = body
    val ms = (System.nanoTime() - t0) / 1e6
    println(f"$label%-32s ${ms / 1000.0}%8.2fs")
    a

  /** 空策略：订阅 trades, 永不下单 (隔离引擎成本)。 */
  final class NoOp extends Strategy:
    def publicStreams = Map(Exchange.Binance -> Set(SubscriptionKind.Trade(symbol)))
    def orderTimeoutMs = 5000L
    def onEvent(event: IncomeEvent, state: StateManager): Vector[OutcomeEvent] = Vector.empty

  def cfg = SimConfig(exchangeToStrategyDelayMs = 100, orderToExchangeDelayMs = 50, initialBalanceUsdt = 100_000.0, makerFeeRate = 0.0002, takerFeeRate = 0.0005)

  println(s"==== BacktestPerf $symbol [$start..$end] ====")
  // A: 解析/IO
  var n = 0L; var checksum = 0.0
  time("A. parse-only (source iterate)") {
    val it = src().events()
    while it.hasNext do
      val e = it.next()
      n += 1
      e.data match { case EventData.MarketTradeUpdate(t) => checksum += t.price; case _ => () }
  }
  println(f"   events=$n  checksum=$checksum%.2f")

  // B: 引擎 + 空策略
  time("B. engine + NoOp strategy") {
    val runner = StrategyRunner.backtest(NoOp(), metas)
    BacktestEngine(Exchange.Binance, src(), Seq(runner), cfg).run()
  }

  // C: 引擎 + 真实策略 (打印结果做 golden 对照, 验证优化不改变回测结果)
  val r = time("C. engine + MaMacd strategy") {
    val strat = MaMacdGridStrategy(Exchange.Binance, symbol, addSpacing = 0.004, largeSpacing = 0.05, exitSpacing = 0.004, baseQty = 100.0, maxPositionCoin = 5000.0)
    val runner = StrategyRunner.backtest(strat, metas)
    BacktestEngine(Exchange.Binance, src(), Seq(runner), cfg).run()
  }
  println(f"   GOLDEN fills=${r.fills} realizedPnl=${r.realizedPnl}%.6f finalEquity=${r.finalEquity}%.6f positions=${r.positions.map(p => f"${p.symbol}:${p.size}%.4f@${p.entryPrice}%.4f").mkString(",")}")
  backend.close()
