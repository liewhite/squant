package app.backtest

import hft.backtest.{BinanceDataKind, BinanceHistory}
import hft.indicator.{KlineSeries, RealizedVol}
import hft.messaging.EventData
import hft.option.BlackScholes
import sttp.client4.DefaultSyncBackend

import java.time.LocalDate

/** **已实现波动 RV 的频率结构** —— 验证"对冲频率失配"假说。
  *
  * 对同一价路径, 在不同采样频率 (1/5/15/30/60/240/1440 分钟) 各算一次年化 RV。理论:
  *   - 若**高频 RV 显著 > 低频 RV** -> 微观结构噪声 (买卖价跳动/Epps 效应) 主导:
  *     高频对冲 (band 死区) 会 realize 这层噪声方差 -> 空 gamma 系统性亏 (按小时 RV 定价时尤甚)。
  *   - 若**低频 RV > 高频 RV** -> 趋势 (正自相关) 主导: 持有越久 realize 越多 -> 少对冲 (裸卖) 反而吃方向。
  *
  * 这能直接量化"按小时 IV 定价、却在分钟级对冲"多付了多少方差, 解释逐月 IV=RV 下对冲仍稳定亏的来源
  * (是真实市场成本, 非回测 bug)。
  *
  * 运行: sbt "runMain app.backtest.RvTermStructure [start] [end]"
  */
@main def RvTermStructure(args: String*): Unit =
  System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn")

  val start = args.lift(0).map(LocalDate.parse).getOrElse(LocalDate.parse("2025-04-10"))
  val end = args.lift(1).map(LocalDate.parse).getOrElse(LocalDate.parse("2026-06-15"))
  val symbol = ShortVolHedgeRunner.Symbol

  // 一次扫描建 1 分钟 bar, 收集分钟收盘价
  println(s"扫描 1 分钟收盘 [$start .. $end] ...")
  val klines = new KlineSeries(60_000L, 24 * 60 * 500)
  val mins = scala.collection.mutable.ArrayBuffer.empty[Double]
  var lastOpen = -1L
  BinanceHistory.source(backend = DefaultSyncBackend(), symbols = Seq(symbol), start = start, end = end, kinds = Seq(BinanceDataKind.Trades))
    .events().foreach { ev =>
      ev.data match
        case EventData.MarketTradeUpdate(t) if t.symbol == symbol =>
          klines.update(t.timestamp, t.price, t.qty)
          klines.bars.lastOption.foreach { b => if b.openTime != lastOpen then { lastOpen = b.openTime; mins += b.close } }
        case _ => ()
    }
  val minCloses = mins.toVector
  println(s"分钟收盘 ${minCloses.size} 根")

  /** 每隔 stepMin 取一个收盘价, 算年化 RV */
  def annualizedRv(stepMin: Int): Double =
    val sampled = minCloses.indices.by(stepMin).map(minCloses).toVector
    // stepMin 分钟 bar 的年化基准 = 每年小时数 × 每小时分钟数 / stepMin
    RealizedVol.annualizedSampleStdFromPrices(sampled, BlackScholes.HoursPerYear * 60.0 / stepMin)

  val steps = Seq(1, 5, 15, 30, 60, 240, 1440)
  val rv60 = annualizedRv(60)
  println()
  println("==================== RV 频率结构 ====================")
  println(f"周期=[$start .. $end]  symbol=$symbol")
  println(f"${"采样"}%8s ${"年化RV"}%9s ${"相对1h"}%9s")
  steps.foreach { s =>
    val rv = annualizedRv(s)
    val label = if s >= 60 then s"${s / 60}h" else s"${s}min"
    println(f"$label%8s ${rv * 100}%8.1f%% ${if rv60 > 0 then rv / rv60 else 0.0}%8.2fx")
  }
  println("-" * 32)
  println("判读: 1min/5min RV 远高于 1h => 高频对冲 realize 的方差远超按小时 RV 收的权利金 (微观结构噪声);")
  println("      这部分超额方差 × ½ΓS² 即空 gamma 高频对冲的系统性亏损 (真实成本, 非回测 bug)。")
  println("=====================================================")
