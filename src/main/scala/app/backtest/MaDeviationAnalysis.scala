package app.backtest

import hft.backtest.{BinanceDataKind, BinanceHistory}
import hft.indicator.{Atr, KlineSeries, Macd, Sma}
import hft.messaging.EventData
import sttp.client4.DefaultSyncBackend

import java.time.LocalDate

/** 历史"价偏离均线 / ATR"分布分析 —— 为 [[hft.strategy.MaMacdGridStrategy]] 方案2 标定阈值 N。
  *
  * 方案2: 当 |price − ma60| / ATR ≥ N 时认定趋势拉伸过度 (超买/超卖)，切移动止盈且停止加仓。
  * N 不能拍脑袋, 须由历史分布决定: 统计每根**已收盘**小时 bar 的 |close − ma| / atr, 给出分位数,
  * 取较高分位 (如 85~90%) 作 N —— 即"价格极少越过该拉伸度", 越过即均值回归概率高。
  *
  * 运行: sbt "runMain app.backtest.MaDeviationAnalysis [SYMBOL] [START] [END]"
  */
@main def MaDeviationAnalysis(args: String*): Unit =
  System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn")

  val symbol = args.lift(0).getOrElse("ETHUSDT")
  val start = LocalDate.parse(args.lift(1).getOrElse("2025-04-10"))
  val end = LocalDate.parse(args.lift(2).getOrElse("2026-06-15"))
  val maPeriod = 60
  val atrPeriod = 14

  val backend = DefaultSyncBackend()
  val source = BinanceHistory.source(backend, Seq(symbol), start, end, kinds = Seq(BinanceDataKind.Trades))

  val klines = new KlineSeries(3_600_000L, maPeriod + atrPeriod + 4) with Macd with Sma with Atr:
    override protected def smaPeriod: Int = maPeriod
    override protected def atrPeriod: Int = 14

  val devs = scala.collection.mutable.ArrayBuffer.empty[Double] // 每根已收盘 bar 的 |close-ma|/atr
  // bars 是有界 deque (满后 size 不再增长), 故以"最新已收盘 bar 的 openTime 是否变化"判定新收盘根
  var lastClosedOpen = -1L
  source.events().foreach { ev =>
    ev.data match
      case EventData.MarketTradeUpdate(t) if t.symbol == symbol =>
        klines.update(t.timestamp, t.price, t.qty)
        klines.bars.lastOption.foreach { b =>
          if b.openTime != lastClosedOpen then
            lastClosedOpen = b.openTime
            for ma <- klines.sma; a <- klines.atr if a > 0.0 do
              devs += math.abs(b.close - ma) / a
        }
      case _ => ()
  }

  val sorted = devs.sorted.toIndexedSeq
  def pct(p: Double): Double =
    if sorted.isEmpty then Double.NaN
    else sorted(math.min(sorted.size - 1, math.max(0, (p / 100.0 * sorted.size).toInt)))

  println("==================== |price-MA| / ATR 分布 ====================")
  println(s"symbol=$symbol range=[$start .. $end] ma=$maPeriod atr=$atrPeriod")
  println(s"已收盘 bar 样本数 = ${sorted.size}")
  if sorted.nonEmpty then
    println(f"min=${sorted.head}%.3f  max=${sorted.last}%.3f  mean=${sorted.sum / sorted.size}%.3f")
    Seq(50.0, 60.0, 70.0, 75.0, 80.0, 85.0, 90.0, 95.0, 99.0).foreach { p =>
      println(f"P$p%-5.0f = ${pct(p)}%.3f")
    }
    println("-" * 50)
    println(f"建议 N (P85) ≈ ${pct(85.0)}%.2f   保守 N (P90) ≈ ${pct(90.0)}%.2f")
  println("==============================================================")
