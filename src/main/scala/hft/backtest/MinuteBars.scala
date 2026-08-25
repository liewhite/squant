package hft.backtest

import hft.domain.{Coin, Exchange, MarketTrade, Price, Symbol}
import hft.event.{AnyEvent, Event, Topics}

import java.io.PrintWriter
import scala.io.Source

/** 1 分钟 OHLCV bar，用于**快速回测回放**：把海量逐笔 trade 预聚合成分钟 bar 落盘，
  * 回放时每分钟只发 4 个合成 trade (O/H/L/C)，事件数降两三个数量级 → 参数扫描可行。
  *
  * 取舍：maker 成交按分钟 H/L 是否穿过挂单价近似 (标准回测假设, 略乐观)；指标用收盘价, 与逐笔几乎一致。
  * 仅供**相对比较** (框架/参数排序)；最终冠军仍以全 trades 回测复核。
  */
final case class MinuteBar(ts: Long, open: Double, high: Double, low: Double, close: Double, volume: Double)

object MinuteBars:

  /** 把数据源的逐笔 trade 流聚合成 1 分钟 OHLCV，写入 CSV (ts,o,h,l,c,v)。一次性慢操作 (与全回测同量级)。 */
  def aggregate(source: MarketDataSource, outPath: String): Long =
    val w = PrintWriter(outPath)
    w.println("ts,o,h,l,c,v")
    var bucket = -1L
    var o, h, l, c, v = 0.0
    var count = 0L
    def flush(): Unit =
      if bucket >= 0 then
        w.println(s"${bucket * 60000L},$o,$h,$l,$c,$v"); count += 1
    try
      source.events().foreach { ev =>
        ev.as(Topics.Trade).foreach { t =>
          val b = t.timestamp / 60000L
          if b != bucket then
            flush()
            bucket = b; o = t.price.value; h = t.price.value; l = t.price.value; c = t.price.value; v = t.qty.value
          else
            h = math.max(h, t.price.value); l = math.min(l, t.price.value); c = t.price.value; v += t.qty.value
        }
      }
      flush()
    finally w.close()
    count

  def load(path: String): Vector[MinuteBar] =
    val src = Source.fromFile(path)
    try
      src.getLines().drop(1).map { line =>
        val a = line.split(',')
        MinuteBar(a(0).toLong, a(1).toDouble, a(2).toDouble, a(3).toDouble, a(4).toDouble, a(5).toDouble)
      }.toVector
    finally src.close()

/** 分钟 bar 回放源：每根 bar 发 4 个合成 trade (O→近端极值→远端极值→C)，时间戳 +0/15/30/45s。
  * 价格走向先触及离开盘价更近的极值, 是单条最可能路径 (供 maker 撮合判定穿越)。 */
final class MinuteBarReplaySource(bars: Vector[MinuteBar], exchange: Exchange, symbol: Symbol) extends MarketDataSource:
  override def events(): Iterator[AnyEvent] =
    bars.iterator.flatMap { b =>
      val (first, second) = if math.abs(b.high - b.open) <= math.abs(b.open - b.low) then (b.high, b.low) else (b.low, b.high)
      val vq = b.volume / 4.0
      Seq((0L, b.open), (15000L, first), (30000L, second), (45000L, b.close)).iterator.map { (dt, px) =>
        val t = MarketTrade(exchange, symbol, Price(px), Coin(vq), isBuyerMaker = false, b.ts + dt)
        Event.stamped(Topics.Trade, t, b.ts + dt, b.ts + dt)
      }
    }
