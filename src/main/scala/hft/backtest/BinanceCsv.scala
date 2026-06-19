package hft.backtest

import hft.domain.*
import hft.messaging.{EventData, IncomeEvent}

import java.io.{BufferedReader, ByteArrayInputStream, InputStreamReader}
import java.util.zip.ZipInputStream
import scala.collection.mutable.ArrayBuffer

/** data.binance.vision CSV (zip) 解析。
  *
  * 官方列定义 (U 本位合约)：
  *   - bookTicker: `update_id, best_bid_price, best_bid_qty, best_ask_price, best_ask_qty, transaction_time, event_time`
  *   - trades:     `id, price, qty, quote_qty, time, is_buyer_maker`
  *
  * 部分文件首行为表头：通过"首字段能否解析为数字"探测，非数字即表头跳过。
  * BBO 时间戳取 event_time (col 6) 以对齐实盘 WS 推送语义。
  */
object BinanceCsv:

  /** 解析 bookTicker zip 字节为 BBO 事件 (单文件全量物化；调用方按天合并排序)。 */
  def parseBookTicker(symbol: Symbol, zipBytes: Array[Byte]): Vector[IncomeEvent] =
    mapRows(zipBytes)(bookTickerRow(symbol))

  /** 解析 trades zip 字节为 MarketTrade 事件 (全量物化)。 */
  def parseTrades(symbol: Symbol, zipBytes: Array[Byte]): Vector[IncomeEvent] =
    mapRows(zipBytes)(tradeRow(symbol))

  /** 流式解析 trades zip：逐行产出，消费即可 GC，常驻内存仅为 zip 字节本身 (不物化整日 Vector)。
    * 单 trades 文件按 id/时间升序，故无需排序——仅供单 symbol、按天已天然有序的场景。
    * 注意：迭代器在读尽时自动关闭底层流；若**提前弃用**则不会关闭 (回测引擎总会读尽，无虞)。
    */
  def streamTrades(symbol: Symbol, zipBytes: Array[Byte]): Iterator[IncomeEvent] =
    streamRows(zipBytes)(tradeRow(symbol))

  private def bookTickerRow(symbol: Symbol)(f: Array[String]): IncomeEvent =
    val bbo = BBO(
      exchange = Exchange.Binance,
      symbol = symbol,
      bidPrice = f(1).toDouble,
      bidQty = f(2).toDouble,
      askPrice = f(3).toDouble,
      askQty = f(4).toDouble,
      timestamp = f(6).toLong,
    )
    // 历史事件的 localTs 即其历史发生时刻 (= exchangeTs)，不取墙钟：既诚实
    // (延迟由回测引擎建模，源数据无网络延迟) 又保证回测确定性。
    historical(bbo.timestamp, EventData.BboUpdate(bbo))

  private def tradeRow(symbol: Symbol)(f: Array[String]): IncomeEvent =
    val trade = MarketTrade(
      exchange = Exchange.Binance,
      symbol = symbol,
      price = f(1).toDouble,
      qty = f(2).toDouble,
      isBuyerMaker = f(5).trim.equalsIgnoreCase("true"),
      timestamp = f(4).toLong,
    )
    historical(trade.timestamp, EventData.MarketTradeUpdate(trade))

  /** 历史事件构造：localTs == exchangeTs (见上)。 */
  private def historical(ts: Long, data: EventData): IncomeEvent = IncomeEvent(ts, ts, data)

  /** 解压 zip 内单一 CSV，逐行切分为字段并映射；自动跳过表头行 (首字段非数字)。 */
  private def mapRows(zipBytes: Array[Byte])(f: Array[String] => IncomeEvent): Vector[IncomeEvent] =
    val zis = ZipInputStream(ByteArrayInputStream(zipBytes))
    try
      if zis.getNextEntry == null then Vector.empty
      else
        val reader = BufferedReader(InputStreamReader(zis))
        val out = ArrayBuffer.empty[IncomeEvent]
        var line = reader.readLine()
        while line != null do
          if line.nonEmpty && isDataRow(line) then out += f(line.split(","))
          line = reader.readLine()
        out.toVector
    finally zis.close()

  /** 解压 zip 内单一 CSV 并**惰性**逐行映射；读尽时关闭底层流 (见 [[streamTrades]] 的提前弃用说明)。 */
  private def streamRows(zipBytes: Array[Byte])(f: Array[String] => IncomeEvent): Iterator[IncomeEvent] =
    val zis = ZipInputStream(ByteArrayInputStream(zipBytes))
    if zis.getNextEntry == null then
      zis.close()
      Iterator.empty
    else
      val reader = BufferedReader(InputStreamReader(zis))
      new Iterator[IncomeEvent]:
        private var pending: String = advance()
        /** 推进到下一数据行 (跳过表头/空行)；读到末尾即关闭流并返回 null */
        private def advance(): String =
          var l = reader.readLine()
          while l != null && !(l.nonEmpty && isDataRow(l)) do l = reader.readLine()
          if l == null then zis.close()
          l
        def hasNext: Boolean = pending != null
        def next(): IncomeEvent =
          if pending == null then throw java.util.NoSuchElementException("streamRows exhausted")
          val ev = f(pending.split(","))
          pending = advance()
          ev

  /** 数据行判定：首字段 (update_id / id) 可解析为数字即数据行，否则为表头。 */
  private def isDataRow(line: String): Boolean =
    val end = line.indexOf(',')
    val head = if end < 0 then line else line.substring(0, end)
    head.nonEmpty && head.forall(c => c.isDigit)
