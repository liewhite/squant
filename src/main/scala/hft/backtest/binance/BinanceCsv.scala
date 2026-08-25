package hft.backtest.binance

import hft.domain.*
import hft.event.{AnyEvent, Event, Topic, Topics}

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.util.zip.ZipInputStream

/** data.binance.vision CSV (zip) 解析 —— 一律**流式**逐行产出。
  *
  * 官方列定义 (U 本位合约)：
  *   - bookTicker: `update_id, best_bid_price, best_bid_qty, best_ask_price, best_ask_qty, transaction_time, event_time`
  *   - trades:     `id, price, qty, quote_qty, time, is_buyer_maker`
  *
  * 部分文件首行为表头：通过"首字段能否解析为数字"探测，非数字即表头跳过。
  *
  * 全流式而非物化：单日单标的 bookTicker 可达上亿行，物化成 Vector 直接爆内存。输入是
  * [[InputStream]] (来自 [[hft.backtest.DataCache]])，解压边读边产出，常驻内存与文件大小无关；
  * 多文件的时间归并由 [[hft.backtest.MarketDataSource.merge]] 以 O(k) 内存完成。
  *
  * 流的所有权：解析器接管传入的流，**读尽时自动关闭**。迭代器提前弃用则不会关闭
  * (回测总会读尽，无虞)。
  */
object BinanceCsv:

  /** 流式解析 trades zip 为 [[MarketTrade]] 事件 (文件内已按 id/时间升序)。 */
  def streamTrades(symbol: Symbol, zip: InputStream): Iterator[AnyEvent] =
    streamRows(zip)(tradeRow(symbol))

  /** 流式解析 bookTicker zip 为 [[BBO]] 事件。
    * 注意：官方文件内偶有毫秒级乱序 (binance-public-data issue #305)，由回测引擎的时间钳制兜底。
    */
  def streamBookTicker(symbol: Symbol, zip: InputStream): Iterator[AnyEvent] =
    streamRows(zip)(bookTickerRow(symbol))

  /** bookTicker 是最大的文件 (单标的单日可达数千万行)，与 [[tradeRow]] 同样单趟切片、
    * 不物化整行 split 数组。
    * 列: update_id(0),bid_price(1),bid_qty(2),ask_price(3),ask_qty(4),transaction_time(5),event_time(6)。 */
  private def bookTickerRow(symbol: Symbol)(line: String): AnyEvent =
    val c0 = line.indexOf(',')
    val c1 = line.indexOf(',', c0 + 1)
    val c2 = line.indexOf(',', c1 + 1)
    val c3 = line.indexOf(',', c2 + 1)
    val c4 = line.indexOf(',', c3 + 1)
    val c5 = line.indexOf(',', c4 + 1) // transaction_time 之后即末列 event_time, 无尾逗号
    val bbo = BBO(
      exchange = Exchange.Binance,
      symbol = symbol,
      bidPrice = Price(line.substring(c0 + 1, c1).toDouble),
      bidQty = Coin(line.substring(c1 + 1, c2).toDouble),
      askPrice = Price(line.substring(c2 + 1, c3).toDouble),
      askQty = Coin(line.substring(c3 + 1, c4).toDouble),
      timestamp = line.substring(c5 + 1).trim.toLong, // event_time, 对齐实盘 WS 推送语义
    )
    historical(Topics.Bbo, bbo, bbo.timestamp)

  /** trades 是回测热路径 (单 symbol 月级达数千万行)，故单趟切片只取所需列、不物化整行 split 数组。
    * 列: id(0),price(1),qty(2),quote_qty(3),time(4),is_buyer_maker(5)；切片子串与 split 完全一致, 解析值逐位相同。 */
  private def tradeRow(symbol: Symbol)(line: String): AnyEvent =
    val c0 = line.indexOf(',')
    val c1 = line.indexOf(',', c0 + 1)
    val c2 = line.indexOf(',', c1 + 1)
    val c3 = line.indexOf(',', c2 + 1)
    val c4 = line.indexOf(',', c3 + 1) // time 之后即末列 is_buyer_maker, 无尾逗号
    val trade = MarketTrade(
      exchange = Exchange.Binance,
      symbol = symbol,
      price = Price(line.substring(c0 + 1, c1).toDouble),
      qty = Coin(line.substring(c1 + 1, c2).toDouble),
      isBuyerMaker = line.substring(c4 + 1).trim.equalsIgnoreCase("true"),
      timestamp = line.substring(c3 + 1, c4).toLong,
    )
    historical(Topics.Trade, trade, trade.timestamp)

  /** 历史事件的 localTs 即其历史发生时刻 (= exchangeTs)，不取墙钟：既诚实
    * (延迟由回测引擎建模，源数据无网络延迟) 又保证回测确定性。 */
  private def historical[K, P](topic: Topic[K, P], payload: P, ts: Long): AnyEvent = Event.stamped(topic, payload, ts, ts)

  /** 解压 zip 内单一 CSV 并**惰性**逐行映射 (跳过表头/空行)；读尽时关闭底层流。 */
  private def streamRows(zip: InputStream)(f: String => AnyEvent): Iterator[AnyEvent] =
    val zis = ZipInputStream(zip)
    if zis.getNextEntry == null then
      zis.close()
      Iterator.empty
    else
      val reader = BufferedReader(InputStreamReader(zis))
      new Iterator[AnyEvent]:
        private var pending: String = advance()
        /** 推进到下一数据行 (跳过表头/空行)；读到末尾即关闭流并返回 null */
        private def advance(): String =
          var l = reader.readLine()
          while l != null && !(l.nonEmpty && isDataRow(l)) do l = reader.readLine()
          if l == null then zis.close()
          l
        def hasNext: Boolean = pending != null
        def next(): AnyEvent =
          if pending == null then throw java.util.NoSuchElementException("streamRows exhausted")
          val ev = f(pending)
          pending = advance()
          ev

  /** 数据行判定：首字段 (update_id / id) 可解析为数字即数据行，否则为表头。 */
  private def isDataRow(line: String): Boolean =
    val end = line.indexOf(',')
    val head = if end < 0 then line else line.substring(0, end)
    head.nonEmpty && head.forall(c => c.isDigit)
