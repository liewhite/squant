package hft.backtest.binance

import hft.backtest.{DataCache, MarketDataKind}
import sttp.client4.testing.SyncBackendStub

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream}
import java.time.LocalDate
import java.util.zip.{ZipEntry, ZipOutputStream}

/** 数据源单测：多 kind / 多 symbol / 跨天流式归并后全局按时间戳升序，且源文件内乱序被修正。
  *
  * 全程离线：内存 [[DataCache]] 喂预制 zip，HTTP 后端换成恒回 404 的 stub —— 缓存缺 key 时
  * 走的正是"该日无此数据"分支，且**任何**用例都不可能悄悄打到真实 data.binance.vision。
  */
class BinanceDailySourceSpec extends munit.FunSuite:

  private def zipOf(content: String): Array[Byte] =
    val baos = ByteArrayOutputStream()
    val zos = ZipOutputStream(baos)
    zos.putNextEntry(ZipEntry("data.csv"))
    zos.write(content.getBytes("UTF-8"))
    zos.closeEntry()
    zos.close()
    baos.toByteArray

  /** 仅返回预置 key 的内存缓存；put 忽略。缺 key = 该日无数据 (404 路径)。 */
  private class MapCache(entries: Map[String, Array[Byte]]) extends DataCache:
    def open(key: String): Option[InputStream] = entries.get(key).map(ByteArrayInputStream(_))
    def put(key: String, data: Array[Byte]): Unit = ()

  /** 恒回 404 的 HTTP 后端：缓存未命中即等价"数据源没有这个文件"。 */
  private def offlineBackend = SyncBackendStub.whenAnyRequest.thenRespondNotFound()

  private def bookKey(symbol: String, date: String) = BinanceHistoryDownloader.dailyKey("bookTicker", symbol, date)
  private def tradeKey(symbol: String, date: String) = BinanceHistoryDownloader.dailyKey("trades", symbol, date)

  private def sourceOf(
      entries: Map[String, Array[Byte]],
      symbols: Seq[String],
      start: LocalDate,
      end: LocalDate,
      kinds: Set[MarketDataKind] = MarketDataKind.All,
  ) =
    BinanceDailySource(BinanceHistoryDownloader(offlineBackend, MapCache(entries)), symbols, start, end, kinds)

  test("bookTicker 与 trades 归并为全局升序; bookTicker 文件内乱序被重排序窗口修正"):
    val date = LocalDate.parse("2024-01-01")
    // bookTicker 故意乱序: event_time 30 排在 10 之前
    val book =
      """1,100.0,1,100.1,1,29,30
        |2,100.2,1,100.3,1,9,10""".stripMargin
    val trades = "1,100.5,0.5,50,20,true" // 时间戳 20 落在两条 bookTicker (10, 30) 之间
    val src = sourceOf(Map(bookKey("BTCUSDT", "2024-01-01") -> zipOf(book), tradeKey("BTCUSDT", "2024-01-01") -> zipOf(trades)), Seq("BTCUSDT"), date, date)
    assertEquals(src.events().map(_.exchangeTs).toVector, Vector(10L, 20L, 30L))

  test("多 symbol 交错归并为全局升序"):
    val date = LocalDate.parse("2024-01-01")
    val btc = """1,100.0,1,50,10,true
                |2,101.0,1,50,30,true""".stripMargin
    val eth = """1,10.0,1,5,20,false
                |2,11.0,1,5,40,false""".stripMargin
    val src = sourceOf(
      Map(tradeKey("BTCUSDT", "2024-01-01") -> zipOf(btc), tradeKey("ETHUSDT", "2024-01-01") -> zipOf(eth)),
      Seq("BTCUSDT", "ETHUSDT"),
      date,
      date,
      Set(MarketDataKind.Trades),
    )
    assertEquals(src.events().map(_.exchangeTs).toVector, Vector(10L, 20L, 30L, 40L))

  test("跨天串联: 后一天的事件全部晚于前一天"):
    val d1 = LocalDate.parse("2024-01-01")
    val d2 = LocalDate.parse("2024-01-02")
    val src = sourceOf(
      Map(tradeKey("BTCUSDT", "2024-01-01") -> zipOf("1,100.0,1,50,10,true"), tradeKey("BTCUSDT", "2024-01-02") -> zipOf("2,101.0,1,50,90000000,true")),
      Seq("BTCUSDT"),
      d1,
      d2,
      Set(MarketDataKind.Trades),
    )
    assertEquals(src.events().map(_.exchangeTs).toVector, Vector(10L, 90000000L))

  test("某日某 kind 缺数据 (404) -> 跳过该文件, 其余照常产出"):
    val date = LocalDate.parse("2024-01-01")
    // 只有 trades, bookTicker 缺失 (对应币安 2024-03-30 之后的真实情况: 只剩逐笔成交)
    val src = sourceOf(Map(tradeKey("BTCUSDT", "2024-01-01") -> zipOf("1,100.0,1,50,10,true")), Seq("BTCUSDT"), date, date)
    assertEquals(src.events().map(_.exchangeTs).toVector, Vector(10L))

  test("只要 Trades 时不去取 bookTicker (省下无谓下载)"):
    val date = LocalDate.parse("2024-01-01")
    val requested = scala.collection.mutable.ArrayBuffer.empty[String]
    val cache = new DataCache:
      def open(key: String): Option[InputStream] =
        requested += key
        Option.when(key.contains("trades"))(ByteArrayInputStream(zipOf("1,100.0,1,50,10,true")))
      def put(key: String, data: Array[Byte]): Unit = ()
    val src = BinanceDailySource(BinanceHistoryDownloader(offlineBackend, cache), Seq("BTCUSDT"), date, date, Set(MarketDataKind.Trades))
    src.events().toVector
    assertEquals(requested.toVector, Vector(tradeKey("BTCUSDT", "2024-01-01")))

  test("装配期即拒绝无效参数 (空 symbol / 空 kind / 倒序区间)"):
    val date = LocalDate.parse("2024-01-01")
    val dl = BinanceHistoryDownloader(offlineBackend, MapCache(Map.empty))
    intercept[IllegalArgumentException](BinanceDailySource(dl, Nil, date, date, MarketDataKind.All))
    intercept[IllegalArgumentException](BinanceDailySource(dl, Seq("BTCUSDT"), date, date, Set.empty))
    intercept[IllegalArgumentException](BinanceDailySource(dl, Seq("BTCUSDT"), date.plusDays(1), date, MarketDataKind.All))
