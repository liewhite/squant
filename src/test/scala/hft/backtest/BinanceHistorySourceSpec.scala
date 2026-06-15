package hft.backtest

import sttp.client4.DefaultSyncBackend

import java.io.ByteArrayOutputStream
import java.time.LocalDate
import java.util.zip.{ZipEntry, ZipOutputStream}

/** 数据源单测：单日内 bookTicker + trades 合并后全局按时间戳升序 (含乱序输入的归并)。
  * 用内存 [[DataCache]] 喂预制 zip，下载器全程命中缓存 -> 无网络。
  */
class BinanceHistorySourceSpec extends munit.FunSuite:

  private def zipOf(content: String): Array[Byte] =
    val baos = ByteArrayOutputStream()
    val zos = ZipOutputStream(baos)
    zos.putNextEntry(ZipEntry("data.csv"))
    zos.write(content.getBytes("UTF-8"))
    zos.closeEntry()
    zos.close()
    baos.toByteArray

  /** 仅返回预置 key 的内存缓存；put 忽略。 */
  private class MapCache(entries: Map[String, Array[Byte]]) extends DataCache:
    def get(key: String): Option[Array[Byte]] = entries.get(key)
    def put(key: String, data: Array[Byte]): Unit = ()

  test("bookTicker 与 trades 合并, 按时间戳升序; bookTicker 内乱序被排序消除"):
    val date = LocalDate.parse("2024-01-01")
    // bookTicker 故意乱序: ts 30 在 10 之前
    val book =
      """1,100.0,1,100.1,1,29,30
        |2,100.2,1,100.3,1,9,10""".stripMargin
    // trades 时间戳 20 落在两条 bookTicker (10, 30) 之间
    val trades = "1,100.5,0.5,50,20,true"

    val cache = MapCache(
      Map(
        BinanceHistoryDownloader.dailyKey("bookTicker", "BTCUSDT", "2024-01-01") -> zipOf(book),
        BinanceHistoryDownloader.dailyKey("trades", "BTCUSDT", "2024-01-01") -> zipOf(trades),
      )
    )
    val downloader = BinanceHistoryDownloader(DefaultSyncBackend(), cache)
    val source = BinanceHistorySource(downloader, Seq("BTCUSDT"), date, date)

    val tss = source.events().map(_.exchangeTs).toVector
    assertEquals(tss, Vector(10L, 20L, 30L)) // 全局升序: bbo(10), trade(20), bbo(30)
