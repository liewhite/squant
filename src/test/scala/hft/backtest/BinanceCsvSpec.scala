package hft.backtest


import java.io.ByteArrayOutputStream
import java.util.zip.{ZipEntry, ZipOutputStream}
import hft.event.Topics

/** CSV (zip) 解析单测：表头探测 + 字段映射 (bookTicker / trades)。 */
class BinanceCsvSpec extends munit.FunSuite:

  private def zipOf(name: String, content: String): Array[Byte] =
    val baos = ByteArrayOutputStream()
    val zos = ZipOutputStream(baos)
    zos.putNextEntry(ZipEntry(name))
    zos.write(content.getBytes("UTF-8"))
    zos.closeEntry()
    zos.close()
    baos.toByteArray

  test("bookTicker 解析: 取 event_time 作时间戳, 字段正确"):
    val csv =
      """123,100.0,1.0,100.1,2.0,1700000000000,1700000000001
        |124,100.2,3.0,100.3,4.0,1700000000010,1700000000011""".stripMargin
    val evs = BinanceCsv.parseBookTicker("BTCUSDT", zipOf("x.csv", csv))
    assertEquals(evs.size, 2)
    evs.head.as(Topics.Bbo) match
      case Some(b) =>
        assertEquals(b.symbol, "BTCUSDT")
        assertEquals(b.bidPrice, 100.0)
        assertEquals(b.askPrice, 100.1)
        assertEquals(b.timestamp, 1700000000001L) // event_time, 非 transaction_time
      case None => fail(s"expected Bbo, got ${evs.head}")

  test("bookTicker 含表头: 首行非数字被跳过"):
    val csv =
      """update_id,best_bid_price,best_bid_qty,best_ask_price,best_ask_qty,transaction_time,event_time
        |123,100.0,1.0,100.1,2.0,1700000000000,1700000000001""".stripMargin
    val evs = BinanceCsv.parseBookTicker("BTCUSDT", zipOf("x.csv", csv))
    assertEquals(evs.size, 1)

  test("trades 解析: price/qty/isBuyerMaker/time 正确"):
    val csv =
      """id,price,qty,quote_qty,time,is_buyer_maker
        |1,100.5,0.5,50.25,1700000000002,true
        |2,100.6,0.3,30.18,1700000000003,false""".stripMargin
    val evs = BinanceCsv.parseTrades("BTCUSDT", zipOf("x.csv", csv))
    assertEquals(evs.size, 2)
    evs.head.as(Topics.Trade) match
      case Some(t) =>
        assertEquals(t.price, 100.5)
        assertEquals(t.qty, 0.5)
        assertEquals(t.isBuyerMaker, true)
        assertEquals(t.timestamp, 1700000000002L)
      case None => fail(s"expected Trade, got ${evs.head}")
    evs(1).as(Topics.Trade) match
      case Some(t) => assertEquals(t.isBuyerMaker, false)
      case None    => fail(s"expected Trade, got ${evs(1)}")

  test("streamTrades 流式解析 = parseTrades (同序同值) + 跳表头"):
    val csv =
      """id,price,qty,quote_qty,time,is_buyer_maker
        |1,100.5,0.5,50.25,1700000000002,true
        |2,100.6,0.3,30.18,1700000000003,false
        |3,100.7,0.2,20.14,1700000000004,true""".stripMargin
    val bytes = zipOf("x.csv", csv)
    val streamed = BinanceCsv.streamTrades("BTCUSDT", bytes).toVector
    assertEquals(streamed, BinanceCsv.parseTrades("BTCUSDT", bytes)) // 流式与物化逐事件一致
    assertEquals(streamed.size, 3)

  test("空 zip / 空内容 -> 空结果"):
    assertEquals(BinanceCsv.parseBookTicker("BTCUSDT", zipOf("x.csv", "")).size, 0)
    assertEquals(BinanceCsv.streamTrades("BTCUSDT", zipOf("x.csv", "")).toVector.size, 0)
