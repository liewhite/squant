package hft.backtest

import hft.messaging.EventData

import java.io.ByteArrayOutputStream
import java.util.zip.{ZipEntry, ZipOutputStream}

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
    evs.head.data match
      case EventData.BboUpdate(b) =>
        assertEquals(b.symbol, "BTCUSDT")
        assertEquals(b.bidPrice, 100.0)
        assertEquals(b.askPrice, 100.1)
        assertEquals(b.timestamp, 1700000000001L) // event_time, 非 transaction_time
      case other => fail(s"expected BboUpdate, got $other")

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
    evs.head.data match
      case EventData.MarketTradeUpdate(t) =>
        assertEquals(t.price, 100.5)
        assertEquals(t.qty, 0.5)
        assertEquals(t.isBuyerMaker, true)
        assertEquals(t.timestamp, 1700000000002L)
      case other => fail(s"expected MarketTradeUpdate, got $other")
    evs(1).data match
      case EventData.MarketTradeUpdate(t) => assertEquals(t.isBuyerMaker, false)
      case other                          => fail(s"expected MarketTradeUpdate, got $other")

  test("空 zip / 空内容 -> 空结果"):
    assertEquals(BinanceCsv.parseBookTicker("BTCUSDT", zipOf("x.csv", "")).size, 0)
