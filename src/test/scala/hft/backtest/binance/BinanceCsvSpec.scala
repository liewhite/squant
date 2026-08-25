package hft.backtest.binance

import hft.TestUnits.given
import hft.event.Topics

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.zip.{ZipEntry, ZipOutputStream}

/** CSV (zip) 流式解析单测：表头探测 + 字段映射 (bookTicker / trades)。 */
class BinanceCsvSpec extends munit.FunSuite:

  /** 预制一个内含单一 CSV 的 zip 流 (解析器接管并负责关闭)。 */
  private def zipOf(name: String, content: String): ByteArrayInputStream =
    val baos = ByteArrayOutputStream()
    val zos = ZipOutputStream(baos)
    zos.putNextEntry(ZipEntry(name))
    zos.write(content.getBytes("UTF-8"))
    zos.closeEntry()
    zos.close()
    ByteArrayInputStream(baos.toByteArray)

  test("bookTicker 解析: 取 event_time 作时间戳, 字段正确"):
    val csv =
      """123,100.0,1.0,100.1,2.0,1700000000000,1700000000001
        |124,100.2,3.0,100.3,4.0,1700000000010,1700000000011""".stripMargin
    val evs = BinanceCsv.streamBookTicker("BTCUSDT", zipOf("x.csv", csv)).toVector
    assertEquals(evs.size, 2)
    evs.head.as(Topics.Bbo) match
      case Some(b) =>
        assertEquals(b.symbol, "BTCUSDT")
        assertEquals(b.bidPrice, 100.0)
        assertEquals(b.bidQty.value, 1.0)
        assertEquals(b.askPrice, 100.1)
        assertEquals(b.askQty.value, 2.0)
        assertEquals(b.timestamp, 1700000000001L) // event_time, 非 transaction_time
      case None => fail(s"expected Bbo, got ${evs.head}")

  test("bookTicker 含表头: 首行非数字被跳过"):
    val csv =
      """update_id,best_bid_price,best_bid_qty,best_ask_price,best_ask_qty,transaction_time,event_time
        |123,100.0,1.0,100.1,2.0,1700000000000,1700000000001""".stripMargin
    assertEquals(BinanceCsv.streamBookTicker("BTCUSDT", zipOf("x.csv", csv)).size, 1)

  test("trades 解析: price/qty/isBuyerMaker/time 正确, 含表头跳过"):
    val csv =
      """id,price,qty,quote_qty,time,is_buyer_maker
        |1,100.5,0.5,50.25,1700000000002,true
        |2,100.6,0.3,30.18,1700000000003,false""".stripMargin
    val evs = BinanceCsv.streamTrades("BTCUSDT", zipOf("x.csv", csv)).toVector
    assertEquals(evs.size, 2)
    evs.head.as(Topics.Trade) match
      case Some(t) =>
        assertEquals(t.price, 100.5)
        assertEquals(t.qty.value, 0.5)
        assertEquals(t.isBuyerMaker, true)
        assertEquals(t.timestamp, 1700000000002L)
      case None => fail(s"expected Trade, got ${evs.head}")
    evs(1).as(Topics.Trade) match
      case Some(t) => assertEquals(t.isBuyerMaker, false)
      case None    => fail(s"expected Trade, got ${evs(1)}")

  test("历史事件的 localTs 等于 exchangeTs (不取墙钟, 保证确定性)"):
    val ev = BinanceCsv.streamTrades("BTCUSDT", zipOf("x.csv", "1,100.5,0.5,50.25,1700000000002,true")).next()
    assertEquals(ev.exchangeTs, 1700000000002L)
    assertEquals(ev.localTs, ev.exchangeTs)

  test("空 zip 内容 -> 空结果 (两种 kind)"):
    assertEquals(BinanceCsv.streamBookTicker("BTCUSDT", zipOf("x.csv", "")).size, 0)
    assertEquals(BinanceCsv.streamTrades("BTCUSDT", zipOf("x.csv", "")).size, 0)
