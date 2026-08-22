package hft.sim

import hft.domain.*
import hft.event.{Event, EventBus, Interest, Topics}
import ox.supervised

import java.nio.file.Files
import scala.jdk.CollectionConverters.*

class FillRecorderSpec extends munit.FunSuite:
  private val ex = Exchange.Binance
  private val sym = "BTCUSDT"
  private def fill(side: Side, price: Price, size: Quantity, ts: Timestamp): Fill =
    Fill(ex, sym, side, price, size, ts)

  // ==================== 纯 record: 累计已实现利润 ====================

  test("record: 开仓本笔实现 0, 平仓实现盈亏并累计"):
    val l0 = Ledger.empty(0.0)
    val (l1, r1) = FillRecorder.record(l0, fill(Side.Long, 100.0, 2.0, 1))
    assertEquals(l1.cash, 0.0)        // 开仓不实现
    assert(r1.endsWith(",0.0,0.0"))   // realizedPnl=0, cumulative=0

    val (l2, r2) = FillRecorder.record(l1, fill(Side.Short, 120.0, 2.0, 2))
    assertEquals(l2.cash, 40.0)       // 平多 2 @120, 实现 +40
    assert(r2.endsWith(",40.0,40.0")) // 本笔 +40, 累计 40

    val (l3, r3) = FillRecorder.record(l2, fill(Side.Short, 100.0, 1.0, 3))
    val (l4, r4) = FillRecorder.record(l3, fill(Side.Long, 90.0, 1.0, 4)) // 平空 @90, 实现 +10
    assertEquals(l4.cash, 50.0)
    assert(r4.endsWith(",10.0,50.0"))

  test("record: CSV 行包含成交字段"):
    val (_, row) = FillRecorder.record(Ledger.empty(0.0), fill(Side.Long, 100.0, 2.0, 1700000000000L))
    assertEquals(row, "1700000000000,Binance,BTCUSDT,Long,100.0,2.0,0.0,0.0")

  // ==================== 集成: 订阅总线写文件 ====================

  test("订阅 事件总线, 把成交写入 CSV (表头 + 每笔一行 + 累计利润)"):
    val tmp = Files.createTempFile("sim-fills", ".csv")
    Files.delete(tmp) // 让 recorder 视作新文件并写表头
    val rec = FillRecorder(tmp)
    supervised:
      val bus = EventBus()
      rec.run(bus.subscribe(Set(Interest.All(Topics.Fill))).events)
      bus.publish(Event.at(Topics.Fill, fill(Side.Long, 100.0, 2.0, 1), 1))
      bus.publish(Event.at(Topics.Fill, fill(Side.Short, 120.0, 2.0, 2), 2))
      // 非成交事件应被忽略
      bus.publish(Event.at(Topics.Bbo, BBO(ex, sym, 100, 1, 101, 1, 3), 3))
      Thread.sleep(150)

    val lines = Files.readAllLines(tmp).asScala.toVector
    Files.deleteIfExists(tmp)
    assertEquals(lines.head, FillRecorder.Header)
    assertEquals(lines.size, 3) // 表头 + 2 笔成交 (BBO 被忽略)
    assert(lines(1).contains("Long,100.0,2.0"))
    assert(lines(2).endsWith(",40.0,40.0")) // 第二笔平仓累计利润 40
    assertEquals(rec.cumulativeRealizedPnl, 40.0)

  test("打开文件失败不致命: 降级为仅内存累计, 不抛出"):
    // 父目录不存在 -> openWriter 抛错, 应被吞掉、记录降级, run/消费均不抛
    val rec = FillRecorder(java.nio.file.Path.of("/nonexistent-dir-xyz/sim-fills.csv"))
    supervised:
      val bus = EventBus()
      rec.run(bus.subscribe(Set(Interest.All(Topics.Fill))).events) // 不应抛
      bus.publish(Event.at(Topics.Fill, fill(Side.Long, 100.0, 2.0, 1), 1))
      bus.publish(Event.at(Topics.Fill, fill(Side.Short, 120.0, 2.0, 2), 2))
      Thread.sleep(150)
    // 写盘禁用, 但内存累计照常推进
    assertEquals(rec.cumulativeRealizedPnl, 40.0)
