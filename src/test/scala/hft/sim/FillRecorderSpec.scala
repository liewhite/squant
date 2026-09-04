package hft.sim

import hft.domain.*
import hft.event.{Event, EventBus, Interest, Topics}
import ox.supervised

import java.nio.file.Files
import scala.jdk.CollectionConverters.*
import hft.TestUnits.given

class FillRecorderSpec extends munit.FunSuite:
  private val ex = Exchange.Binance
  private val sym = "BTCUSDT"
  private def fill(side: Side, price: Price, size: Coin, ts: Timestamp): Fill =
    Fill(AccountId.Live, ex, sym, side, price, size, ts)

  // ==================== 纯 record: 累计已实现利润 ====================

  test("record: 开仓本笔实现 0, 平仓实现盈亏并累计"):
    val l0 = Ledger.empty(AccountId.Live, 0.0)
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
    val (_, row) = FillRecorder.record(Ledger.empty(AccountId.Live, 0.0), fill(Side.Long, 100.0, 2.0, 1700000000000L))
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
      bus.publish(Event.at(Topics.Bbo, BBO(ex, sym, 100, Coin(1), 101, Coin(1), 3), 3))
      Thread.sleep(150)

    val lines = Files.readAllLines(tmp).asScala.toVector
    Files.deleteIfExists(tmp)
    assertEquals(lines.head, FillRecorder.Header)
    assertEquals(lines.size, 3) // 表头 + 2 笔成交 (BBO 被忽略)
    assert(lines(1).contains("Long,100.0,2.0"))
    assert(lines(2).endsWith(",40.0,40.0")) // 第二笔平仓累计利润 40
    assertEquals(rec.cumulativeRealizedPnl, 40.0)

  test("CSV 打不开即抛 —— 那是装配期配置错误, 不是运行期 IO 抖动"):
    // 降级成"仅内存累计"的代价: 跑完一整天才发现 CSV 是空的, 而那份记录补不回来了。
    // 运行期写入失败仍然吞 (真实仓位在手, 不值得为一行 CSV 停机), 两者不是一类。
    val dir = Files.createTempDirectory("fill-recorder-spec")
    val unwritable = dir.resolve("no-such-dir").resolve("fills.csv") // 父目录不存在
    val e = intercept[IllegalStateException](FillRecorder(unwritable).open())
    assert(e.getMessage.contains("打不开成交记录 CSV"), e.getMessage)
