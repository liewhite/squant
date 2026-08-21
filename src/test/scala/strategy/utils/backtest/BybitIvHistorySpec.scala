package strategy.utils.backtest

import java.nio.file.Files

/** BybitIvHistory 纯逻辑单测: 分页/覆盖率判断、严格无前视采样、CSV 缓存读写 (跳过表头)。不打网络。 */
class BybitIvHistorySpec extends munit.FunSuite:
  import BybitIvHistory.{HourMs, WindowMs, missingWindows, sampleAt}

  test("missingWindows: 全覆盖 -> 无需补取"):
    val full = (a: Long, b: Long) => ((b - a) / HourMs).toInt // 每窗口都满覆盖
    assertEquals(missingWindows(0, WindowMs * 2, full), Seq.empty)

  test("missingWindows: 零覆盖 -> 全部窗口都要补 (29天切窗 + 末尾余量窗)"):
    val none = (_: Long, _: Long) => 0
    val ws = missingWindows(0, WindowMs * 2 + HourMs * 5, none)
    assertEquals(ws, Seq((0L, WindowMs), (WindowMs, WindowMs * 2), (WindowMs * 2, WindowMs * 2 + HourMs * 5)))

  test("missingWindows: 覆盖率刚好低于阈值才补 (阈值=0.5)"):
    val expected = (WindowMs / HourMs).toInt // 一个满窗口的理论小时点数
    // 第一窗覆盖 = 阈值以上 -> 跳过; 第二窗覆盖 = 阈值以下 -> 补
    val cov = (a: Long, _: Long) => if a == 0L then (expected * 0.6).toInt else (expected * 0.4).toInt
    assertEquals(missingWindows(0, WindowMs * 2, cov), Seq((WindowMs, WindowMs * 2)))

  test("missingWindows: 起止相等 -> 无窗口"):
    assertEquals(missingWindows(1000, 1000, (_, _) => 0), Seq.empty)

  test("sampleAt 严格无前视: 取 ≤ts 的最近点; ts 早于首点 -> None; 空 -> None"):
    val s = Vector((100L, 0.5), (200L, 0.6), (300L, 0.7))
    assertEquals(sampleAt(s, 250L), Some(0.6))   // ≤250 最近 = 200
    assertEquals(sampleAt(s, 200L), Some(0.6))   // 命中边界
    assertEquals(sampleAt(s, 300L), Some(0.7))
    assertEquals(sampleAt(s, 99L), None)         // 早于首点 -> 不回退未来点
    assertEquals(sampleAt(Vector.empty, 100L), None)

  test("CSV 缓存: 写入带表头, 读回跳过表头并还原点 (经 hourly 全覆盖路径, 不触发网络)"):
    val dir = Files.createTempDirectory("iv-cache-test")
    try
      // 预置缓存文件 (含表头), 令 hourly 判定全覆盖 -> 不调用 client
      val file = dir.resolve("bybit-iv").resolve("ETH-USDT-7.csv")
      Files.createDirectories(file.getParent)
      val pts = (0L until (WindowMs / HourMs)).map(i => (i * HourMs, 0.5 + i * 1e-6))
      val sb = new StringBuilder("ts,iv\n")
      pts.foreach { case (ts, iv) => sb.append(ts).append(',').append(iv).append('\n') }
      Files.writeString(file, sb.toString)
      // backend 不会被用到 (全覆盖 -> 无 fetch); 传 null 验证确实不打网络
      val got = BybitIvHistory.hourly(null, "ETH", "USDT", 7, 0, WindowMs - HourMs, dir.toString)
      assertEquals(got.size, pts.size)
      assertEquals(got.head, (0L, 0.5))
      assert(got.forall(p => !p._2.isNaN), "表头 ts,iv 行不应被当作数据点")
    finally
      import scala.jdk.CollectionConverters.*
      Files.walk(dir).iterator.asScala.toSeq.reverse.foreach(Files.deleteIfExists)
