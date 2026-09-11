package hft.sim

import hft.domain.*
import hft.event.{AnyEvent, EventBus, Topics}
import org.slf4j.LoggerFactory
import ox.{Ox, fork}
import ox.channels.Source

import java.io.BufferedWriter
import java.nio.file.{Files, Path, StandardOpenOption}
import scala.util.control.NonFatal

/** 成交记录器：订阅 事件总线、把每笔成交写入 CSV 并累计**已实现**利润。
  *
  * 这是策略**之外**的旁路观察者——成交回报本就在 事件总线上广播 (EventBus 给每个订阅者
  * 独立 channel)，故记录 CSV 不与策略争抢事件、也不需要策略承担任何写文件副作用，策略保持纯粹。
  *
  * 利润核算复用 [[Ledger]] (同向加仓均价、反向平仓实现盈亏)，初始现金置 0，故 `ledger.cash`
  * 即为跨 symbol 的累计已实现盈亏。消费在单一 fork 内串行进行，无需同步。
  *
  * **隔离契约**：作为旁路观察者，记录器绝不因**运行期**的 IO 失败 (写入/关闭) 影响核心——
  * 那类异常只打错误日志、就地吞掉，内存累计照常推进 (丢的是该行 CSV，账本不丢)。真实仓位在手时，
  * 一次写盘失败不值得把引擎停掉。
  *
  * **打开失败不在此列**：路径不存在、没有写权限，是**装配期的配置错误**，在第一笔成交之前就已成立,
  * 且整轮运行一行都不会落盘。把它降级成"仅内存累计"的代价是：跑完一整天才发现 CSV 是空的,
  * 而那份记录已经不可能补回来了。所以 [[open]] 失败即抛 —— 配置错误该在启动时大声失败。
  */
final class FillRecorder(csvPath: Path):
  private val logger = LoggerFactory.getLogger(classOf[FillRecorder])

  @volatile private var ledger = Ledger.empty(AccountId.Live, 0.0)
  @volatile private var writer: Option[BufferedWriter] = None

  /** 累计已实现利润 (跨 symbol)。供外部观察/日志 */
  def cumulativeRealizedPnl: Double = ledger.cash

  /** 打开 CSV writer (幂等)。**打开失败即抛** —— 那是装配期的配置错误，见类文档的隔离契约。 */
  def open(): Unit =
    if writer.isEmpty then
      writer =
        try Some(openWriter())
        catch
          case NonFatal(e) =>
            throw IllegalStateException(s"打不开成交记录 CSV $csvPath —— 检查路径与写权限", e)
      logger.info(s"FillRecorder started, writing to ${csvPath.toAbsolutePath}")

  /** 关闭 writer (幂等)。 */
  def close(): Unit =
    writer.foreach(closeQuietly)
    writer = None

  /** 同步观察一个事件：仅消费 FillUpdate 写 CSV + 累计利润。
    * 供实盘 fork 循环与回测单线程循环共用——本身无并发设施，调用方决定线程模型。
    */
  def onEvent(ev: AnyEvent): Unit = ev.as(Topics.Fill).foreach(onFill)

  /** 启动消费循环：在调用方作用域 fork 一条常驻虚拟线程，对每个事件调用 [[onEvent]]。 */
  def run(events: Source[AnyEvent])(using Ox): Unit =
    open()
    fork {
      try while true do onEvent(events.receive())
      finally close()
    }
    ()

  private def onFill(fill: Fill): Unit =
    val (next, row) = FillRecorder.record(ledger, fill)
    ledger = next // 内存累计先行: 写盘失败也不影响 cumulativeRealizedPnl
    writer.foreach { w =>
      try
        w.write(row)
        w.newLine()
        w.flush() // 每笔落盘, 进程被打断也不丢已成交记录
        logger.info(s"[REC] $row")
      catch case NonFatal(e) => logger.error(s"failed to write fill row (dropped): $row", e)
    }

  /** 打开 CSV (追加)；文件不存在/为空时先写表头 */
  private def openWriter(): BufferedWriter =
    val fresh = !Files.exists(csvPath) || Files.size(csvPath) == 0
    val w = Files.newBufferedWriter(csvPath, StandardOpenOption.CREATE, StandardOpenOption.APPEND)
    if fresh then
      w.write(FillRecorder.Header)
      w.newLine()
      w.flush()
    w

  private def closeQuietly(w: BufferedWriter): Unit =
    try w.close()
    catch case NonFatal(e) => logger.warn("failed to close CSV writer", e)

object FillRecorder:
  /** `kind` 在 symbol 之后 —— 账本按标的记账, 而同一个 symbol 底下可能有 U 本位永续、
    * 币本位永续、几十个期权。少了这一列, 事后按 (exchange, symbol) 分组重建盈亏的人
    * 会把两条独立持仓的 realizedPnl 混在一起, 且无从察觉。 */
  val Header = "timestamp,exchange,symbol,kind,side,price,size,realizedPnl,cumulativeRealizedPnl"

  /** 纯：把一笔成交计入账本，返回 (新账本, CSV 行)。
    * realizedPnl 为本笔实现盈亏 (平仓时非零)，cumulativeRealizedPnl 为累计 (= 新账本现金)。
    */
  def record(ledger: Ledger, fill: Fill): (Ledger, String) =
    val before = ledger.cash
    val next = ledger.applyFill(fill.instrument, fill.side, fill.price, fill.size)
    val realized = next.cash - before
    val row = s"${fill.timestamp},${fill.exchange},${fill.symbol},${fill.kind},${fill.side},${fill.price},${fill.size},$realized,${next.cash}"
    (next, row)
