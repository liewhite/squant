package hft.backtest.binance

import hft.backtest.{MarketDataKind, MarketDataSource}
import hft.domain.Symbol
import hft.event.AnyEvent
import org.slf4j.LoggerFactory

import java.io.InputStream
import java.time.LocalDate

/** 币安每日文件：交易所无关的 [[MarketDataKind]] 到"官方路径段 + 解析器"的映射。
  *
  * 币安私有知识 (目录怎么命名、CSV 怎么解析) 只在此处落地，回测层只见 [[MarketDataKind]]。
  * 新增数据类型 = 新增一个 case (开放封闭)。
  *
  * `values` 的**声明顺序**即同刻事件的归并定序，改动会改变回测结果，勿随意调整。
  */
private[binance] object BinanceDailyFile:
  /** bookTicker 有界重排序窗口 (事件数)：官方乱序为相邻数条互换，几十已绰绰有余；
    * 内存代价 O(window)，取 256 留足余量。 */
  val ReorderWindow = 256

private[binance] enum BinanceDailyFile(val kind: MarketDataKind, val segment: String):
  case Trades extends BinanceDailyFile(MarketDataKind.Trades, "trades")
  case BookTicker extends BinanceDailyFile(MarketDataKind.Bbo, "bookTicker")

  def stream(symbol: Symbol, zip: InputStream): Iterator[AnyEvent] = this match
    case Trades => BinanceCsv.streamTrades(symbol, zip) // 文件按 id 升序, 天然满足契约
    // bookTicker 文件内偶有毫秒级乱序 (binance-public-data issue #305) -> 有界窗口就地修正,
    // 使本流满足 MarketDataSource 的升序契约, 归并才成立
    case BookTicker => MarketDataSource.reorder(BinanceCsv.streamBookTicker(symbol, zip), BinanceDailyFile.ReorderWindow)

/** 币安每日历史数据源：把指定 symbols、日期区间 [start, end] 的若干 [[MarketDataKind]]
  * 还原为全局时间有序的事件流。
  *
  * 内存策略：**全流式**。文件天然按天切分且天内升序，故"逐天串联 + 天内 k 路归并"即得全局升序，
  * 无需物化任何一天，也不把 zip 整包读进内存 (缓存返回流，解压边读边产出)。常驻内存 = O(标的数 ×
  * 数据类型数) 个解压缓冲 + 归并游标，与回测跨度、单日数据量都无关。
  *
  * 两处已知边界，都由回测引擎的时间钳制吸收并计数告警：文件按 transaction_time 切天而 BBO
  * 事件戳取 event_time，故午夜前后可能有跨天逆序；单日内的多流归并只保证各流自身有序。
  *
  * 流的生命周期：解析器读尽时自动关闭。回测正常路径必然读尽；中途抛异常则当天这批文件句柄
  * (标的数 × 类型数, 有界) 要等进程退出才回收 —— 回测是短命进程，接受这个取舍。
  *
  * 缺数据 (404) 跳过并告警：币安 futures 的 bookTicker 每日文件止于 2024-03-30，此后只有
  * trades —— 该区间要 BBO 需另接数据源 (换一个 [[hft.backtest.MarketDataProvider]] 实现)
  * 或用合成盘口 ([[hft.backtest.SyntheticBboSource]])。
  */
final class BinanceDailySource(
    downloader: BinanceHistoryDownloader,
    symbols: Seq[Symbol],
    startDate: LocalDate,
    endDate: LocalDate,
    kinds: Set[MarketDataKind],
) extends MarketDataSource:
  private val logger = LoggerFactory.getLogger(classOf[BinanceDailySource])

  /** 按枚举声明序过滤 —— 不用 `kinds` 自身的迭代序 (Set 无序, 会让回测不可复现)。 */
  private val files: Vector[BinanceDailyFile] =
    BinanceDailyFile.values.toVector.filter(f => kinds.contains(f.kind))

  require(symbols.nonEmpty, "backtest needs at least one symbol")
  require(files.nonEmpty, s"no binance daily file maps to kinds=$kinds")
  require(!startDate.isAfter(endDate), s"empty date range: $startDate .. $endDate")

  override def events(): Iterator[AnyEvent] = dateRange.iterator.flatMap(loadDay)

  private def dateRange: Seq[LocalDate] =
    Iterator.iterate(startDate)(_.plusDays(1)).takeWhile(!_.isAfter(endDate)).toSeq

  /** 单日：每个 (symbol, file) 一条惰性流，按时间归并。流的**下标顺序**决定同刻定序 -> 确定性。 */
  private def loadDay(date: LocalDate): Iterator[AnyEvent] =
    val dateStr = date.toString // ISO YYYY-MM-DD
    // 先打日志再取数: 冷缓存下这一步要下载几十 MB, 静默几分钟会让人以为卡死
    logger.info(s"replaying $dateStr: ${symbols.mkString(",")} kinds=${files.map(_.segment).mkString("+")}")
    val streams =
      for
        symbol <- symbols
        file <- files
      yield fetch(file, symbol, dateStr).fold(Iterator.empty[AnyEvent])(file.stream(symbol, _))
    MarketDataSource.merge(streams)

  private def fetch(file: BinanceDailyFile, symbol: Symbol, date: String): Option[InputStream] =
    downloader.fetch(BinanceHistoryDownloader.dailyKey(file.segment, symbol, date))
