package hft.backtest

import hft.domain.Symbol
import hft.messaging.IncomeEvent
import org.slf4j.LoggerFactory

import java.time.LocalDate

/** 币安每日历史数据类型：自带数据源路径 key 段与对应解析器。新增类型只需加一个 case (开放封闭)。
  * trade-native 回测只需 [[Trades]]，避免去取已停发/无需的 [[BookTicker]]。
  */
enum BinanceDataKind(val key: String):
  case BookTicker extends BinanceDataKind("bookTicker")
  case Trades extends BinanceDataKind("trades")

  def parse(symbol: Symbol, zipBytes: Array[Byte]): Vector[IncomeEvent] = this match
    case BookTicker => BinanceCsv.parseBookTicker(symbol, zipBytes)
    case Trades     => BinanceCsv.parseTrades(symbol, zipBytes)

/** 币安历史数据源：把指定 symbols、日期区间 [start, end] 的若干 [[BinanceDataKind]] 还原为
  * 全局时间有序的事件流。
  *
  * 内存策略：**按天**加载——单日内所有 symbol 的各类文件解析后合并、按时间戳稳定排序再产出，
  * 逐天串联成 Iterator。内存上界 = 单日数据量。bookTicker 偶有乱序 (官方 issue #305) 由
  * 单日排序消除；超大高频 symbol 的流式 k-way merge 留作后续优化 (届时换实现即可，接口不变)。
  *
  * @param kinds 加载的数据类型，默认全部；trade-native 回测传 `Seq(BinanceDataKind.Trades)` 即可只取 trades。
  */
final class BinanceHistorySource(
    downloader: BinanceHistoryDownloader,
    symbols: Seq[Symbol],
    startDate: LocalDate,
    endDate: LocalDate,
    kinds: Seq[BinanceDataKind] = BinanceDataKind.values.toIndexedSeq,
) extends MarketDataSource:
  private val logger = LoggerFactory.getLogger(classOf[BinanceHistorySource])

  override def events(): Iterator[IncomeEvent] =
    dateRange.iterator.flatMap(loadDay)

  /** 单 symbol + 仅 trades：单文件已按时间升序，可流式 (常驻内存 O(1) 而非整日 Vector)，
    * 无需合并排序。多 symbol / 含 bookTicker (官方偶有乱序) 仍走物化+排序路径以保确定性。 */
  private val canStream: Boolean =
    symbols.sizeIs == 1 && kinds == Seq(BinanceDataKind.Trades)

  private def dateRange: Seq[LocalDate] =
    Iterator.iterate(startDate)(_.plusDays(1)).takeWhile(!_.isAfter(endDate)).toSeq

  /** 加载单日事件：可流式则惰性逐行产出，否则全 symbol×kinds 合并后按时间戳稳定升序。 */
  private def loadDay(date: LocalDate): Iterator[IncomeEvent] =
    val dateStr = date.toString // ISO YYYY-MM-DD
    if canStream then
      logger.info(s"streaming $dateStr trades (${symbols.head})")
      fetch(BinanceDataKind.Trades.key, symbols.head, dateStr)
        .fold(Iterator.empty)(BinanceCsv.streamTrades(symbols.head, _))
    else
      val dayEvents = symbols.flatMap { sym =>
        kinds.flatMap(kind => fetch(kind.key, sym, dateStr).fold(Vector.empty)(kind.parse(sym, _)))
      }.toVector
      logger.info(s"loaded $dateStr: ${dayEvents.size} events (${symbols.mkString(",")}, kinds=${kinds.map(_.key).mkString("+")})")
      dayEvents.sortBy(_.exchangeTs).iterator // 稳定排序: 同时间戳保持文件内相对序 -> 确定性
  end loadDay

  private def fetch(kind: String, symbol: Symbol, date: String): Option[Array[Byte]] =
    downloader.fetch(BinanceHistoryDownloader.dailyKey(kind, symbol, date))
