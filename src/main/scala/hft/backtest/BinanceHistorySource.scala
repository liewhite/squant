package hft.backtest

import hft.domain.Symbol
import hft.messaging.IncomeEvent
import org.slf4j.LoggerFactory

import java.time.LocalDate

/** 币安历史数据源：把指定 symbols、日期区间 [start, end] 的 bookTicker + trades 还原为
  * 全局时间有序的事件流。
  *
  * 内存策略：**按天**加载——单日内所有 symbol 的两类文件解析后合并、按时间戳稳定排序再产出，
  * 逐天串联成 Iterator。内存上界 = 单日数据量。bookTicker 偶有乱序 (官方 issue #305) 由
  * 单日排序消除；超大高频 symbol 的流式 k-way merge 留作后续优化 (届时换实现即可，接口不变)。
  */
final class BinanceHistorySource(
    downloader: BinanceHistoryDownloader,
    symbols: Seq[Symbol],
    startDate: LocalDate,
    endDate: LocalDate,
) extends MarketDataSource:
  private val logger = LoggerFactory.getLogger(classOf[BinanceHistorySource])

  override def events(): Iterator[IncomeEvent] =
    dateRange.iterator.flatMap(loadDay)

  private def dateRange: Seq[LocalDate] =
    Iterator.iterate(startDate)(_.plusDays(1)).takeWhile(!_.isAfter(endDate)).toSeq

  /** 加载单日全部 symbol 的 bookTicker + trades，合并后按时间戳稳定升序。 */
  private def loadDay(date: LocalDate): Vector[IncomeEvent] =
    val dateStr = date.toString // ISO YYYY-MM-DD
    val dayEvents = symbols.flatMap { sym =>
      val book = fetch("bookTicker", sym, dateStr).fold(Vector.empty)(BinanceCsv.parseBookTicker(sym, _))
      val trades = fetch("trades", sym, dateStr).fold(Vector.empty)(BinanceCsv.parseTrades(sym, _))
      book ++ trades
    }.toVector
    logger.info(s"loaded $dateStr: ${dayEvents.size} events (${symbols.mkString(",")})")
    dayEvents.sortBy(_.exchangeTs) // 稳定排序: 同时间戳保持文件内相对序 -> 确定性
  end loadDay

  private def fetch(kind: String, symbol: Symbol, date: String): Option[Array[Byte]] =
    downloader.fetch(BinanceHistoryDownloader.dailyKey(kind, symbol, date))
