package hft.backtest

import hft.domain.{Exchange, Symbol}
import hft.event.AnyEvent

import java.time.LocalDate
import scala.collection.mutable

/** 回测行情数据源：产出**全局按 exchangeTs 升序**的市场事件 (BBO / MarketTrade / ...)。
  *
  * 这是回测与"数据从哪来、怎么缓存"之间的唯一缝隙。[[BacktestEngine]] 只消费有序事件流，
  * 不关心是币安历史文件、内存假数据还是别的来源。
  *
  * 升序是**契约**：虚拟时间不可倒流。产出方负责保证；引擎侧另有兜底 (见 [[BacktestEngine]] 的
  * 时间钳制)，那是防御而非许可。
  */
trait MarketDataSource:
  def events(): Iterator[AnyEvent]

object MarketDataSource:

  /** 把若干**各自已按 exchangeTs 升序**的流归并为一条升序流 —— k 路归并，流式，常驻内存 O(k)。
    *
    * 用它替代"全部物化后 sortBy"：单日 ETHUSDT 的 bookTicker 就有上亿行，物化整日不可行。
    * 同时间戳按**输入流下标**定序 (非哈希序、非线程序)，故同一输入必得同一结果。
    */
  def merge(streams: Seq[Iterator[AnyEvent]]): Iterator[AnyEvent] =
    val live = streams.iterator.map(_.buffered).zipWithIndex.filter(_._1.hasNext).toVector
    if live.isEmpty then Iterator.empty
    else if live.sizeIs == 1 then live.head._1 // 单流免归并开销 (最常见: 单 symbol 单 kind)
    else MergedStreams(live)

  /** 用**有界重排序窗口**修正一条局部乱序的流：始终缓冲最近 `window` 个事件，每次产出其中
    * 时间戳最小者。能完全吸收跨度小于 window 的乱序 (如相邻数条互换)，代价是 O(window) 内存。
    *
    * 用于源文件自身违反升序契约的情形 (币安 bookTicker 的已知问题)。修正在**数据源侧**完成 ——
    * 升序是 [[MarketDataSource]] 的契约，谁产出谁负责；[[BacktestEngine]] 的时间钳制是兜底，
    * 不是许可。同时间戳按到达序定序，故结果确定。
    */
  def reorder(stream: Iterator[AnyEvent], window: Int): Iterator[AnyEvent] =
    require(window > 0, s"reorder window must be positive, got $window")
    new Iterator[AnyEvent]:
      // 最小堆 (PriorityQueue 弹最大, 故 compare 取反), 键 = (时间戳, 到达序)
      private given Ordering[(AnyEvent, Long)] with
        def compare(x: (AnyEvent, Long), y: (AnyEvent, Long)): Int =
          val byTs = java.lang.Long.compare(y._1.exchangeTs, x._1.exchangeTs)
          if byTs != 0 then byTs else java.lang.Long.compare(y._2, x._2)
      private val buf = mutable.PriorityQueue.empty[(AnyEvent, Long)]
      private var arrival = 0L
      private def pull(): Unit =
        while buf.size < window && stream.hasNext do
          buf.enqueue((stream.next(), arrival))
          arrival += 1
      pull()
      override def hasNext: Boolean = buf.nonEmpty
      override def next(): AnyEvent =
        if buf.isEmpty then throw java.util.NoSuchElementException("reorder exhausted")
        val ev = buf.dequeue()._1
        pull()
        ev

  /** 归并游标：`it.head` 为该流的待产出事件 (BufferedIterator 已缓存, 比较不重复读)。 */
  private final class MergedStreams(live: Vector[(BufferedIterator[AnyEvent], Int)]) extends Iterator[AnyEvent]:
    private type Cursor = (BufferedIterator[AnyEvent], Int)
    // 最小堆: 先按事件时间、同刻按输入流下标。自定义 compare 而非 Ordering.by(tuple),
    // 避免每次堆比较分配 Tuple2 + 装箱 (亿级行数下是 GC 大头)。PriorityQueue 弹最大, 故取反。
    private given Ordering[Cursor] with
      def compare(a: Cursor, b: Cursor): Int =
        val byTs = java.lang.Long.compare(b._1.head.exchangeTs, a._1.head.exchangeTs)
        if byTs != 0 then byTs else java.lang.Integer.compare(b._2, a._2)
    private val pq = mutable.PriorityQueue.from(live)

    override def hasNext: Boolean = pq.nonEmpty

    override def next(): AnyEvent =
      if pq.isEmpty then throw java.util.NoSuchElementException("merge exhausted")
      val cursor = pq.dequeue()
      val ev = cursor._1.next()
      if cursor._1.hasNext then pq.enqueue(cursor) // 该流未尽 -> 带着新 head 重新入堆
      ev

/** 行情数据类型 —— **交易所无关**。各 [[MarketDataProvider]] 自行映射到它的存储路径与解析器。
  *
  * 两类数据对应撮合的两侧 (见 [[hft.sim.Matcher]])：
  *   - [[Trades]]：逐笔成交，maker 挂单"被真实成交穿越"的依据 (悲观侧)
  *   - [[Bbo]]：L1 盘口，taker 单取对手价成交的依据 (乐观侧)
  */
enum MarketDataKind:
  case Trades
  case Bbo

object MarketDataKind:
  val All: Set[MarketDataKind] = MarketDataKind.values.toSet

/** 交易所历史行情供应商 —— "回测数据从哪个交易所来"的唯一抽象。
  *
  * 职责：把 (标的, 日期区间, 数据类型) 还原为一条全局时间有序的事件流，下载/缓存/解析等
  * 交易所私有细节全部藏在实现内。新增交易所 = 新增一个实现，回测引擎与策略一行不动。
  *
  * [[exchange]] 是数据的归属交易所，也是回测装配的单一数据源：撮合标哪个交易所应取自它，
  * 而非在调用点另写一遍 —— 杜绝"数据是币安、撮合标 OKX"的错配。
  */
trait MarketDataProvider:
  def exchange: Exchange

  /** 指定标的与日期区间 [start, end] (含两端) 的行情流。
    * 某标的某日缺某类数据时跳过并告警 (交易所历史数据本就有缺口)，不中断回测。
    */
  def source(
      symbols: Seq[Symbol],
      start: LocalDate,
      end: LocalDate,
      kinds: Set[MarketDataKind] = MarketDataKind.All,
  ): MarketDataSource
