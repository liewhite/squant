package hft.indicator

import scala.collection.mutable

/** 一根 K 线。closed=false 表示盘中 (in-progress) 根，仍会随新成交更新。 */
final case class Candle(
    openTime: Long,
    open: Double,
    high: Double,
    low: Double,
    close: Double,
    volume: Double,
    closed: Boolean,
)

/** K 线聚合**基类** —— 只负责按 [[periodMs]] 把逐笔成交聚合成 K 线序列 (缓存最多 [[maxBars]] 根已收盘
  * bar) + 一根盘中根，**不含任何指标**。
  *
  * 技术指标以**可叠加 trait** (stackable traits) 混入，覆写钩子 [[onBarClosed]] / [[onBarUpdated]]
  * (须 `abstract override` 并调 `super`，从而沿 mix-in 链依次更新)：
  * {{{
  *   val klines = new KlineSeries(60_000, 200) with Macd with Kdj
  *   klines.update(ts, price, qty)   // 先更新 K 线，再依次更新各指标
  *   klines.macdDirection; klines.kdjValues
  * }}}
  *
  * 钩子时序：跨周期时先把上一根 bar 收盘 (closed=true，已入序列) 再触发 [[onBarClosed]]，
  * 随后总会以最新盘中根触发 [[onBarUpdated]] —— 指标据此"收盘固定 + 盘中动态"。
  */
class KlineSeries(val periodMs: Long, val maxBars: Int):
  private val completed = mutable.ArrayDeque.empty[Candle]
  private var cur: Option[Candle] = None

  /** 一根 bar 收盘 (跨周期固定)，此时该 bar 已在 [[bars]] 末尾。指标 trait 覆写以冻结其值。 */
  protected def onBarClosed(bar: Candle): Unit = ()

  /** 盘中根刷新 (含刚开的新根)。指标 trait 覆写以动态更新其值。 */
  protected def onBarUpdated(bar: Candle): Unit = ()

  /** 喂入一笔成交：先更新 K 线，再触发指标钩子 (final，指标只覆写钩子而非此模板方法) */
  final def update(ts: Long, price: Double, qty: Double = 0.0): Unit =
    val bucket = ts / periodMs
    cur match
      case None =>
        cur = Some(Candle(bucket * periodMs, price, price, price, price, qty, closed = false))
      case Some(c) =>
        // 跨多个空缺周期 (成交稀疏) 只收盘上一根、不补空 bar：指标按"有成交的 bar"推进 (常见做法)
        if bucket > c.openTime / periodMs then
          val closedBar = c.copy(closed = true)
          completed += closedBar
          while completed.size > maxBars do completed.removeHead()
          onBarClosed(closedBar)
          cur = Some(Candle(bucket * periodMs, price, price, price, price, qty, closed = false))
        else
          cur = Some(c.copy(
            high = math.max(c.high, price),
            low = math.min(c.low, price),
            close = price,
            volume = c.volume + qty,
          ))
    cur.foreach(onBarUpdated)

  /** 已收盘 K 线 (最旧在前、最新在后)，最多 maxBars 根 */
  def bars: collection.Seq[Candle] = completed

  /** 盘中 (未收盘) K 线 */
  def current: Option[Candle] = cur

  /** 已收盘 bar 的收盘价序列 */
  def closes: collection.Seq[Double] = completed.map(_.close)
