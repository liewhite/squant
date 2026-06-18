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

/** 增量 K 线聚合 + 技术指标工具 (单线程访问，无需同步)。
  *
  * 不断喂入逐笔 (ts, price, qty)，按 [[periodMs]] 分桶维护 K 线序列 (最多缓存 [[maxBars]] 根已收盘 bar)：
  *   - **最新一根是盘中的**：同周期内的成交持续刷新其 high/low/close/volume；
  *   - **跨周期时**：把上一根 bar **固定** (closed=true) 连同其指标 EMA 基线冻结、存入已收盘序列，
  *     再开一根新的盘中 bar 动态维护。
  *
  * 指标 (MACD) 对盘中根**动态**更新：以上一根收盘时冻结的 EMA 为基线，叠加当前盘中收盘价推算
  * 当前 MACD/柱；盘中根收盘时该动态值即成为新的冻结基线。预热不足 (已收盘 bar 数 < slow+signal)
  * 时 [[macdDirection]] 返回 0。
  *
  * 设计为可扩展：已收盘 K 线序列 ([[bars]]/[[closes]]) 对外可见，后续可在其上加更多指标。
  */
final class KlineSeries(
    val periodMs: Long,
    val maxBars: Int,
    macdFast: Int = 12,
    macdSlow: Int = 26,
    macdSignal: Int = 9,
):
  private val completed = mutable.ArrayDeque.empty[Candle]
  private var cur: Option[Candle] = None

  // 冻结的 EMA 基线 (截至最近一根已收盘 bar)
  private var fEmaFast = 0.0
  private var fEmaSlow = 0.0
  private var fEmaSignal = 0.0
  private var closedCount = 0

  // 盘中根的动态 MACD
  private var dMacd = 0.0
  private var dSignal = 0.0
  private var dHist = 0.0

  /** 喂入一笔成交 */
  def update(ts: Long, price: Double, qty: Double = 0.0): Unit =
    val bucket = ts / periodMs
    cur match
      case None =>
        cur = Some(Candle(bucket * periodMs, price, price, price, price, qty, closed = false))
      case Some(c) =>
        if bucket > c.openTime / periodMs then
          freeze(c.close)                 // 固定上一根 bar 的指标基线
          completed += c.copy(closed = true)
          while completed.size > maxBars do completed.removeHead()
          cur = Some(Candle(bucket * periodMs, price, price, price, price, qty, closed = false))
        else
          cur = Some(c.copy(
            high = math.max(c.high, price),
            low = math.min(c.low, price),
            close = price,
            volume = c.volume + qty,
          ))
    recomputeDynamic()

  /** 收盘一根 bar：推进冻结 EMA 基线 (首根播种) */
  private def freeze(close: Double): Unit =
    if closedCount == 0 then
      fEmaFast = close
      fEmaSlow = close
      fEmaSignal = 0.0 // macd@首根 = 0
    else
      fEmaFast = ema(fEmaFast, close, macdFast)
      fEmaSlow = ema(fEmaSlow, close, macdSlow)
      fEmaSignal = ema(fEmaSignal, fEmaFast - fEmaSlow, macdSignal)
    closedCount += 1

  /** 用冻结基线 + 盘中收盘价推算当前动态 MACD */
  private def recomputeDynamic(): Unit =
    cur.foreach { c =>
      if closedCount == 0 then
        dMacd = 0.0; dSignal = 0.0; dHist = 0.0
      else
        val df = ema(fEmaFast, c.close, macdFast)
        val ds = ema(fEmaSlow, c.close, macdSlow)
        dMacd = df - ds
        dSignal = ema(fEmaSignal, dMacd, macdSignal)
        dHist = dMacd - dSignal
    }

  private def ema(prev: Double, x: Double, period: Int): Double =
    val k = 2.0 / (period + 1)
    x * k + prev * (1 - k)

  // ==================== 查询 ====================

  /** 已收盘 K 线 (最旧在前、最新在后)，最多 maxBars 根 */
  def bars: collection.Seq[Candle] = completed

  /** 盘中 (未收盘) K 线 */
  def current: Option[Candle] = cur

  /** 已收盘 bar 的收盘价序列 (供其它指标计算) */
  def closes: collection.Seq[Double] = completed.map(_.close)

  /** 已收盘 bar 数 (预热进度) */
  def closedBars: Int = closedCount

  /** 当前 (含盘中根) MACD 柱值 */
  def macdHistogram: Double = dHist
  def macdLine: Double = dMacd
  def macdSignalLine: Double = dSignal

  /** 方向：+1 看多 (柱>0)、-1 看空 (柱<0)、0 预热不足或持平 */
  def macdDirection: Int =
    if closedCount < macdSlow + macdSignal then 0
    else if dHist > 0 then 1
    else if dHist < 0 then -1
    else 0
