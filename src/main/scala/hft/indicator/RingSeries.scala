package hft.indicator

import scala.collection.mutable

/** 有界历史序列 (环形缓冲) —— 指标用来保存逐根**已收盘**的标量值，支持回看与单调性判定。
  *
  * 只保留最近 [[maxLen]] 个值 (最旧自动丢弃)。索引以 [[apply]] 从最新往回数 (0=最新)。
  */
final class RingSeries(val maxLen: Int):
  private val buf = mutable.ArrayDeque.empty[Double]

  def push(x: Double): Unit =
    buf += x
    while buf.size > maxLen do buf.removeHead()

  /** 清空 —— 用于"这段历史不再代表当下"的场合 (如数据断档后重新预热) */
  def clear(): Unit = buf.clear()

  def size: Int = buf.size
  def isEmpty: Boolean = buf.isEmpty
  def nonEmpty: Boolean = buf.nonEmpty

  /** 最新值 */
  def last: Option[Double] = buf.lastOption

  /** 从最新往回数第 back 个 (0=最新, 1=上一根...)，越界返回 None */
  def apply(back: Int): Option[Double] =
    if back < 0 || back >= buf.size then None else Some(buf(buf.size - 1 - back))

  /** 全部值，最旧 -> 最新 */
  def values: collection.Seq[Double] = buf

  /** 最近 n 个值，最旧 -> 最新 */
  def recent(n: Int): collection.Seq[Double] = buf.takeRight(n)

  /** 最近 n 个值是否都满足 pred (需至少 n 个值, 否则 false)；不分配中间集合。
    * 等价 `recent(n).sizeIs >= n && recent(n).forall(pred)`, 供逐笔热路径避免 takeRight 分配。 */
  def lastAll(n: Int)(pred: Double => Boolean): Boolean =
    if n < 1 || buf.size < n then false
    else
      var i = buf.size - n
      var ok = true
      while ok && i < buf.size do
        ok = pred(buf(i))
        i += 1
      ok

  /** 最近 n 步是否**严格递减** (需 >= n+1 个值)：v[t] < v[t-1] 连续 n 次。
    * 例：`falling(2)` = 连续 2 个周期递减 (最近 3 个值严格下行)。 */
  def falling(n: Int): Boolean = monotone(n)(_ < _)

  /** 最近 n 步是否**严格递增** */
  def rising(n: Int): Boolean = monotone(n)(_ > _)

  /** newer cmp older 连续成立 n 次 */
  private def monotone(n: Int)(cmp: (Double, Double) => Boolean): Boolean =
    n >= 1 && buf.size >= n + 1 && (0 until n).forall(k => cmp(buf(buf.size - 1 - k), buf(buf.size - 2 - k)))
