package strategy.utils.backtest

import strategy.utils.option.BybitOptionsClient
import sttp.client4.SyncBackend

import java.nio.file.{Files, Path}

/** Bybit 历史 IV 指数的**分页拉取 + 文件缓存** (回测数据层, SSOT)。
  *
  * Bybit `historical-volatility` 单次窗口 ≤30 天 -> 这里按 29 天分页覆盖 [startMs, endMs]，结果按
  * `{cacheDir}/bybit-iv/{base}-{quote}-{period}.csv` (列 `ts,iv`) 缓存; 重跑命中缓存的窗口不再打 API。
  *
  * 客户端只做单窗口取数 (单一职责), 分页/缓存/采样在此, 与 [[BybitOptionsClient.historicalIv]] 解耦。 */
object BybitIvHistory:
  private val logger = org.slf4j.LoggerFactory.getLogger(getClass)
  val HourMs = 3_600_000L
  val WindowMs = 29L * 24L * HourMs // 留 1 天余量, 避免触达 30 天硬上限被拒
  /** 一个窗口内缓存点数低于"理论小时点数×此比例"才重新拉取 (容忍交易所数据自然缺口)。 */
  val CoverageThreshold = 0.5

  /** 纯逻辑: 把 [startMs, endMs] 按 29 天切窗, 返回**需要补取**的窗口 (覆盖率 < 阈值的)。
    * `coveredCount(a, b)` = 已有缓存落在 [a, b] 内的点数。便于不打网络地单测分页/覆盖率判断。 */
  def missingWindows(startMs: Long, endMs: Long, coveredCount: (Long, Long) => Int): Seq[(Long, Long)] =
    val out = scala.collection.mutable.ArrayBuffer.empty[(Long, Long)]
    var cursor = startMs
    while cursor < endMs do
      val winEnd = math.min(cursor + WindowMs, endMs)
      val expected = math.max(1L, (winEnd - cursor) / HourMs)
      if coveredCount(cursor, winEnd) < expected * CoverageThreshold then out += ((cursor, winEnd))
      cursor = winEnd
    out.toSeq

  /** 拉取 [startMs, endMs] 的小时级 IV 指数 (年化小数), 缺失窗口按需补取并落盘。返回升序去重序列。
    *
    * @param baseCoin  期权基础币 (ETH/BTC)
    * @param quoteCoin 计价币 (ETH 期权为 USDT, 否则返回空)
    * @param period    恒定期限分桶天数 (7/14/21/30/...)
    */
  def hourly(
      backend: SyncBackend,
      baseCoin: String,
      quoteCoin: String,
      period: Int,
      startMs: Long,
      endMs: Long,
      cacheDir: String,
  ): Vector[(Long, Double)] =
    val client = BybitOptionsClient(backend, credentials = None)
    val file = Path.of(cacheDir, "bybit-iv", s"$baseCoin-$quoteCoin-$period.csv")
    val cache = scala.collection.mutable.TreeMap.from(loadCache(file))

    var fetched = 0
    val errors = scala.collection.mutable.ArrayBuffer.empty[String]
    missingWindows(startMs, endMs, (a, b) => cache.range(a, b + 1).size).foreach { case (winStart, winEnd) =>
      client.historicalIv(baseCoin, quoteCoin, period, winStart, winEnd) match
        case Right(pts) =>
          pts.foreach { case (ts, iv) => cache.update(ts, iv) }
          fetched += pts.size
        case Left(err) => errors += s"[$winStart..$winEnd] $err"
    }
    // 接口报错与"该窗口本就无数据"不同 -> 显式上报, 不静默吞掉 (否则回测会建立在残缺 IV 上)
    if errors.nonEmpty then
      logger.error(s"[BybitIvHistory] $baseCoin-$quoteCoin-$period: ${errors.size} 个窗口拉取失败, IV 序列将有缺口 (受影响周会被下游剔除): ${errors.take(3).mkString("; ")}")
    if fetched > 0 then saveCache(file, cache.toSeq)

    cache.range(startMs, endMs + 1).toVector

  /** 取 ≤ tsMs 的最近一个 IV (进场时可观测, **严格无前视**: tsMs 之前无点则 None, 不回退到未来点)。 */
  def sampleAt(series: Vector[(Long, Double)], tsMs: Long): Option[Double] =
    series.takeWhile(_._1 <= tsMs).lastOption.map(_._2)

  private def loadCache(file: Path): Seq[(Long, Double)] =
    if !Files.isRegularFile(file) then Seq.empty
    else
      import scala.jdk.CollectionConverters.*
      val lines = Files.readAllLines(file).asScala
      lines.flatMap { line =>
        line.split(",", 2) match
          case Array(ts, iv) => ts.trim.toLongOption.zip(iv.trim.toDoubleOption)
          case _             => None
      }.toSeq

  private def saveCache(file: Path, points: Seq[(Long, Double)]): Unit =
    Files.createDirectories(file.getParent)
    val sb = new StringBuilder("ts,iv\n")
    points.sortBy(_._1).foreach { case (ts, iv) => sb.append(ts).append(',').append(iv).append('\n') }
    Files.writeString(file, sb.toString)
