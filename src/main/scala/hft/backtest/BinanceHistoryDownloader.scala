package hft.backtest

import org.slf4j.LoggerFactory
import sttp.client4.*
import sttp.model.Uri

import scala.concurrent.duration.*

/** 从 data.binance.vision 拉取历史数据 zip，带 [[DataCache]] 透写缓存。
  *
  * 流程：先查缓存命中即返回；未命中则 HTTP 下载、落缓存、返回字节。
  * 404 视为"该日无此数据"返回 None (而非错误)，其余非 2xx 抛异常 fail-fast。
  */
final class BinanceHistoryDownloader(
    backend: SyncBackend,
    cache: DataCache,
    baseUrl: String = BinanceHistoryDownloader.BaseUrl,
):
  private val logger = LoggerFactory.getLogger(classOf[BinanceHistoryDownloader])

  /** 取 key 对应的 zip 字节；None 表示数据源 404 (该日无数据)。 */
  def fetch(key: String): Option[Array[Byte]] =
    cache.get(key) match
      case some @ Some(_) => some
      case None =>
        val url = s"$baseUrl/$key"
        logger.info(s"downloading $url")
        val response = basicRequest
          .get(Uri.unsafeParse(url))
          .readTimeout(60.seconds)
          .response(asByteArrayAlways)
          .send(backend)
        if response.code.isSuccess then
          cache.put(key, response.body)
          Some(response.body)
        else if response.code.code == 404 then
          logger.warn(s"no data (404): $key")
          None
        else sys.error(s"download failed ${response.code}: $url")

object BinanceHistoryDownloader:
  val BaseUrl = "https://data.binance.vision/data"

  /** U 本位合约 (futures/um) 每日数据的 key。镜像官方目录结构。
    * @param kind 如 "bookTicker" / "trades"
    * @param date YYYY-MM-DD
    */
  def dailyKey(kind: String, symbol: String, date: String): String =
    s"futures/um/daily/$kind/$symbol/$symbol-$kind-$date.zip"
