package hft.backtest.binance

import hft.backtest.DataCache
import org.slf4j.LoggerFactory
import sttp.client4.*
import sttp.model.Uri

import java.io.{ByteArrayInputStream, InputStream}
import scala.concurrent.duration.*

/** 从 data.binance.vision 拉取历史数据 zip，带 [[DataCache]] 透写缓存。
  *
  * 流程：先查缓存命中即返回；未命中则 HTTP 下载、落缓存、**从刚落盘的缓存重开流**。
  * 404 视为"该日无此数据"返回 None (而非错误)，其余非 2xx 抛异常 fail-fast。
  *
  * 返回**流**而非字节数组：常驻内存与文件大小无关 (见 [[DataCache]])。下载时整包在内存中转
  * (sttp 同步后端的响应形态)，但落盘后即改从缓存读 —— 否则那份字节会被回放游标一直握到当天
  * 回放结束，冷缓存首跑 (最需要流式的那一次) 反而把整日数据钉在内存里。
  */
final class BinanceHistoryDownloader(
    backend: SyncBackend,
    cache: DataCache,
    baseUrl: String = BinanceHistoryDownloader.BaseUrl,
):
  private val logger = LoggerFactory.getLogger(classOf[BinanceHistoryDownloader])

  /** 打开 key 对应的 zip 流；None 表示数据源 404 (该日无数据)。**调用方负责关闭**。 */
  def fetch(key: String): Option[InputStream] =
    cache.open(key) match
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
          // 从缓存重开: 让 response.body 立即可回收。缓存若没真的落盘 (如只读介质) 就退回内存
          // 字节 —— 结果仍正确, 但要告警, 因为整日回放的内存特征会与预期不同
          cache.open(key).orElse {
            logger.warn(s"cache did not persist $key; falling back to in-memory bytes for this replay")
            Some(ByteArrayInputStream(response.body))
          }
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
