package hft.backtest

import org.slf4j.LoggerFactory

import java.nio.file.{Files, Path}

/** 历史数据缓存：纯 key -> bytes 存储，与"从哪下载"解耦。
  *
  * key 直接镜像数据源路径 (如 `futures/um/daily/bookTicker/BTCUSDT/BTCUSDT-bookTicker-2024-01-01.zip`)。
  * 当前提供本地文件系统实现 [[LocalFsDataCache]]，未来可加 OSS 实现而不动下载/回放代码。
  */
trait DataCache:
  def get(key: String): Option[Array[Byte]]
  def put(key: String, data: Array[Byte]): Unit

/** 本地文件系统缓存：key 作为 root 下的相对路径落盘。 */
final class LocalFsDataCache(root: Path) extends DataCache:
  private val logger = LoggerFactory.getLogger(classOf[LocalFsDataCache])

  override def get(key: String): Option[Array[Byte]] =
    val path = root.resolve(key)
    if Files.isRegularFile(path) then
      logger.debug(s"cache hit: $key")
      Some(Files.readAllBytes(path))
    else None

  override def put(key: String, data: Array[Byte]): Unit =
    val path = root.resolve(key)
    Files.createDirectories(path.getParent)
    Files.write(path, data)
    logger.debug(s"cache put: $key (${data.length} bytes)")
