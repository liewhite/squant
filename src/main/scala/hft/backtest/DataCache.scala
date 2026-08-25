package hft.backtest

import org.slf4j.LoggerFactory

import java.io.{BufferedInputStream, InputStream}
import java.nio.file.{Files, Path}

/** 历史数据缓存：纯 key -> 字节流存储，与"从哪下载"解耦。
  *
  * key 直接镜像数据源路径 (如 `futures/um/daily/bookTicker/BTCUSDT/BTCUSDT-bookTicker-2024-01-01.zip`)。
  * 当前提供本地文件系统实现 [[LocalFsDataCache]]，未来可加 OSS 实现而不动下载/回放代码。
  *
  * 读取面是**流**而非 `Array[Byte]`：单日单标的的 bookTicker zip 可达数十 MB，多标的同时回放
  * 时若各自把整包驻留内存，常驻开销随标的数线性膨胀。流式读取让常驻内存与文件大小无关。
  */
trait DataCache:
  /** 打开缓存内容；None = 未缓存。**调用方负责关闭**返回的流。 */
  def open(key: String): Option[InputStream]
  def put(key: String, data: Array[Byte]): Unit

/** 本地文件系统缓存：key 作为 root 下的相对路径落盘。 */
final class LocalFsDataCache(root: Path) extends DataCache:
  private val logger = LoggerFactory.getLogger(classOf[LocalFsDataCache])

  override def open(key: String): Option[InputStream] =
    val path = root.resolve(key)
    Option.when(Files.isRegularFile(path)):
      logger.debug(s"cache hit: $key")
      BufferedInputStream(Files.newInputStream(path))

  override def put(key: String, data: Array[Byte]): Unit =
    val path = root.resolve(key)
    Files.createDirectories(path.getParent)
    // 先写临时文件再原子改名: 下载中途崩溃不会在缓存里留下一个"看起来命中"的半截文件。
    // 临时名带唯一后缀 —— 并发补同一个 key 时两者各写各的, 不会互相把文件抽走
    val tmp = path.resolveSibling(s"${path.getFileName}.${ProcessHandle.current().pid()}-${Thread.currentThread().threadId()}.tmp")
    Files.write(tmp, data)
    Files.move(tmp, path, java.nio.file.StandardCopyOption.REPLACE_EXISTING)
    logger.debug(s"cache put: $key (${data.length} bytes)")
