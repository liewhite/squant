package strategy.monitor

import com.github.plokhotnyuk.jsoniter_scala.core.*
import com.github.plokhotnyuk.jsoniter_scala.macros.*
import hft.domain.{Exchange, Symbol}
import hft.exchange.AccountMonitor

import java.nio.file.{Files, Path}
import scala.util.control.NonFatal

/** 一家交易所的接入信息 —— **按交易所分成不同的 case**, 不是三家字段的并集。
  *
  * 并集形态 (`passphrase`/`quote`/`accountType` 都是 `Option`, 再按交易所 `match` 分发) 试过,
  * 问题在于**校验只能靠人记得写**: OKX 缺 passphrase 会被拦住, 而 `quote` 填在 Binance 上、
  * `accountType` 填在 OKX 上都被静默忽略 —— 配置者会一直以为它生效了, 而这正是这个仓库
  * 反复在消灭的东西。分成不同的 case 之后, 填错字段是 JSON 解析失败, 连校验分支都不需要。
  *
  * `symbols` / `pollMs` 是三家共有的, 由 [[VenueConfig]] 统一给。
  */
enum VenueConfig(
    /** 建议用**只读**密钥, 理由见 [[DashboardConfig]] */
    val apiKey: String,
    val apiSecret: String,
    /** **即使空仓也要显示一行**的标的。账上实际持有的会自动出现 (见 [[AccountMonitor]]),
      * 所以这里不必列全; 列在这里的意义是"我在看的就是它"。 */
    val symbols: Set[Symbol],
    /** 是否同时订阅这些标的的盘口。true 才看得到价格与跨所比价; false 只看账户, 少几条 WS 连接。 */
    val watchMarket: Boolean,
    val pollMs: Long,
):
  case Binance(k: String, s: String, syms: Set[Symbol], mkt: Boolean, poll: Long)
      extends VenueConfig(k, s, syms, mkt, poll)

  /** OKX 独有 `passphrase`; `quote` 留空则用客户端自己的默认值, 不在这里抄第二份。 */
  case Okx(k: String, s: String, passphrase: String, syms: Set[Symbol], mkt: Boolean, poll: Long, quote: Option[String] = None)
      extends VenueConfig(k, s, syms, mkt, poll)

  /** Bybit 独有 `accountType`; 留空同上。 */
  case Bybit(k: String, s: String, syms: Set[Symbol], mkt: Boolean, poll: Long, accountType: Option[String] = None)
      extends VenueConfig(k, s, syms, mkt, poll)

  def exchange: Exchange = this match
    case _: Binance => Exchange.Binance
    case _: Okx     => Exchange.Okx
    case _: Bybit   => Exchange.Bybit

  /** 逐家校验。在**任何连接之前**跑完 —— 配置错误该在启动时大声失败, 而不是连上两家之后
    * 才发现第三家的密钥是空的。 */
  def validated: VenueConfig =
    require(apiKey.nonEmpty, s"$exchange 缺 apiKey")
    require(apiSecret.nonEmpty, s"$exchange 缺 apiSecret")
    this match
      case o: Okx => require(o.passphrase.nonEmpty, "OKX 的 passphrase 不能为空")
      case _      => ()
    require(
      pollMs >= AccountMonitor.MinPollMs,
      s"$exchange 的 pollMs=$pollMs 太快, 下限 ${AccountMonitor.MinPollMs}ms (账户接口有权重限制)",
    )
    this

/** 只读实盘看板的配置。**含 API 密钥 -> chmod 600 且勿入库。**
  *
  * ## 强烈建议用只读密钥
  *
  * 这个进程为每家装的是 [[AccountMonitor]] —— 它不注册任何命令处理能力, 所以**下单指令在这个
  * 进程里连路由都没有**, 那是结构保证而不是约定。
  *
  * 但进程里毕竟握着能签名的密钥。交易所侧的**只读密钥**是另一道、也是更外的一道保证:
  * 三家都支持 (Binance 的 "Enable Reading"、OKX 的 read-only、Bybit 的 Read-Only),
  * 而监控要做的事 (查持仓/挂单/钱包/净值) 只读密钥全都做得到。两道一起上。
  *
  * @param exchanges **只填要看的那几家**: 没填的不会连、不会占资源。
  *                  Hyperliquid 不在可选之列 —— 框架只有它的公共行情客户端, 给不出账户读数;
  *                  它是 [[VenueConfig]] 里压根没有那个 case, 而不是运行期再拒绝。
  */
final case class DashboardConfig(
    exchanges: Vector[VenueConfig],
    port: Int,
    host: String = "127.0.0.1",
):
  /** 校验并按交易所排序 (顺序确定, 启动日志才可对比)。 */
  def validated: Vector[VenueConfig] =
    require(exchanges.nonEmpty, "至少要配一家交易所, 否则这个看板没有任何账户可看")
    require(port > 0, s"port 须为正, 实为 $port")
    val checked = exchanges.map(_.validated)
    val venues = checked.map(_.exchange)
    require(venues.distinct.sizeIs == venues.size, s"同一家交易所配了两次: ${venues.mkString(",")}")
    checked.sortBy(_.exchange.toString)

object DashboardConfig:
  given JsonValueCodec[DashboardConfig] =
    JsonCodecMaker.make(
      CodecMakerConfig
        // JSON 里用 "venue" 指明是哪家 —— 每家只认自己那些字段。
        .withDiscriminatorFieldName(Some("venue"))
        // **不跳过未知字段。** jsoniter 默认是跳过的, 于是 `quote` 填在 Binance 上、
        // 或者把 `passphrase` 拼成 `passphrease`, 都会被静默忽略, 而配置者一直以为它生效了。
        // 配置文件里的一个错字应该大声失败 —— 按交易所分 case 只解决了"类型上不该有这个字段",
        // 解析宽容度是另一半, 两半都要。
        .withSkipUnexpectedFields(false)
    )

  def load(path: String): Either[String, DashboardConfig] =
    try Right(readFromArray[DashboardConfig](Files.readAllBytes(Path.of(path))))
    catch case NonFatal(e) => Left(s"读配置 $path 失败: ${e.getMessage}")
