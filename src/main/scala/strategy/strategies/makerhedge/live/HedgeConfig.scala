package strategy.strategies.makerhedge.live

import com.github.plokhotnyuk.jsoniter_scala.core.*
import com.github.plokhotnyuk.jsoniter_scala.macros.*

import java.nio.file.{Files, Path}

/** 永续 delta 对冲腿的**公共调参** (Bybit/OKX 共用, SSOT)。缺省字段用默认值 (jsoniter 自动回填)。
  *
  * @param symbol      标的 symbol (Bybit: ETHUSDT; OKX: 基础币 ETH, 框架按 ETH-quote-SWAP 转换)
  * @param ccy         现货修正与 greeks 的币种 (期权基础币, 如 ETH)
  * @param klineBar    ATR/均线预热的 K 线粒度 (Bybit: "60"=60min; OKX: "1H")
  * @param greeksPollMs 期权净 greeks 轮询间隔 (ms); 陈旧阈值 = 4× 此值
  * @param offset      maker 挂单价相对 BBO 的偏移比例
  * @param requoteMs   maker 重挂间隔 (ms)
  * @param tightAtr    顺势方向对冲带宽度 (ATR 倍数; AsymHedgeBand.trendSideMult, 卖方取紧)
  * @param looseAtr    逆势方向对冲带宽度 (ATR 倍数; AsymHedgeBand.counterTrendMult, 卖方取松)
  * @param maxHedgeQty 单笔对冲张数硬上限 (sanity, 超出不下单+告警)
  */
final case class HedgeTuning(
    symbol: String,
    ccy: String,
    klineBar: String,
    greeksPollMs: Long = 3000,
    offset: Double = 0.0001,
    requoteMs: Long = 5000,
    tightAtr: Double = 1.0,
    looseAtr: Double = 2.0,
    maxHedgeQty: Double = 5.0,
):
  /** [[klineBar]] 换算成毫秒 —— 与它是**同一个事实**的两种表示。
    *
    * 预热取数用粒度串 (交易所要), K 线序列聚合用毫秒 (策略要)。从前只有串, 策略那边就用了
    * 构造参数的默认值 1h —— 配置改成半小时的话, 预热数据会被按 1h 打时间戳喂进 1h 序列,
    * ATR/均线静默算错。粒度只配一处, 毫秒由它派生。
    *
    * 两家的串格式不同 (Bybit 是分钟数 `"60"`, OKX 是 `"1H"`), 所以两种都认。
    * 认不出就抛: 配置写错该在启动时大声失败。
    */
  def klineBarMs: Long =
    val m = 60_000L
    klineBar match
      case s if s.nonEmpty && s.forall(_.isDigit) => s.toLong * m // Bybit: 分钟数
      case "1m"                                   => m
      case "3m"                                   => 3 * m
      case "5m"                                   => 5 * m
      case "15m"                                  => 15 * m
      case "30m"                                  => 30 * m
      case "1H"                                   => 60 * m
      case "2H"                                   => 120 * m
      case "4H"                                   => 240 * m
      case "6H"                                   => 360 * m
      case "12H"                                  => 720 * m
      case "1D"                                   => 1440 * m
      case other =>
        sys.error(s"无法识别的 K 线粒度 '$other' —— Bybit 用分钟数 (如 \"60\"), OKX 用 \"1H\"/\"30m\" 这类串")

/** Bybit 永续对冲配置 (JSON)。**含 API 密钥 -> 配置文件务必 chmod 600 且勿入库** (已 .gitignore)。 */
final case class BybitHedgeConfig(
    apiKey: String,
    apiSecret: String,
    tuning: HedgeTuning,
    testnet: Boolean = false,
)

/** OKX 永续对冲配置 (JSON)。OKX 比 Bybit 多 passphrase 与计价币 quote。**含密钥, 同上保护**。 */
final case class OkxHedgeConfig(
    apiKey: String,
    apiSecret: String,
    passphrase: String,
    tuning: HedgeTuning,
    quote: String = "USDT",
    simulated: Boolean = false,
)

object HedgeConfig:
  private given bybitCodec: JsonValueCodec[BybitHedgeConfig] = JsonCodecMaker.make
  private given okxCodec: JsonValueCodec[OkxHedgeConfig] = JsonCodecMaker.make

  def loadBybit(path: String): Either[String, BybitHedgeConfig] = load(path)
  def loadOkx(path: String): Either[String, OkxHedgeConfig] = load(path)

  /** 从 JSON 文件加载配置; 文件缺失/解析失败 -> Left(原因) (不静默)。 */
  private def load[T](path: String)(using JsonValueCodec[T]): Either[String, T] =
    try Right(readFromArray[T](Files.readAllBytes(Path.of(path))))
    catch case e: Throwable => Left(s"读配置 $path 失败: ${e.getMessage}")
