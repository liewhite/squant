package app.live.hedge

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
  * @param tightAtr    均线上方对冲带宽度 (ATR 倍数)
  * @param looseAtr    均线下方对冲带宽度 (ATR 倍数)
  * @param maxHedgeQty 单笔对冲张数硬上限 (sanity, 超出不下单+告警)
  */
final case class HedgeTuning(
    symbol: String,
    ccy: String,
    klineBar: String,
    greeksPollMs: Long = 3000,
    offset: Double = 0.0002,
    requoteMs: Long = 5000,
    tightAtr: Double = 1.0,
    looseAtr: Double = 2.0,
    maxHedgeQty: Double = 5.0,
)

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
