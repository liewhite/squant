package strategy.volsell.live
import strategy.volsell.logic.VolSell

import com.github.plokhotnyuk.jsoniter_scala.core.*
import com.github.plokhotnyuk.jsoniter_scala.macros.*

import java.nio.file.{Files, Path}

/** 卖方期权腿的**公共调参** (Bybit/OKX 共用, SSOT)。映射到 [[VolSell.Config]] + runNow。
  *
  * @param symbol     标的 symbol (Bybit: ETHUSDT; OKX: 基础币 ETH, 拼 ETH-quote-SWAP 取 K 线)
  * @param baseCoin   期权基础币 (取期权链 instFamily/baseCoin, 如 ETH)
  * @param targetDays 目标到期天数 (选最接近的 ATM 跨式, 默认 21)
  * @param gridHigh   本周 RV 较上周**上升**时的卖出倍数 (默认 2×)
  * @param gridLow    本周 RV 较上周**下降**时的卖出倍数 (默认 1×)
  * @param baseQty    单腿基准张数 (默认 1, 小仓)
  * @param maxQty     单腿张数硬上限 (sanity, 缺省=baseQty×3)；超出整体跳过+告警, 防 scale bug
  * @param bars2w     算 RV 的 5min K 线根数 (默认 2 周=4032)
  * @param runNow     启动即决策一次 (不等周五), 一次性场景用; 默认 false
  */
final case class SellTuning(
    symbol: String,
    baseCoin: String,
    targetDays: Int = 21,
    gridHigh: Double = 2.0,
    gridLow: Double = 1.0,
    baseQty: Double = 1.0,
    maxQty: Option[Double] = None,
    bars2w: Int = 2 * 7 * 24 * 12,
    runNow: Boolean = false,
):
  /** 转交易所无关的 [[VolSell.Config]]; maxQty 缺省回落到 baseQty×3 (与原 env 默认一致) */
  def toVolSellConfig: VolSell.Config =
    VolSell.Config(symbol, baseCoin, targetDays, gridHigh, gridLow, baseQty, bars2w, maxQty.getOrElse(baseQty * 3))

/** Bybit 卖方配置 (JSON)。**含 API 密钥 -> 配置文件 chmod 600 且勿入库** (已 .gitignore)。 */
final case class BybitSellConfig(
    apiKey: String,
    apiSecret: String,
    tuning: SellTuning,
    testnet: Boolean = false,
)

/** OKX 卖方配置 (JSON)。OKX 比 Bybit 多 passphrase 与计价币 quote。**含密钥, 同上保护**。 */
final case class OkxSellConfig(
    apiKey: String,
    apiSecret: String,
    passphrase: String,
    tuning: SellTuning,
    quote: String = "USDT",
    simulated: Boolean = false,
)

object SellConfig:
  private given bybitCodec: JsonValueCodec[BybitSellConfig] = JsonCodecMaker.make
  private given okxCodec: JsonValueCodec[OkxSellConfig] = JsonCodecMaker.make

  def loadBybit(path: String): Either[String, BybitSellConfig] = load(path)
  def loadOkx(path: String): Either[String, OkxSellConfig] = load(path)

  /** 从 JSON 文件加载配置; 文件缺失/解析失败 -> Left(原因) (不静默)。 */
  private def load[T](path: String)(using JsonValueCodec[T]): Either[String, T] =
    try Right(readFromArray[T](Files.readAllBytes(Path.of(path))))
    catch case e: Throwable => Left(s"读配置 $path 失败: ${e.getMessage}")
