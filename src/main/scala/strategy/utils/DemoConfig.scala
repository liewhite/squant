package strategy.utils

import com.github.plokhotnyuk.jsoniter_scala.core.*
import com.github.plokhotnyuk.jsoniter_scala.macros.*

import java.nio.file.{Files, Path}

/** demo 通用配置: **可选** API 密钥 + live 开关。密钥仅用于接私有流/真实下单, 缺省则走公共行情/dry-run。
  * **含密钥 -> chmod 600 且勿入库** (conf 下真实 json 已 .gitignore)。 */
final case class DemoConfig(apiKey: String = "", apiSecret: String = "", live: Boolean = false):
  def hasCreds: Boolean = apiKey.nonEmpty && apiSecret.nonEmpty

object DemoConfig:
  private given codec: JsonValueCodec[DemoConfig] = JsonCodecMaker.make

  /** 可选加载: 文件不存在 -> 空配置 (公共行情/dry-run, 保留 demo 零配置可跑);
    * 文件存在但解析失败 -> 抛 (显式失败, 不静默)。 */
  def loadOrEmpty(path: String): DemoConfig =
    val p = Path.of(path)
    if !Files.exists(p) then DemoConfig()
    else readFromArray[DemoConfig](Files.readAllBytes(p))
