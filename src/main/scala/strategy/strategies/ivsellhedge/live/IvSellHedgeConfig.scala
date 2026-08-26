package strategy.strategies.ivsellhedge.live

import strategy.strategies.ivsellhedge.logic.SellPlan
import strategy.utils.hedge.{QuotePolicy, QuoteStyle}

import com.github.plokhotnyuk.jsoniter_scala.core.*
import com.github.plokhotnyuk.jsoniter_scala.macros.*

import java.nio.file.{Files, Path}
import scala.util.control.NonFatal

/** IV 定量卖出 + KAMA 死区对冲的**全部调参** (SSOT)。缺省字段由 jsoniter 回填默认值。
  *
  * 分两段：`卖出腿` 归 [[OptionSellerActor]]，`对冲腿` 归 `DeltaKamaHedgeStrategy`。
  *
  * @param symbol            标的 symbol (OKX: 基础币 ETH; 内部拼 ETH-<quote>-SWAP)
  * @param baseCoin          期权基础币 (期权链 instFamily = `<baseCoin>-USD`)
  * @param ccy               现货余额与敞口读数的币种 (币本位期权即基础币)
  * @param targetDays        目标到期天数 (相对今天的**滑动**量)
  * @param minTtlDays        剩余期限低于它的到期一律排除 (当日到期权利金≈0 而 gamma 极大)
  * @param minStrikeDistance 两腿行权价距现价的最小比例 (0.02 = 2%)
  * @param ivStart           起卖点 IV (0.2 = 20%)：低于它一张不卖。**必填**——参考实现里 0 表示
 *                          "不启用缩放"，这里的语义却是"从 IV=0 起线性放大"(IV 稍高就顶到上限)，
 *                          语义相反，给默认值等于埋一个看着合理的错配置
  * @param ivQtyStart        起卖量 (到起卖点即卖的张数)
  * @param ivQtySlope        每 1 个波动率点 (1% IV) 增加的张数
  * @param ivQtyMax          目标张数上限 (兜住极端行情与异常 markVol)
  * @param minPremium        权利金门槛 (按 bid 判)
  * @param maxSpreadRatio    ask/bid 上限
  * @param maxOptionLeverage 期权杠杆率上限 (Σ|张数|×ctVal×现价 / 净值)
  * @param enableOpen        false = 只打印卖出意图不下单 (对冲仍真实运行)
  * @param publishExposureMs 敞口发布间隔 (delta 每这么久按最新价重算一次)
  * @param refreshMarksMs    IV / 持仓 / 现金的刷新间隔
  * @param sellIntervalMs    卖出对账间隔。**必须显著大于成交结算延迟** (IOC 无挂单可跟, 判据是已结算持仓)
 * @param settleRounds      提交过卖单后强制静默的轮数 (给持仓落地留时间, 防同一缺口被连卖两轮)
  * @param deltaThreshold    对冲死区基准阈值 (币本位, 如 0.3 ETH)
  * @param macdTightenRatio  MACD 逆势侧的收紧系数 (0.5 = 减半; 1.0 = 不收紧)
  * @param kamaBucketMs      KAMA 的一步多长 (默认 5 分钟)
  * @param macdBar           MACD 的 K 线粒度 (OKX 粒度串, 如 "1H")；预热与实时聚合共用这一个事实
  * @param offset            对冲挂单相对盘口的外移比例 (保证 PostOnly 不吃单)
  * @param requoteMs         对冲挂单未成交的重挂间隔
  * @param minHedgeQty       最小对冲量 (低于它不动, 兼作 KAMA 滞后导致的微量触发的吸收器)
  * @param maxHedgeQty       单笔对冲量硬上限 (sanity: 疑似 delta 计算 bug 时不下单 + 告警)
  * @param maxExposureStaleMs 敞口读数陈旧阈值; 缺省 = 4×publishExposureMs
  */
final case class IvSellTuning(
    symbol: String,
    baseCoin: String,
    ccy: String,
    // ---- 卖出腿 ----
    targetDays: Int = 3,
    minTtlDays: Int = 1,
    minStrikeDistance: Double = 0.02,
    ivStart: Double,
    ivQtyStart: Double = 1.0,
    ivQtySlope: Double = 1.0,
    ivQtyMax: Double = 10.0,
    minPremium: Double = 0.0,
    maxSpreadRatio: Double = 1.1,
    maxOptionLeverage: Double = 1.0,
    enableOpen: Boolean = false,
    publishExposureMs: Long = 1000,
    refreshMarksMs: Long = 5000,
    sellIntervalMs: Long = 5000,
    settleRounds: Int = 1,
    riskFreeRate: Double = 0.0,
    // ---- 对冲腿 ----
    deltaThreshold: Double = 0.3,
    macdTightenRatio: Double = 0.5,
    kamaBucketMs: Long = 300_000,
    kamaErPeriod: Int = 10,
    kamaFast: Int = 2,
    kamaSlow: Int = 30,
    macdBar: String = "1H",
    macdFast: Int = 12,
    macdSlow: Int = 26,
    macdSignal: Int = 9,
    trendErThreshold: Double = 0.5,
    passiveOffset: Double = 0.0002,
    passiveTtlMs: Long = 60_000,
    crossOffset: Double = 0.0005,
    crossTtlMs: Long = 1000,
    cancelConfirmMs: Long = 3000,
    minHedgeQty: Double = 0.001,
    maxHedgeQty: Double = 5.0,
    maxExposureStaleMs: Option[Long] = None,
):
  /** 敞口陈旧阈值缺省 = 4×发布间隔 (连续几次拉取失败即暂停对冲, 不按过期 delta 乱挂) */
  def exposureStaleMs: Long = maxExposureStaleMs.getOrElse(publishExposureMs * 4)

  /** MACD 的 K 线粒度换算成毫秒 —— 与 [[macdBar]] 是**同一个事实**的两种表示，
    * 所以只配一处、这里派生。分两个字段配的话，预热用的粒度与实时聚合的粒度可以配得不一致，
    * 而症状只是"MACD 方向偶尔和图上不一样"。 */
  def macdBarMs: Long = IvSellTuning.barToMillis(macdBar)

  /** 报价方式的选择：敞口平缓 -> 被动慢挂；走单边 -> 跨价追单 */
  def quotePolicy: QuotePolicy = QuotePolicy.byEfficiency(
    trendErThreshold,
    calm = QuoteStyle.passive(passiveOffset, passiveTtlMs),
    trending = QuoteStyle.crossing(crossOffset, crossTtlMs),
  )

  def toSellerConfig: OptionSellerActor.Config =
    OptionSellerActor
      .Config(
        symbol = symbol,
        baseCoin = baseCoin,
        ccy = ccy,
        targetDays = targetDays,
        minTtlMs = minTtlDays.toLong * SellPlan.DayMs,
        minStrikeDistance = minStrikeDistance,
        ivQty = SellPlan.IvQty(ivStart, ivQtyStart, ivQtySlope, ivQtyMax),
        minPremium = minPremium,
        maxSpreadRatio = maxSpreadRatio,
        maxOptionLeverage = maxOptionLeverage,
        enableOpen = enableOpen,
        publishExposureMs = publishExposureMs,
        refreshMarksMs = refreshMarksMs,
        sellIntervalMs = sellIntervalMs,
        settleRounds = settleRounds,
        riskFreeRate = riskFreeRate,
      )
      .validated

object IvSellTuning:
  /** OKX K 线粒度 -> 毫秒。未知粒度**抛错**而不是猜一个默认值：猜错的后果是 MACD 跑在一个
    * 谁都没想要的周期上，且没有任何外在症状。 */
  def barToMillis(bar: String): Long =
    val m = 60_000L
    bar match
      case "1m"  => m
      case "3m"  => 3 * m
      case "5m"  => 5 * m
      case "15m" => 15 * m
      case "30m" => 30 * m
      case "1H"  => 60 * m
      case "2H"  => 120 * m
      case "4H"  => 240 * m
      case "6H"  => 360 * m
      case "12H" => 720 * m
      case "1D"  => 1440 * m
      case other => sys.error(s"未知的 K 线粒度 '$other' (支持 1m/3m/5m/15m/30m/1H/2H/4H/6H/12H/1D)")

/** OKX 实盘配置 (JSON)。**含 API 密钥 -> 配置文件 chmod 600 且勿入库** (已 .gitignore)。 */
final case class OkxIvSellHedgeConfig(
    apiKey: String,
    apiSecret: String,
    passphrase: String,
    tuning: IvSellTuning,
    quote: String = "USDT",
    simulated: Boolean = false,
)

object IvSellHedgeConfig:
  private given codec: JsonValueCodec[OkxIvSellHedgeConfig] = JsonCodecMaker.make

  /** 从 JSON 文件加载; 文件缺失/解析失败 -> Left(原因) (不静默) */
  def loadOkx(path: String): Either[String, OkxIvSellHedgeConfig] =
    try Right(readFromArray[OkxIvSellHedgeConfig](Files.readAllBytes(Path.of(path))))
    catch case NonFatal(e) => Left(s"读配置 $path 失败: ${e.getMessage}")
