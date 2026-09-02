package strategy.strategies.ivsellhedge.live

import strategy.strategies.ivsellhedge.logic.{PortfolioDelta, SellPlan, SigmaSource}
import strategy.utils.hedge.{DeltaBand, QuotePolicy, QuoteStyle}
import hft.domain.Coin

import com.github.plokhotnyuk.jsoniter_scala.core.*
import com.github.plokhotnyuk.jsoniter_scala.macros.*

import java.nio.file.{Files, Path}
import scala.util.control.NonFatal

/** IV 定量卖出 + delta 死区对冲的**全部调参** (SSOT)。缺省字段由 jsoniter 回填默认值。
  *
  * 分两段：`卖出腿` 归 [[OptionSellerActor]]，`对冲腿` 归 `DeltaHedgeStrategy`。
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
  * @param tightMult         顺势侧死区阈值的收紧系数 (× 预测波动范围)
  * @param looseMult         逆势侧死区阈值的放宽系数 (× 预测波动范围)
  * @param hedgeHorizonMinutes 预测波动范围的时间跨度 (分钟)：阈值 ∝ σ×√(horizon)
  * @param minTheta          死区阈值下限 (币本位)：σ 未就绪或极小时兜住"裸着敞口"
  * @param maxTheta          死区阈值上限 (币本位)：σ 异常放大时兜住"永不对冲"
  * @param sigmaSource       σ 的取数口径 ("realized" = 已实现波动)
  * @param fastBar           细粒度序列的 K 线粒度 (OKX 粒度串, 默认 "1m")。σ 与 ER 都建在它上面,
 *                          所以都能用历史 K 线预热, 开机即就绪
 * @param erPeriod          ER 的回看根数 (默认 10)。ER **只用于选报价方式**, 不再参与死区
 * @param rvBars            实现波动的回看根数 (默认 30)
  * @param macdBar           MACD 的 K 线粒度 (OKX 粒度串, 如 "1H")；预热与实时聚合共用这一个事实
  * @param macdFast          MACD 快线周期
  * @param macdSlow          MACD 慢线周期
  * @param macdSignal        MACD 信号线周期
  * @param trendErThreshold  ER 高于它按"单边"选报价方式 (跨价), 否则按"折返" (被动挂)
  * @param passiveOffset     被动挂单相对盘口的外移比例 (保证 PostOnly 不吃单)
  * @param passiveTtlMs      被动挂单的存活时间, 超时撤单重挂
  * @param crossOffset       跨价挂单相对盘口的让价比例
  * @param crossTtlMs        跨价挂单的存活时间
  * @param cancelConfirmMs   撤单确认等待上限 (超时视为撤单请求丢失, 重发)
  * @param riskFreeRate      BS 定价的无风险利率
  * @param minHedgeQty       最小对冲量 (低于它不动, 吸收死区边缘的微量反复触发)
  * @param maxHedgeQty       单笔对冲量硬上限 (sanity: 疑似 delta 计算 bug 时不下单 + 告警)
  * @param maxExposureStaleMs 敞口读数陈旧阈值; 缺省 = 4×publishExposureMs
  */
final case class IvSellTuning(
    symbol: String,
    baseCoin: String,
    ccy: String,
    // ---- 卖出腿 ----
    // 默认值全部取自 [[OptionSellerActor.Defaults]] —— **这里不再有第二份**。
    // 从前两处各写一份, 且在危险方向上不一致: 配置漏写 enableOpen 就真实下单、
    // 漏写 minStrikeDistance 就卖平值跨式。
    targetDays: Int = OptionSellerActor.Defaults.TargetDays,
    minTtlDays: Int = OptionSellerActor.Defaults.MinTtlDays,
    minStrikeDistance: Double = OptionSellerActor.Defaults.MinStrikeDistance,
    /** 起卖点 IV。**必填** —— 见字段文档: 给它默认值等于埋一个看着合理的错配置。 */
    ivStart: Double,
    ivQtyStart: Double = 3.0,
    ivQtySlope: Double = 1.0,
    ivQtyMax: Double = 10.0,
    minPremium: Double = OptionSellerActor.Defaults.MinPremium,
    maxSpreadRatio: Double = OptionSellerActor.Defaults.MaxSpreadRatio,
    maxOptionLeverage: Double = OptionSellerActor.Defaults.MaxOptionLeverage,
    enableOpen: Boolean = OptionSellerActor.Defaults.EnableOpen,
    publishExposureMs: Long = OptionSellerActor.Defaults.PublishExposureMs,
    refreshMarksMs: Long = OptionSellerActor.Defaults.RefreshMarksMs,
    sellIntervalMs: Long = OptionSellerActor.Defaults.SellIntervalMs,
    settleRounds: Int = OptionSellerActor.Defaults.SettleRounds,
    riskFreeRate: Double = PortfolioDelta.DefaultRate,
    // ---- 对冲腿 ----
    tightMult: Double = 0.5,
    looseMult: Double = 2.0,
    hedgeHorizonMinutes: Int = 30,
    minTheta: Double = 0.02,
    maxTheta: Double = 1.0,
    sigmaSource: String = "realized",
    fastBar: String = "1m",
    erPeriod: Int = 10,
    rvBars: Int = 30,
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

  /** 细粒度序列的 K 线粒度换算成毫秒 (同 [[macdBarMs]]: 粒度只配一处, 预热与实时聚合共用) */
  def fastBarMs: Long = IvSellTuning.barToMillis(fastBar)

  /** 敞口死区：阈值按预测波动范围定, 方向决定两侧不对称 (判据始终是真实敞口) */
  def deltaBand: DeltaBand = DeltaBand.volScaled(
    tightMult, looseMult, hedgeHorizonMinutes.toLong * 60_000L, Coin(minTheta), Coin(maxTheta))

  /** σ 来源。未知取值**抛错**而不是静默回退 —— 配错了只表现为对冲疏密不对, 没有别的症状 */
  def sigma: SigmaSource = sigmaSource.toLowerCase match
    case "realized" => SigmaSource.Realized
    case "iv"       => SigmaSource.ImpliedVol
    case "max"      => SigmaSource.MaxOfBoth
    case other      => sys.error(s"未知的 sigmaSource '$other' (支持 realized / iv / max)")

  /** 报价方式的选择：价格平缓 -> 被动慢挂；走单边 -> 跨价追单 */
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
    /** **目前只对期权腿生效, 置 true 会被启动器拒绝启动。**
      * 永续的 REST/WS 客户端工厂里写死了主网地址 —— 半个开关比没有开关更危险
      * (期权去模拟盘、对冲腿在主网下真单)。 */
    simulated: Boolean = false,
)

object IvSellHedgeConfig:
  private given codec: JsonValueCodec[OkxIvSellHedgeConfig] = JsonCodecMaker.make

  /** 从 JSON 文件加载; 文件缺失/解析失败 -> Left(原因) (不静默) */
  def loadOkx(path: String): Either[String, OkxIvSellHedgeConfig] =
    try Right(readFromArray[OkxIvSellHedgeConfig](Files.readAllBytes(Path.of(path))))
    catch case NonFatal(e) => Left(s"读配置 $path 失败: ${e.getMessage}")
