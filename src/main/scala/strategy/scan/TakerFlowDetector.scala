package strategy.scan

import hft.domain.{Exchange, MarketTrade, Symbol, Timestamp}

import scala.collection.mutable

/** 检测器参数。窗口/基线属于**怎么观测**，阈值属于**怎么判定**（后者在 [[AnomalyRule]] 里）。
  *
  * @param bucketMs        分桶时长
  * @param windowBuckets   滚动窗口桶数（窗口 = bucketMs × windowBuckets）
  * @param baselineSamples 基线样本数（基线覆盖 = bucketMs × baselineSamples）
  * @param cooldownMs      同一标的两次报警的最小间隔。一次异动会持续整个窗口，
  *                        不设冷却就会每个节拍重复报同一件事
  */
final case class FlowScanConfig(
    bucketMs: Long = 1000,
    windowBuckets: Int = 30,
    baselineSamples: Int = 600,
    cooldownMs: Long = 60_000,
)

/** 全市场 taker 流向异动检测器 —— **纯逻辑**：无框架依赖、无 IO、不读墙钟。
  *
  * 时间一律由调用方传入，因此可脱离引擎逐笔喂数据做同步单测，也因此能被两种驱动共用：
  * 实盘/研究用 [[FlowScanner]]（actor），实盘交易与回测用 [[FlowAnomalyStrategy]]（策略）。
  * 驱动只负责"从哪拿事件、往哪发结论"，检测语义只有这一份。
  *
  * 职责边界：本类管**观测与编排**（分桶累计、推进时间、冷却），不管**判定**——
  * 判定是 [[AnomalyRule]] 的事，注入进来即可替换。
  */
final class TakerFlowDetector(
    val exchange: Exchange,
    config: FlowScanConfig = FlowScanConfig(),
    rule: AnomalyRule = CrossSectionalMedianRule(),
):
  private val flows = mutable.Map.empty[Symbol, SymbolTakerFlow]
  private val lastAlertAt = mutable.Map.empty[Symbol, Timestamp]
  private var lastEvalAt: Timestamp = Long.MinValue
  private var tradesSeen: Long = 0L
  private var alertsEmitted: Long = 0L

  /** 记一笔公共成交。非本交易所的成交直接忽略。 */
  def onTrade(t: MarketTrade): Unit =
    if t.exchange == exchange then
      tradesSeen += 1
      // isBuyerMaker=true 表示买方是挂单方 -> 主动方是卖 -> taker 卖
      flows
        .getOrElseUpdate(t.symbol, SymbolTakerFlow(config.bucketMs, config.windowBuckets, config.baselineSamples))
        .onTrade(t.timestamp, t.qty.notional(t.price).value, takerBuy = !t.isBuyerMaker)

  /** 一个节拍：推进所有标的的时间轴、交给规则判定、按冷却过滤后产出异动。
    *
    * 评估放在节拍上而非每笔成交：逐笔评估要把全市场过一遍，白烧 CPU；
    * 而异动本就是窗口尺度的事实，一个节拍的延迟无关紧要。
    */
  def evaluate(now: Timestamp): Vector[FlowAnomaly] =
    if lastEvalAt != Long.MinValue && now - lastEvalAt < config.bucketMs then Vector.empty
    else
      lastEvalAt = now
      // 没有成交的标的也要推进到当下 —— 否则它们的窗口停留在最后一笔成交时，
      // 一个早已冷掉的标的会带着几分钟前的流量参与横截面。
      flows.valuesIterator.foreach(_.advanceTo(now))

      val observations = flows.iterator.flatMap { (symbol, flow) =>
        flow.zScore.map(z => FlowObservation(symbol, z, flow.windowFlow, flow.windowNotional))
      }.toVector

      rule.detect(observations).flatMap { v =>
        val cooling = lastAlertAt.get(v.symbol).exists(last => now - last < config.cooldownMs)
        Option.unless(cooling) {
          lastAlertAt(v.symbol) = now
          alertsEmitted += 1
          FlowAnomaly(
            exchange = exchange,
            symbol = v.symbol,
            side = v.side,
            residualZ = v.residualZ,
            ownZ = v.ownZ,
            marketZ = v.marketZ,
            windowFlow = flows(v.symbol).windowFlow,
            windowNotional = flows(v.symbol).windowNotional,
            timestamp = now,
          )
        }
      }

  /** 观测用快照：已消费的成交数、跟踪的标的数、基线就绪的标的数、累计报警数。
    * 基线未就绪前一条都不会报，这个数字用来区分"还没到点"与"接线错了"。 */
  def stats: TakerFlowDetector.Stats =
    TakerFlowDetector.Stats(
      tradesSeen = tradesSeen,
      symbolsTracked = flows.size,
      symbolsReady = flows.count((_, f) => f.baselineSize >= config.baselineSamples),
      alertsEmitted = alertsEmitted,
    )

  /** 预热完成所需的时长（毫秒）—— 装配方用它决定何时开始相信信号 */
  def warmupMs: Long = config.bucketMs * config.baselineSamples

object TakerFlowDetector:
  final case class Stats(tradesSeen: Long, symbolsTracked: Int, symbolsReady: Int, alertsEmitted: Long)
