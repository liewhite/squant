package strategy.scan

import hft.actor.{Actor, ActorContext}
import hft.domain.{Exchange, Timestamp}
import hft.event.{AnyEvent, Event, Interest, Topics}
import org.slf4j.LoggerFactory

/** 扫描器驱动（actor 形态）—— **只看不交易**的研究/监控入口。
  *
  * 薄壳：订阅全市场成交、按节拍问 [[TakerFlowDetector]]、把异动发上总线。检测语义一行都不在这里。
  *
  * 与 [[FlowAnomalyStrategy]] 的区别只是**定位**：本类不交易任何标的，故以
  * `Interest.All(Topics.Trade)` 收全市场且不产生任何标的归属（[[hft.event.Interest.All]] 不派生
  * instruments），代价是它自己不会让行情流动起来 —— 需要装配方调
  * [[hft.engine.Engine.watchMarket]]，或者同进程里已有策略订了那些标的。
  *
  * 真要**据此下单**时用 [[FlowAnomalyStrategy]]：那是策略形态，声明即订阅、且能进回测。
  */
final class FlowScanner(
    exchange: Exchange,
    config: FlowScanConfig = FlowScanConfig(),
    rule: AnomalyRule = CrossSectionalMedianRule(),
) extends Actor:
  private val logger = LoggerFactory.getLogger(classOf[FlowScanner])
  private val detector = TakerFlowDetector(exchange, config, rule)
  private var ctx: ActorContext = scala.compiletime.uninitialized

  override def name: String = s"flow-scanner@$exchange"

  /** 行情全量收 —— 扫描器的输入就是"全市场"，它不该也无法预先知道该看哪些标的。 */
  override def interests: Set[Interest] = Set(Interest.All(Topics.Trade), Interest.All(Topics.Clock))

  override def onStart(context: ActorContext): Unit =
    ctx = context
    logger.info(s"flow scanner started on $exchange (预热 ${detector.warmupMs / 1000}s 后开始出信号)")

  override def onEvent(event: AnyEvent, now: Timestamp): Vector[AnyEvent] =
    event.as(Topics.Trade).foreach(detector.onTrade)
    event.as(Topics.Clock).toVector.flatMap { _ =>
      detector.evaluate(now).map { a =>
        logger.info(FlowScanner.describe(a))
        Event.local(FlowAnomalies, a)
      }
    }

  def stats: TakerFlowDetector.Stats = detector.stats

object FlowScanner:
  /** 人读的一行 —— 驱动与启动器共用，免得两处各拼一遍格式 */
  def describe(a: FlowAnomaly): String =
    f"[异动] ${a.symbol}%-14s ${a.side}%-5s residualZ=${a.residualZ}%7.1f " +
      f"(自身 z=${a.ownZ}%7.1f 大盘 z=${a.marketZ}%6.1f) " +
      f"净流向=${a.windowFlow}%,15.0f 名义额=${a.windowNotional}%,15.0f"
