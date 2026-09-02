package strategy.strategies.crossspread.live

import hft.actor.{Actor, ActorContext}
import hft.domain.Timestamp
import hft.event.{AnyEvent, Event, Interest, Topics}
import org.slf4j.LoggerFactory
import strategy.strategies.crossspread.logic.*

/** 跨所价差异动监控器（actor 形态）—— **只看不交易**。
  *
  * 薄壳：把盘口喂给 [[CrossSpreadDetector]]、按时钟节拍问它、把结论打日志并发上总线。
  * 检测语义一行都不在这里。
  *
  * ## 为什么是 actor 而不是 Strategy
  *
  * 用 `Strategy` 声明一个标的的行情就等于**交易**它：框架会独占登记 (账户, 标的)、
  * 补齐私有回报、做启动仓位对齐。而这里要同时盯三家所上百个标的、一个都不下单 ——
  * 走策略形态会把这些标的全部占住，之后任何真要交易它们的策略都起不来。
  *
  * 代价是它自己不会让行情流动起来：装配方要调
  * [[hft.engine.Engine.watchMarket]] 把这些标的的盘口订上，否则它订了个空。
  *
  * ## 采样节拍来自时钟
  *
  * 判定挂在 [[Topics.Clock]] 上而不是每条盘口都算一遍：价差中枢是窗口尺度的事实，
  * 逐条评估要把上百个对全过一遍，白烧 CPU。真正的采样间隔是
  * `max(时钟间隔, config.sampleMs)` —— 时钟比采样慢时，均线的时间刻度由时钟决定。
  */
final class CrossSpreadMonitor(detector: CrossSpreadDetector) extends Actor:
  private val logger = LoggerFactory.getLogger(classOf[CrossSpreadMonitor])

  override def name: String = "cross-spread-monitor"

  override def interests: Set[Interest] =
    Set(Interest.Keyed(Topics.Bbo, detector.instruments), Interest.All(Topics.Clock))

  override def onStart(context: ActorContext): Unit =
    logger.info(
      s"跨所价差监控启动: ${detector.pairs.size} 个价差对 / ${detector.instruments.size} 个标的, " +
        s"预热 ${detector.config.warmupMs / 1000}s 后开始出信号"
    )

  override def onEvent(event: AnyEvent, now: Timestamp): Vector[AnyEvent] =
    event.as(Topics.Bbo).foreach(detector.onQuote)
    event.as(Topics.Clock).toVector.flatMap { _ =>
      val result = detector.evaluate(now)
      result.rejections.foreach { r =>
        logger.warn(
          f"[配对剔除] ${r.pair.ticker}%-6s ${r.pair.a} 与 ${r.pair.b} 价差 ${r.spreadBps}%.0fbp " +
            "超出上限, 视为不是同一个标的或合约乘数不同, 不再参与监控"
        )
      }
      result.dislocations.map { d =>
        logger.warn(SpreadDislocation.describe(d))
        Event.stamped(SpreadDislocations, d, now, now)
      }
    }

  /** 观测用快照，供启动器打心跳 */
  def stats: CrossSpreadDetector.Stats = detector.stats
