package hft.engine

import hft.domain.*
import hft.messaging.{EventBus, IncomeEvent}
import hft.strategy.{OutcomeEvent, Strategy}
import org.slf4j.LoggerFactory
import ox.{Ox, fork}
import ox.channels.Source

/** 策略执行器：每个策略一个 Executor，独占一个虚拟线程串行消费事件。
  *
  * 仅是 [[StrategyRunner]] 的实盘/模拟盘**传输薄壳**：从 income 总线收事件、按订阅过滤、
  * 调用 runner 产出信号、发布到 outcome 总线。策略逻辑与精度转换全在 [[StrategyRunner]]，
  * 与回测共用 (回测在单线程虚拟时间循环里直接调用同一个 runner)。
  */
final class Executor(
    strategy: Strategy,
    symbolMetas: Map[(Exchange, Symbol), SymbolMeta],
    outcomeBus: EventBus[OutcomeEvent],
):
  private val logger = LoggerFactory.getLogger(classOf[Executor])

  private val runner = StrategyRunner(strategy, symbolMetas)

  /** 启动执行循环 */
  def run(events: Source[IncomeEvent])(using Ox): Unit =
    fork {
      logger.info(s"Executor started: subscriptions=${runner.subscriptions}")
      while true do
        val event = events.receive()
        if runner.accepts(event) then runner.onEvent(event, nowMs).foreach(outcomeBus.publish)
    }
    ()
