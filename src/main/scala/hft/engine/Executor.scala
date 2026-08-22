package hft.engine

import hft.domain.*
import hft.event.{AnyEvent, Event, EventBus, Subscription}
import hft.strategy.{OrderIntent, Strategy}
import org.slf4j.LoggerFactory
import ox.{Ox, fork}

/** 策略执行器：每个策略一个 Executor，独占一个虚拟线程串行消费事件。
  *
  * 仅是 [[StrategyRunner]] 的实盘/模拟盘**传输薄壳**：从总线收事件、调用 runner 产出信号、
  * 把信号作为 [[OrderIntent]] 事件发回同一条总线。策略逻辑与精度转换全在 [[StrategyRunner]]，
  * 与回测共用 (回测在单线程虚拟时间循环里直接调用同一个 runner)。
  *
  * 不再需要收下事件后自行过滤：总线按 [[Subscription]] 精确投递，到达邮箱的就是该收的。
  */
final class Executor(
    strategy: Strategy,
    symbolMetas: Map[(Exchange, Symbol), SymbolMeta],
    bus: EventBus,
):
  private val logger = LoggerFactory.getLogger(classOf[Executor])

  private val runner = StrategyRunner(strategy, symbolMetas)

  /** 本策略的订阅范围。引擎据此向交易所订阅行情、做启动对齐 */
  def subscription: Subscription = runner.subscription

  /** 订阅总线并启动执行循环。
    *
    * 订阅在 [[run]] 里完成而不是由调用方传入事件流：订阅范围是 runner 的私有知识
    * (策略声明 + 框架补齐)，让调用方拼一遍就又多了一处会与判据错开的声明。
    */
  def run()(using Ox): Unit =
    val events = bus.subscribe(runner.subscription.interests)
    fork {
      logger.info(s"Executor started: subscription=${runner.subscription.interests.size} interests, instruments=${runner.subscription.instruments}")
      while true do
        val event: AnyEvent = events.receive()
        runner.onEvent(event, nowMs).foreach(outcome => bus.publish(Event.local(OrderIntent, outcome)))
    }
    ()
