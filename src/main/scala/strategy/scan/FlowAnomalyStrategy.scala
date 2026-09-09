package strategy.scan

import hft.domain.{Exchange, Instrument, Symbol, Timestamp}
import hft.event.{AnyEvent, Topics}
import hft.strategy.{Strategy, StrategyContext, StrategyHandlers}

/** 异动之后做什么 —— **"策略待定"的插座**。
  *
  * 检测与下单是两件事：同一个检测器可以配不同的响应（追进去 / 反着做 / 只做市），
  * 不同响应也可以复用同一个检测器。焊在一起就没法拿同一批信号对比两种做法，
  * 更没法单独回测检测器本身。
  *
  * 实现方拿到的是异动本身加上标准的 [[StrategyContext]]（能读只读状态、能构造下单意图），
  * 返回的事件由框架统一处理 —— 与任何策略处理器的契约完全一致。
  */
trait AnomalyResponse:
  def onAnomaly(anomaly: FlowAnomaly, ctx: StrategyContext, now: Timestamp): Vector[AnyEvent]

object AnomalyResponse:
  /** 默认：只出信号、不下单。
    *
    * 交易逻辑未定之前，这是唯一诚实的默认值 —— 一个"看起来合理"的内置响应会被当成
    * 经过验证的东西直接用上实盘（同 [[hft.perf.PromotionPolicy]] 默认 NeverPromote 的理由）。
    */
  val SignalOnly: AnomalyResponse = (_, _, _) => Vector.empty

/** 全市场 taker 流向异动策略（策略形态）—— 实盘与回测共用的驱动。
  *
  * 薄壳：声明标的、把成交喂给 [[TakerFlowDetector]]、按节拍取异动、交给 [[AnomalyResponse]]
  * 决定下不下单，并把异动本身 `emit` 到总线供外部观察。检测语义与响应语义都不在这里。
  *
  * ## 为什么是一个实例覆盖全宇宙，而不是"发现异动再起实例"
  *
  * "没有交易信号"与"策略实例不存在"是两回事。启动即声明全部标的、没被点名就不发单，
  * 于是不需要任何动态起停：[[hft.engine.InstrumentClaims]] 不构成障碍（只有一个实例，
  * 独占这些标的正当），启动对齐一次做完，而且**这个形态今天就能回测** ——
  * [[hft.backtest.BacktestEngine]] 收的是固定的一组 runner，正好匹配。
  *
  * 若单实例吞吐不够，把宇宙切成不相交的若干份、起多个实例即可 ——
  * 独占登记恰好保证切片不重叠，不需要动框架。
  *
  * @param universe 本实例负责的标的。声明即表示**可能交易**它们，这是诚实的：
  *                 任何一个被点名都会下单。
  */
final class FlowAnomalyStrategy(
    exchange: Exchange,
    universe: Set[Symbol],
    config: FlowScanConfig = FlowScanConfig(),
    rule: AnomalyRule = CrossSectionalMedianRule(),
    response: AnomalyResponse = AnomalyResponse.SignalOnly,
) extends Strategy:
  require(universe.nonEmpty, "universe 不能为空")

  private val detector = TakerFlowDetector(exchange, config, rule)
  private val instruments: Set[Instrument] = universe.map(Instrument(exchange, _))

  override def handlers: StrategyHandlers = StrategyHandlers.empty
    .market(Topics.Trade, instruments) { (t, _, _) =>
      detector.onTrade(t)
      Vector.empty
    }
    .onClock { (ctx, now) =>
      detector.evaluate(now).flatMap { anomaly =>
        // 先把信号本身发出去（外部可观察、可被别的策略消费），再让响应决定下不下单。
        // 顺序有意：即便响应决定不动作，信号也留下了痕迹。
        ctx.emit(FlowAnomalies, anomaly) +: response.onAnomaly(anomaly, ctx, now)
      }
    }

  /** 观测用快照（预热进度 / 累计报警），供启动器打心跳 */
  def stats: TakerFlowDetector.Stats = detector.stats
  def warmupMs: Long = detector.warmupMs
