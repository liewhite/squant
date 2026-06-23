package strategy.utils.backtest

import hft.backtest.{BacktestEngine, BinanceDataKind, BinanceHistory, BsGreeksConfig, BsGreeksSource, MarketDataSource, TradeBboAugmentSource}
import hft.domain.{Exchange, Position, Symbol, SymbolMeta}
import hft.engine.StrategyRunner
import hft.messaging.{EventData, IncomeEvent}
import hft.sim.{FillRecorder, SimConfig}
import hft.strategy.Strategy
import strategy.utils.hedge.{HedgeExecution, LimitRepegHedgeExecution, MarketHedgeExecution}
import sttp.client4.SyncBackend

import java.nio.file.Path
import java.time.{LocalDate, ZoneOffset}

/** 一次 short-vol 对冲回测的参数 (单只 ATM 短跨式, 持有到 end+1)。
  *
  * `strategyFactory` 注入对冲策略 (而非写死某一种)：runner 只负责合成 greeks/撮合/拆解，**对冲规则
  * 由策略决定** (TargetDelta / Breakout 等，开放封闭、不在 runner 里 flag 分发)。用工厂而非实例：
  * 策略含可变状态，每次 [[ShortVolHedgeRunner.run]] 须拿全新实例 (批量实验复用同一 params 多次跑不串状态)。
  */
final case class ShortVolParams(
    start: LocalDate,
    end: LocalDate,
    /** 对冲策略工厂: (交易所, 永续 symbol, greeks 币种, 执行器) => 策略。每次 run 产新实例。 */
    strategyFactory: (Exchange, Symbol, String, HedgeExecution) => Strategy,
    /** 手续费率 (maker=taker 同值, 回测简化) */
    feeRate: Double,
    /** true=市价(taker, 附零价差 BBO 撮合) / false=限价 3s 追价(maker) */
    useMarket: Boolean,
    /** 卖方报价 IV (年化), 决定权利金与 VRP */
    impliedVol: Double = 0.5,
    /** 跨式份数 (负=卖方 short straddle) */
    straddles: Double = -10.0,
    /** 宽跨价外宽度 (0=ATM 跨式; >0=宽跨, call/put 行权 = 首价·(1±w)) */
    strangleWidthPct: Double = 0.0,
    /** 行情->策略延迟 (ms)：交易所事件到策略收到的时延 */
    exchangeToStrategyDelayMs: Long = 100,
    /** 下单->交易所延迟 (ms)：策略下单到柜台撮合的时延 */
    orderToExchangeDelayMs: Long = 50,
    /** Some=把成交写入该 CSV (CLI 单跑用); None=不落盘 (批量实验用) */
    recorderPath: Option[Path] = None,
)

/** 资金曲线采样点 (小时下采样)。
  *
  * `totalEquity` = 对冲腿净值 (含初始本金 + 已/未实现 + 手续费) + 期权腿 MTM (跨式现值 − 进场权利金)，
  * 即账户**完整净值**随时间的轨迹。`price` 为采样时刻最新标的价，便于与 buy&hold/价格走势对照。
  */
final case class EquityPoint(ts: Long, totalEquity: Double, price: Double, optionPnl: Double, hedgeEquity: Double)

/** 回测两腿 P&L 拆解 (USDT)。
  *
  * **关键不变量**: [[optionPnl]] 只取决于 (价路径, IV, tenor)，**与对冲规则无关**——同一周不同对冲
  * 配置下 optionPnl 必相等。故对冲规则之间的差异**全部**体现在 [[hedgePnl]] (对冲腿: gamma 滑点 + 手续费)。
  */
final case class ShortVolOutcome(
    start: LocalDate,
    end: LocalDate,
    atmStrike: Double,
    lastPx: Double,
    marketEvents: Long,
    fills: Int,
    optionPnl: Double,
    hedgePnl: Double,
    realizedPnl: Double,
    endPositions: Vector[Position],
    /** 小时下采样的完整净值曲线 (供多周期资金曲线对比 / 最大回撤计算) */
    equityCurve: Vector[EquityPoint] = Vector.empty,
):
  def totalPnl: Double = optionPnl + hedgePnl
  def netMovePct: Double = (lastPx / atmStrike - 1.0) * 100.0

/** short-vol 对冲回测的**可复用、策略无关运行核心** (SSOT)。
  *
  * 从注入的 trade 源合成单只 ATM 短跨式 greeks ([[BsGreeksSource]])，跑由 `params.strategyFactory`
  * 注入的对冲策略 (TargetDelta / Breakout 等，runner 不感知规则)，返回两腿 P&L 拆解。
  * CLI ([[ShortVolHedgeBacktest]]) 与批量结构性实验 ([[StructuralEdgeExperiment]]) 共用此核心，
  * 保证口径一致、改一处即全改。
  *
  * trade 源与 symbolMetas **注入**而非内部构造：便于实验复用 (metas 只拉一次) 与单测 (喂假数据源)。
  */
object ShortVolHedgeRunner:
  val Symbol: Symbol = "ETHUSDT"
  val Ccy: String = "ETH"
  val Exchange_ : Exchange = Exchange.Binance
  val RiskFreeRate: Double = 0.0
  val InitialBalanceUsdt: Double = 100_000.0

  /** 用真实 Binance 历史 trades 构造一个可重复调用的 trade 源工厂 (回测层负责下载/缓存)。 */
  def tradeSourceFactory(backend: SyncBackend, start: LocalDate, end: LocalDate): () => MarketDataSource =
    () => BinanceHistory.source(backend, Seq(Symbol), start, end, kinds = Seq(BinanceDataKind.Trades))

  /** 拉取合约规格 (ETHUSDT 永续)；离线/被封锁取不到时回退已知离线兜底规格 (使回测可离线复现)。 */
  def fetchSymbolMetas(backend: SyncBackend): Map[(Exchange, Symbol), SymbolMeta] =
    val publicClient = hft.exchange.binance.BinanceClient(backend, credentials = None)
    val fallback = Map(
      (Exchange_, Symbol) -> SymbolMeta(Exchange_, Symbol, tickSize = 0.01, sizeStep = 0.001, minOrderSize = 0.001, contractSize = 1.0)
    )
    scala.util.Try(publicClient.fetchAllSymbolMetas()).toOption.flatMap(_.toOption) match
      case Some(metas) => metas.map(m => (m.exchange, m.symbol) -> m).toMap
      case None =>
        System.err.println(s"[warn] fetch symbol metas failed; 用离线兜底规格 $fallback")
        fallback

  /** 跑一次回测，返回两腿拆解。`tradeSourceFactory` 每次调用须产出独立的新迭代器。 */
  def run(
      symbolMetas: Map[(Exchange, Symbol), SymbolMeta],
      tradeSourceFactory: () => MarketDataSource,
      params: ShortVolParams,
  ): ShortVolOutcome =
    // 以回测起点首个真实成交价作为 ATM 行权价 (peek 仅加载首日)
    val atmStrike = tradeSourceFactory()
      .events()
      .collectFirst { case IncomeEvent(_, _, EventData.MarketTradeUpdate(t)) => t.price }
      .getOrElse(sys.error(s"no market data for $Symbol [${params.start} .. ${params.end}]"))

    // 单只 ATM 短跨式，到期 = 回测结束日+1 (tenor ≈ 回测周期, 全程持有不滚动)
    val expiryMs = params.end.plusDays(1).atStartOfDay.toInstant(ZoneOffset.UTC).toEpochMilli
    val greeksConfig = BsGreeksConfig(
      exchange = Exchange_,
      ccy = Ccy,
      underlyingSymbol = Symbol,
      straddles = params.straddles,
      impliedVol = params.impliedVol,
      expiry = expiryMs,
      riskFreeRate = RiskFreeRate,
      spotHolding = 0.0,
      emitIntervalMs = 1000,
      strangleWidthPct = params.strangleWidthPct,
    )
    val greeksSource = BsGreeksSource(tradeSourceFactory(), greeksConfig)
    // 市价对冲需 BBO 撮合取对手价 -> 附加零价差 BBO; 限价走 trade-print 无需 BBO
    val source = if params.useMarket then TradeBboAugmentSource(greeksSource) else greeksSource

    val execution: HedgeExecution =
      if params.useMarket then MarketHedgeExecution(Exchange_, Symbol)
      else LimitRepegHedgeExecution(Exchange_, Symbol, repegMs = 3000)
    val strategy = params.strategyFactory(Exchange_, Symbol, Ccy, execution)
    val runner = StrategyRunner.backtest(strategy, symbolMetas)

    // 旁路观察者: 跟踪最新标的价与时间，用于回测末期期权腿 MTM 估值
    var lastMid = atmStrike
    var lastTs = 0L
    // 完整净值曲线 (小时下采样): 每个 AccountInfoUpdate 取对冲腿净值 + 当时期权腿 MTM
    val curve = scala.collection.mutable.ArrayBuffer.empty[EquityPoint]
    var lastCurveTs = Long.MinValue
    val CurveIntervalMs = 3_600_000L
    val priceObserver: IncomeEvent => Unit = ev =>
      ev.data match
        case EventData.MarketTradeUpdate(t) => lastMid = t.price; lastTs = t.timestamp
        case EventData.AccountInfoUpdate(_, info) =>
          if lastCurveTs == Long.MinValue || ev.exchangeTs - lastCurveTs >= CurveIntervalMs then
            lastCurveTs = ev.exchangeTs
            val optPnl = greeksSource.optionPnl(lastMid, ev.exchangeTs)
            curve += EquityPoint(ev.exchangeTs, info.equity + optPnl, lastMid, optPnl, info.equity)
        case _ => ()

    val recorderOpt = params.recorderPath.map(FillRecorder(_))
    recorderOpt.foreach(_.open())
    try
      val engine = BacktestEngine(
        exchange = Exchange_,
        source = source,
        runners = Seq(runner),
        config = SimConfig(
          exchangeToStrategyDelayMs = params.exchangeToStrategyDelayMs,
          orderToExchangeDelayMs = params.orderToExchangeDelayMs,
          initialBalanceUsdt = InitialBalanceUsdt,
          makerFeeRate = params.feeRate,
          takerFeeRate = params.feeRate,
        ),
        observers = priceObserver +: recorderOpt.map(_.onEvent).toSeq,
      )
      val result = engine.run()
      ShortVolOutcome(
        start = params.start,
        end = params.end,
        atmStrike = atmStrike,
        lastPx = lastMid,
        marketEvents = result.marketEvents,
        fills = result.fills,
        optionPnl = greeksSource.optionPnl(lastMid, lastTs), // short: 衰减为正 (收权利金)
        hedgePnl = result.finalEquity - result.initialBalance, // 对冲腿 (含未实现 + 手续费)
        realizedPnl = result.realizedPnl,
        endPositions = result.positions,
        equityCurve = curve.toVector,
      )
    finally recorderOpt.foreach(_.close())
