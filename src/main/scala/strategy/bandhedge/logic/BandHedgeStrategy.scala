package strategy.bandhedge.logic

import strategy.utils.hedge.{HedgeBand, HedgeCtx}

import hft.domain.*
import hft.exchange.SubscriptionKind
import hft.indicator.{Atr, KlineSeries, Macd, RealizedVol, Sma}
import hft.messaging.{EventData, IncomeEvent, StateManager}
import hft.strategy.{OutcomeEvent, Strategy}

/** **价格主导、delta 定量、带宽可插拔** 的期权买方 (long-gamma) 对冲核心。
  *
  * 机制 (各变体共享、不变)：以最新价为中心 center，维护逐笔聚合的小时 K 线 (ATR/MACD/RV 指标)；
  * 当 `px−center` 越过 [[HedgeBand]] 给出的上/下带宽时，立即市价 take 当前净 delta 使账户回中性，
  * 成交后把 center 移到成交价。净 delta = 期权 delta(greeks，含现货修正) + 永续持仓。
  *
  * 策略 (何时对冲、上下是否对称) 完全由注入的 [[band]] 决定——这是寻找买方 edge 的唯一旋钮：
  * 期权腿盈亏由 IV/路径/到期固定，只能靠对冲腿的触发时机与不对称来改变总盈亏。新 edge 思路 =
  * 新 [[HedgeBand]] 实现，核心不动 (开放封闭)。退化：band 给对称恒定带宽即为
  * [[strategy.atrtakehedge.logic.AtrTakeHedgeStrategy]] 基线。
  *
  * **需要盘口**：market 单到撮合需 BBO，故订阅 [[SubscriptionKind.BBO]]；回测以
  * [[hft.backtest.TradePrintBboSource]] 把 trades 合成零价差盘口。onEvent 由框架单线程串行调用，
  * 内部可变状态无需同步。
  */
final class BandHedgeStrategy(
    exchange: Exchange,
    symbol: Symbol,
    ccy: String,
    /** 对冲带策略 (何时对冲 / 上下是否对称)——edge 旋钮 */
    band: HedgeBand,
    atrPeriodBars: Int = 14,
    /** MACD 柱方向强度回看的已收盘根数 (histBias 的 trendBars) */
    macdTrendBars: Int = 2,
    rvShortWindowBars: Int = 24,
    rvLongWindowBars: Int = 168,
    /** 均线周期 (根)，供 maBias = sign(px − MA) (默认 MA20) */
    maSmaPeriod: Int = 20,
    barIntervalMs: Long = 3_600_000L,
    minHedgeQty: Quantity = 0.001,
) extends Strategy:

  private val klines =
    new KlineSeries(barIntervalMs, math.max(math.max(atrPeriodBars * 4, rvLongWindowBars + 8), 64))
      with Atr with Macd with RealizedVol with Sma:
      override protected def atrPeriod: Int = atrPeriodBars
      override protected def rvShortBars: Int = rvShortWindowBars
      override protected def rvLongBars: Int = rvLongWindowBars
      override protected def smaPeriod: Int = maSmaPeriod

  /** 对冲中心价 (NaN = 尚未初始化，首个行情设为现价) */
  private var center: Double = Double.NaN

  override def publicStreams: Map[Exchange, Set[SubscriptionKind]] =
    Map(exchange -> Set(SubscriptionKind.BBO(symbol)))

  override def orderTimeoutMs: Long = 5000

  override def onEvent(event: IncomeEvent, state: StateManager): Vector[OutcomeEvent] =
    event.data match
      case EventData.BboUpdate(b) if b.exchange == exchange && b.symbol == symbol =>
        val px = b.midPrice
        klines.update(b.timestamp, px)
        if center.isNaN then center = px
        hedge(px, state)
      case EventData.GreeksUpdate(g) if g.exchange == exchange && g.ccy == ccy =>
        state.symbolState(symbol).flatMap(_.bbo(exchange)).map(b => hedge(b.midPrice, state)).getOrElse(Vector.empty)
      case _ => Vector.empty

  private def hedge(px: Price, state: StateManager): Vector[OutcomeEvent] =
    (for
      symbolState <- state.symbolState(symbol)
      greeks <- state.greeks(exchange, ccy) // greeks 与 cashBal 均到达才动作
      atr <- klines.atr                     // ATR 未预热 -> 不动作 (gating)
      if atr > 0.0 && !center.isNaN
    yield
      // 信号未就绪时以中性默认填充, 各带在预热期自然退化为对称基线
      val maBias = klines.sma.fold(0)(m => math.signum(px - m).toInt)
      val ctx = HedgeCtx(px, center, atr, klines.volRatio.getOrElse(1.0), klines.histBias(macdTrendBars), maBias)
      val (upBand, downBand) = band.bands(ctx)
      val crossed = (px - center > upBand) || (center - px > downBand)
      if !crossed then Vector.empty
      else
        val netDelta = greeks.delta + symbolState.positionSize(exchange)
        val qty = math.abs(netDelta)
        if qty < minHedgeQty then Vector.empty
        else
          val side = if netDelta > 0 then Side.Short else Side.Long // 净多 -> 卖, 净空 -> 买
          center = px // 成交后中心移到成交价 (market@touch、delay=0)，并防止本笔重复触发
          Vector(
            OutcomeEvent.PlaceOrders(
              Vector(Order("", exchange, symbol, side, OrderType.Market, qty, reduceOnly = false, clientOrderId = "")),
              f"band_hedge | $side netDelta=$netDelta%.4f qty=$qty%.4f px=$px%.2f atr=$atr%.2f up=$upBand%.2f down=$downBand%.2f macdBias=${ctx.macdBias} maBias=${ctx.maBias} volR=${ctx.volRatio}%.2f",
            )
          )
    ).getOrElse(Vector.empty)
