package strategy.strategies.bandhedge.logic

import strategy.utils.hedge.{HedgeBand, HedgeCtx}

import hft.domain.*
import hft.indicator.{Atr, KlineSeries, Macd, RealizedVol, Sma}
import hft.event.{AnyEvent, Topics}
import hft.strategy.{Strategy, StrategyContext, StrategyHandlers}

/** **价格主导、delta 定量、带宽可插拔** 的期权买方 (long-gamma) 对冲核心。
  *
  * 机制 (各变体共享、不变)：以最新价为中心 center，维护逐笔聚合的小时 K 线 (ATR/MACD/RV 指标)；
  * 当 `px−center` 越过 [[HedgeBand]] 给出的上/下带宽时，立即市价 take 当前净 delta 使账户回中性，
  * 成交后把 center 移到成交价。净 delta = 期权 delta(greeks，含现货修正) + 永续持仓。
  *
  * 策略 (何时对冲、上下是否对称) 完全由注入的 [[band]] 决定——这是寻找买方 edge 的唯一旋钮：
  * 期权腿盈亏由 IV/路径/到期固定，只能靠对冲腿的触发时机与不对称来改变总盈亏。新 edge 思路 =
  * 新 [[HedgeBand]] 实现，核心不动 (开放封闭)。退化：band 给对称恒定带宽即为
  * "对称恒定带宽"这条基线 (那个类已随不对称带的引入删除)。
  *
  * **需要盘口**：market 单到撮合需 BBO，故订阅 [[SubscriptionKind.BBO]]；回测以
  * [[hft.backtest.SyntheticBboSource]] 把 trades 合成零价差盘口。onEvent 由框架单线程串行调用，
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
    minHedgeQty: Coin = Coin(0.001),
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

  override def orderTimeoutMs: Long = Strategy.RecommendedOrderTimeoutMs

  override def handlers: StrategyHandlers = StrategyHandlers.empty
    .market(Topics.Bbo, Instrument(exchange, symbol)) { (b, ctx, _) =>
      val px = b.midPrice.value
      klines.update(b.timestamp, px)
      if center.isNaN then center = px
      hedge(px, ctx)
    }
    // greeks 的路由键只到交易所，币种在载荷里，故 ccy 仍需自行判断
    .account(Topics.Greeks) { (g, ctx, _) =>
      if g.ccy != ccy then Vector.empty
      else ctx.state.symbolState(symbol).flatMap(_.bbo(exchange)).map(b => hedge(b.midPrice.value, ctx)).getOrElse(Vector.empty)
    }

  private def hedge(px: Double, ctx: StrategyContext): Vector[AnyEvent] =
    (for
      symbolState <- ctx.state.symbolState(symbol)
      greeks <- ctx.state.greeks(exchange, ccy) // greeks 与 cashBal 均到达才动作
      atr <- klines.atr                     // ATR 未预热 -> 不动作 (gating)
      if atr > 0.0 && !center.isNaN
    yield
      // 信号未就绪时以中性默认填充, 各带在预热期自然退化为对称基线
      val maBias = klines.sma.fold(0)(m => math.signum(px - m).toInt)
      val hc = HedgeCtx(px, center, atr, klines.volRatio.getOrElse(1.0), klines.histBias(macdTrendBars), maBias)
      val (upBand, downBand) = band.bands(hc)
      val crossed = (px - center > upBand) || (center - px > downBand)
      if !crossed then Vector.empty
      else
        val netDelta = greeks.delta + symbolState.positionSize(exchange).value // 同为币本位敞口, 解包比较
        val qty = Coin(math.abs(netDelta))
        if qty < minHedgeQty then Vector.empty
        else
          val side = if netDelta > 0 then Side.Short else Side.Long // 净多 -> 卖, 净空 -> 买
          center = px // 成交后中心移到成交价 (market@touch、delay=0)，并防止本笔重复触发
          ctx.place(
            Order("", exchange, symbol, side, OrderType.Market, qty, reduceOnly = false, clientOrderId = ""),
            f"band_hedge | $side netDelta=$netDelta%.4f qty=${qty.value}%.4f px=$px%.2f atr=$atr%.2f up=$upBand%.2f down=$downBand%.2f macdBias=${hc.macdBias} maBias=${hc.maBias} volR=${hc.volRatio}%.2f",
          )
    ).getOrElse(Vector.empty)
