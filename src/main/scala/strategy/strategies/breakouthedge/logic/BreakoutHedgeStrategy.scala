package strategy.strategies.breakouthedge.logic
import strategy.utils.hedge.{HedgeExecution, HedgeRequest}

import hft.strategy.{OutcomeEvent, Strategy}

import hft.domain.*
import hft.exchange.SubscriptionKind
import hft.indicator.{Donchian, KlineSeries}
import hft.messaging.{EventData, IncomeEvent, StateManager}

/** **突破式中性 delta 对冲** (空 gamma 友好)：用最近 N (默认 5h) 的 Donchian 通道高/低点做突破判据，
  * 配 delta 阈值 (以期权头寸 ETH 名义为基数) 决定何时把净 delta 中和到 0。
  *
  *   - **发生突破** (价 > [[Donchian.windowHigh]] 或 价 < windowLow, 方向无关) -> 用阈值 [[minThresholdPct]]
  *     把净 delta 中和到 0 (及时对冲突破带来的趋势敞口)。方向无关使其对**空 gamma (卖方) 与多 gamma
  *     (买方)** 都成立: 卖方 delta 恒顺突破方向, 买方恒逆突破方向, 二者在"突破即归零"下统一;
  *   - **未突破** (仍在通道内) -> 由 [[maxThresholdPct]] 决定:
  *       - `Some(p)` (双阈值模式): |净 delta| 达 p·头寸即对冲 (容忍区间漂移但设绝对上限, 防静默失控)；
  *       - `None` (**纯突破闸门模式**): 区间内**完全不对冲**, 只有突破 5h 高/低点 且达阈值才动作 ——
  *         即只对真突破出手、不为区间内来回的均值回复 delta 漂移付对冲成本。
  *
  * 阈值 = pct · [[optionPositionEth]] (期权头寸名义)。target 恒 0 (纯中性, 无方向押注)。
  *
  * 与 [[TargetDeltaHedgeStrategy]] 并列、各自独立实现 (开放封闭——不在一个类里用 flag 分发对冲规则)。
  * 净 delta = 期权 delta (greeks, 含现货修正) + 永续持仓。onEvent 由框架单线程串行调用。
  */
final class BreakoutHedgeStrategy(
    /** 永续对冲场所 + greeks 标记的交易所 */
    exchange: Exchange,
    /** 永续 symbol, e.g. "ETHUSDT" */
    symbol: Symbol,
    /** greeks 币种, e.g. "ETH" */
    ccy: String,
    /** 对冲执行器 (市价 / 限价追价) */
    execution: HedgeExecution,
    /** 期权头寸名义 (ETH)，delta 阈值的基数 (= |straddles|, 每份跨式约 1 ETH 标的单边最大 delta) */
    optionPositionEth: Double,
    /** 通道/突破窗口 bar 数 (默认 300 = 5h @ 1min) */
    windowBars: Int = 300,
    /** 通道 bar 周期 (ms, 默认 1min) */
    barIntervalMs: Long = 60_000,
    /** 区间内 (未突破) 容忍阈值占比: Some(p)=阈值 p·optionPositionEth (双阈值, 设上限); None=区间内不对冲 (纯突破闸门) */
    maxThresholdPct: Option[Double] = None,
    /** 突破时阈值占比: 阈值 = minThresholdPct · optionPositionEth (顺突破方向才对冲) */
    minThresholdPct: Double = 0.01,
) extends Strategy:

  private val channel = new KlineSeries(barIntervalMs, windowBars) with Donchian:
    override protected def donchianBars: Int = windowBars

  private val maxThreshold: Option[Double] = maxThresholdPct.map(_ * math.abs(optionPositionEth))
  private val minThreshold = minThresholdPct * math.abs(optionPositionEth)

  override def publicStreams: Map[Exchange, Set[SubscriptionKind]] =
    Map(exchange -> Set(SubscriptionKind.Trade(symbol)))

  override def orderTimeoutMs: Long = 5000

  override def onEvent(event: IncomeEvent, state: StateManager): Vector[OutcomeEvent] =
    event.data match
      // 逐笔成交：更新通道 + 以最新成交价评估对冲
      case EventData.MarketTradeUpdate(t) if t.exchange == exchange && t.symbol == symbol =>
        channel.update(t.timestamp, t.price, t.qty)
        hedge(t.price, t.timestamp, state)
      // greeks 变化 (delta 漂移) 也即时触发，用 state 中最新成交价为基准
      case EventData.GreeksUpdate(g) if g.exchange == exchange && g.ccy == ccy =>
        state
          .symbolState(symbol)
          .flatMap(_.lastTradePrice(exchange))
          .map(hedge(_, g.timestamp, state))
          .getOrElse(Vector.empty)
      case _ => Vector.empty

  private def hedge(refPrice: Price, now: Timestamp, state: StateManager): Vector[OutcomeEvent] =
    (for
      symbolState <- state.symbolState(symbol)
      greeks <- state.greeks(exchange, ccy) // greeks 与 cashBal 均到达才动作
    yield
      val netDelta = greeks.delta + symbolState.positionSize(exchange)
      val needBuy = netDelta < 0 // 偏空 -> 买回中性
      // 发生突破 (方向无关) -> 中和 delta。卖方 delta 顺突破、买方逆突破, 都在此统一为"突破即归零"
      val brokeUp = channel.windowHigh.exists(refPrice > _)
      val brokeDown = channel.windowLow.exists(refPrice < _)
      val eager = brokeUp || brokeDown
      // 突破 -> minThreshold; 否则用区间内容忍阈值 (None=纯闸门, 区间内不对冲)
      val threshold: Option[Double] = if eager then Some(minThreshold) else maxThreshold
      val side = if needBuy then Side.Long else Side.Short
      val req = threshold
        .filter(th => math.abs(netDelta) >= th)
        .map(_ => HedgeRequest(side, math.abs(netDelta), refPrice)) // 中和到 0
      execution(req, symbolState.pendingOrders.headOption, now)
    ).getOrElse(Vector.empty)
