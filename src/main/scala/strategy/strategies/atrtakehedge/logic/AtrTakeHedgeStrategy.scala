package strategy.strategies.atrtakehedge.logic

import hft.strategy.{OutcomeEvent, Strategy}

import hft.domain.*
import hft.exchange.SubscriptionKind
import hft.indicator.{Atr, KlineSeries}
import hft.messaging.{EventData, IncomeEvent, StateManager}

/** **价格主导、delta 定量** 的期权买方 (long-gamma) 对冲策略 —— ATR 通道 take 式 rehedge。
  *
  * 与 [[GammaScalpStrategy]] (固定 delta 阈值 + 固定 offset maker) 的根本区别：
  * 用**价格位移**而非 delta 阈值决定**何时**对冲，用**当前净 delta** 决定**对冲多少**。
  *
  * 动机：按 delta 阈值触发，在深实/深虚 (gamma 变小) 时，同样的 delta 漂移需要价格走很远才累积到，
  * 挂单位置被推得很远、难成交。改为价格主导后，无论 gamma 大小，价格每走固定的 atrMult×ATR
  * 就对冲一次；对冲量 = 当前净 delta —— gamma 大 (近 ATM) 时 delta 漂移快、单次对冲量自然大，
  * gamma 小时对冲量自然小，**量随 delta 自适应**。
  *
  * 机制：
  *   - 以**最新价为中心** center；维护逐笔聚合的小时 K 线 [[Atr]]。
  *   - 当 |最新价 − center| > [[atrMult]]×ATR 时，**立即 take (市价/taker)** 当前净 delta 的数量
  *     使账户回到 delta 中性；成交后**把 center 移到成交价**，继续监控。
  *   - 净 delta = 期权 delta (greeks，含现货 cashBal 修正) + 永续持仓。净多 → 卖、净空 → 买。
  *
  * **需要盘口**：market 单到达撮合需 BBO (见 [[hft.sim.SimState.onOrderArrived]])，故订阅 [[SubscriptionKind.BBO]];
  * 回测以 [[hft.backtest.TradePrintBboSource]] 把 trades 合成零价差盘口，market 单即以现价成交。
  * onEvent 由框架单线程串行调用，内部可变状态无需同步。
  */
final class AtrTakeHedgeStrategy(
    /** 永续对冲场所 + greeks 标记的交易所 (回测中二者一致) */
    exchange: Exchange,
    /** 永续 symbol, e.g. "ETHUSDT" */
    symbol: Symbol,
    /** greeks 币种, e.g. "ETH" */
    ccy: String,
    /** 对冲触发的价格通道宽度 (×ATR)：|最新价 − center| 超过 atrMult×ATR 即对冲 */
    atrMult: Double = 2.0,
    /** ATR 周期 (根) */
    atrPeriodBars: Int = 14,
    /** K 线周期 (由逐笔合成)，默认 1 小时 */
    barIntervalMs: Long = 3_600_000L,
    /** 最小对冲数量 (币本位)，低于此不动作避免碎单 */
    minHedgeQty: Quantity = 0.001,
) extends Strategy:

  private val klines = new KlineSeries(barIntervalMs, math.max(atrPeriodBars * 4, 64)) with Atr:
    override protected def atrPeriod: Int = atrPeriodBars

  /** 对冲中心价 (NaN = 尚未初始化，首个行情设为现价) */
  private var center: Double = Double.NaN

  override def publicStreams: Map[Exchange, Set[SubscriptionKind]] =
    Map(exchange -> Set(SubscriptionKind.BBO(symbol)))
  // greeks 为账户级广播事件，StateManager 自动维护，按 ccy 查询，无需订阅

  override def orderTimeoutMs: Long = 5000

  override def onEvent(event: IncomeEvent, state: StateManager): Vector[OutcomeEvent] =
    event.data match
      // 盘口更新：喂 ATR + 以中间价 (= 现价) 评估是否越通道
      case EventData.BboUpdate(b) if b.exchange == exchange && b.symbol == symbol =>
        val px = b.midPrice
        klines.update(b.timestamp, px)
        if center.isNaN then center = px
        hedge(px, state)
      // greeks 漂移也触发，用 state 中最新盘口中间价为基准
      case EventData.GreeksUpdate(g) if g.exchange == exchange && g.ccy == ccy =>
        state.symbolState(symbol).flatMap(_.bbo(exchange)).map(b => hedge(b.midPrice, state)).getOrElse(Vector.empty)
      case _ => Vector.empty

  private def hedge(px: Price, state: StateManager): Vector[OutcomeEvent] =
    (for
      symbolState <- state.symbolState(symbol)
      greeks <- state.greeks(exchange, ccy) // greeks 与 cashBal 均到达才动作
      atr <- klines.atr                     // ATR 未预热 -> 不动作
      if atr > 0.0 && !center.isNaN && math.abs(px - center) > atrMult * atr
    yield
      val netDelta = greeks.delta + symbolState.positionSize(exchange)
      val qty = math.abs(netDelta)
      if qty < minHedgeQty then Vector.empty
      else
        val side = if netDelta > 0 then Side.Short else Side.Long // 净多 -> 卖, 净空 -> 买
        center = px // 成交后中心移到成交价 (market@touch、delay=0 -> 成交价≈px)，并防止本笔重复触发
        Vector(
          OutcomeEvent.PlaceOrders(
            Vector(Order("", exchange, symbol, side, OrderType.Market, qty, reduceOnly = false, clientOrderId = "")),
            f"atr_take | $side netDelta=$netDelta%.4f qty=$qty%.4f px=$px%.2f atr=$atr%.2f band=${atrMult * atr}%.2f",
          )
        )
    ).getOrElse(Vector.empty)
