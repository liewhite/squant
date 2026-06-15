package hft.strategy

import hft.domain.*
import hft.exchange.SubscriptionKind
import hft.messaging.{EventData, IncomeEvent, PendingOrder, StateManager, SymbolState}

import scala.collection.mutable

/** 追暴跌做空策略 (动量追空)。
  *
  * 思路：**平时只监控不交易**，识别到"突然暴跌"信号后，用 PostOnly maker 卖单**不断加仓追暴跌做空**，
  * 顺着跌势金字塔式加空，价格继续下挫时分批止盈 (买回平仓)，反弹超过阈值则市价止损。
  *
  * 为什么是做空：题面要求"抓突然暴跌的行情""追暴跌"——即从暴跌中获利，方向是顺势做空。
  * maker 加空在机制上成立：把卖单挂在现价上方一点 (PostOnly 不吃单)，下跌途中频繁的小反弹会
  * 逐档击穿卖单成交 (卖在相对高点，对空头是更优入场)；随价格下移把卖单逐档下调，持续加空。
  *
  * 三个核心因子 (全部来自 BBO 中间价，无需额外数据)：
  *   1. 暴跌幅度 (drawdown)：当前中间价相对 [now-windowMs, now] 窗口内峰值的回撤。
  *   2. 暴跌时效 (window)：峰值只在 windowMs 窗口内统计 —— 保证是"突然"而非缓慢阴跌。
  *   3. 持仓均价 (avgEntry)：逐笔成交加权，驱动止盈/止损线。
  *
  * 状态机 (由持仓与信号隐式表达，无显式枚举)：
  *   - 监控：drawdown 未达阈值且空仓 -> 不挂任何单。
  *   - 加空：drawdown <= -crashThreshold 且冷却结束且杠杆未触顶 -> 在 ask 上方挂一张 PostOnly 卖单，
  *           成交即累积空头，随价格下移逐档下调追价 (单边同一时刻最多一张在途)。
  *   - 离场：持仓时维护**单张**平仓买单 (reduceOnly) 挂在 avgEntry*(1-takeProfit) 止盈 (继续下跌即平);
  *           若中间价反弹超过 avgEntry*(1+stopLoss)，把该平仓单切换为 Market 单 (taker) 立即止损。
  *   - 冷却：平仓后冷却 cooldownMs，避免同一波反弹里刚止损又立刻追空。
  *
  * 风控：每次加空前校验"加空后名义价值 / 净值 < maxLeverage"，触顶即停止加仓。
  * 平仓单恒为单张 Long + reduceOnly + 数量=当前空头，从不反手。
  *
  * 依赖账户净值 (AccountInfoUpdate，由回测/实盘引擎周期刷新提供)，净值未知或非正时不交易 (安全侧)。
  *
  * onEvent 由框架保证单线程串行调用，内部可变状态无需同步。
  *
  * 不变量与已知限制：
  *   - **假设 contractSize == 1 (币本位数量 == 合约张数)**。shortCoin 累积的是 Fill 回报里的张数，
  *     离场单又把它当币本位交回 Runner 转换——仅当 contractSize==1 时平仓数量精确等于持仓 (Binance
  *     UM 恒为 1，与 BboMaker 同一约定)。换非 1 合约前需统一数量口径。
  *   - **不支持带仓重启**：shortCoin/avgEntry 仅从本进程启动后看到的成交累积，不从框架已有持仓引导。
  *     回测从空仓起步无影响；直接上实盘前需改为以 SymbolState.positionSize 为持仓事实来源。
  */
final class CrashChaseStrategy(
    targetExchange: Exchange,
    symbol: Symbol,
    /** 暴跌识别窗口 (毫秒)：只在此窗口内统计峰值，保证捕捉"突然"暴跌 */
    windowMs: Long = 60_000,
    /** 暴跌阈值：中间价相对窗口峰值回撤达到该比例即触发，0.02 = 2% */
    crashThreshold: Double = 0.02,
    /** 卖单挂在 ask 上方的偏移比例，0.0005 = 0.05% (保证 PostOnly 不吃单) */
    entryOffsetRatio: Double = 0.0005,
    /** 每档加仓的名义价值 (USDT)，与价格无关 -> 适配任意币种 */
    rungUsdt: Double = 500.0,
    /** 杠杆率上限: 加仓后 (名义价值 / 净值) 必须低于该值 */
    maxLeverage: Double = 3.0,
    /** 止盈比例: 中间价相对持仓均价继续下跌该比例时平仓，0.01 = 1% */
    takeProfitRatio: Double = 0.01,
    /** 止损比例: 中间价相对持仓均价反弹该比例时市价止损，0.03 = 3% */
    stopLossRatio: Double = 0.03,
    /** 平仓后冷却时长 (毫秒)，期间不开新仓 */
    cooldownMs: Long = 60_000,
    /** 挂单价偏离目标价超过该比例则撤单重挂 */
    repriceToleranceRatio: Double = 0.0003,
    /** 价格采样间隔 (毫秒)：窗口按此降采样，限定峰值扫描成本 */
    sampleIntervalMs: Long = 500,
) extends Strategy:

  /** 降采样后的价格窗口 (时间戳, 中间价)，仅保留最近 windowMs */
  private val priceWindow = mutable.Queue.empty[(Timestamp, Double)]
  private var lastSampleTs: Timestamp = 0L

  /** 已发出撤单、尚未确认移除的订单，防止重复撤单 */
  private val cancelling = mutable.Set.empty[OrderId]

  /** 自维护的空头持仓 (币本位, 取正数表示空头大小) 与加权均价 */
  private var shortCoin: Double = 0.0
  private var avgEntry: Double = 0.0
  /** 平仓后的冷却截止时刻 */
  private var cooldownUntil: Timestamp = 0L

  private val Eps = 1e-9

  override def publicStreams: Map[Exchange, Set[SubscriptionKind]] =
    Map(targetExchange -> Set(SubscriptionKind.BBO(symbol)))

  override def orderTimeoutMs: Long = 5000

  override def onEvent(event: IncomeEvent, state: StateManager): Vector[OutcomeEvent] =
    event.data match
      case EventData.BboUpdate(bbo) if bbo.exchange == targetExchange && bbo.symbol == symbol =>
        onBbo(bbo, state)
      case EventData.FillUpdate(fill) if fill.exchange == targetExchange && fill.symbol == symbol =>
        onFill(fill)
        Vector.empty // 下单决策统一在行情事件做，节奏由订单生命周期限速
      case _ => Vector.empty

  /** 逐笔成交即时维护空头均价与冷却；买回平仓后重置并进入冷却 */
  private def onFill(fill: Fill): Unit =
    fill.side match
      case Side.Short => // 加空
        val newPos = shortCoin + fill.size
        avgEntry = if shortCoin <= Eps then fill.price else (avgEntry * shortCoin + fill.price * fill.size) / newPos
        shortCoin = newPos
      case Side.Long => // 买回平仓
        shortCoin = math.max(0.0, shortCoin - fill.size)
        if shortCoin <= Eps then
          avgEntry = 0.0
          cooldownUntil = fill.timestamp + cooldownMs

  private def onBbo(bbo: BBO, state: StateManager): Vector[OutcomeEvent] =
    val mid = bbo.midPrice
    val drawdown = updateWindowAndDrawdown(bbo.timestamp, mid)
    (for
      symbolState <- state.symbolState(symbol)
      account <- state.accountInfo(targetExchange)
      if account.equity > 0
    yield
      cancelling.filterInPlace(id => symbolState.pendingOrders.exists(_.order.id == id))
      val crashActive = drawdown <= -crashThreshold
      entrySide(bbo, crashActive, account, symbolState).toVector ++
        exitSide(mid, symbolState).toVector
    ).getOrElse(Vector.empty)

  /** 更新降采样窗口，返回当前中间价相对窗口峰值的回撤 (<=0) */
  private def updateWindowAndDrawdown(ts: Timestamp, mid: Double): Double =
    if priceWindow.isEmpty || ts - lastSampleTs >= sampleIntervalMs then
      priceWindow.enqueue((ts, mid))
      lastSampleTs = ts
    while priceWindow.nonEmpty && priceWindow.front._1 < ts - windowMs do priceWindow.dequeue()
    val peak = priceWindow.iterator.map(_._2).maxOption.getOrElse(mid).max(mid)
    mid / peak - 1.0

  // ==================== 加空 (卖) ====================

  /** 暴跌且未触顶杠杆时，在 ask 上方维护一张 PostOnly 卖单 (逐档追价加空) */
  private def entrySide(
      bbo: BBO,
      crashActive: Boolean,
      account: AccountInfo,
      symbolState: SymbolState,
  ): Option[OutcomeEvent] =
    val targetAsk = bbo.askPrice * (1 + entryOffsetRatio)
    val canEnter =
      crashActive && bbo.timestamp >= cooldownUntil && (account.notional + rungUsdt) < maxLeverage * account.equity
    symbolState.pendingOrders.find(_.order.side == Side.Short) match
      case Some(pending) =>
        if !canEnter then cancelConfirmed(pending)
        else maybeRepriceLimit(pending, targetAsk)
      case None =>
        if canEnter then
          val rungCoin = rungUsdt / bbo.askPrice
          Some(place(Side.Short, OrderType.Limit(targetAsk, TimeInForce.PostOnly), rungCoin, reduceOnly = false,
            f"crash_short | ask=${bbo.askPrice}%.6f px=$targetAsk%.6f qty=$rungCoin%.4f"))
        else None

  // ==================== 离场 (买回, 单张) ====================

  /** 持仓时维护单张平仓买单：常态 PostOnly 止盈，反弹破止损线则切 Market 立即止损 */
  private def exitSide(mid: Double, symbolState: SymbolState): Option[OutcomeEvent] =
    if shortCoin <= Eps then None
    else
      val stop = mid >= avgEntry * (1 + stopLossRatio)
      val tpPrice = avgEntry * (1 - takeProfitRatio)
      symbolState.pendingOrders.find(_.order.side == Side.Long) match
        case Some(pending) =>
          pending.order.orderType match
            case OrderType.Limit(restingPrice, _) =>
              val qtyDrift = math.abs(pending.order.quantity - shortCoin) / shortCoin > repriceToleranceRatio
              val priceDrift = math.abs(restingPrice - tpPrice) / tpPrice > repriceToleranceRatio
              if stop || priceDrift || qtyDrift then cancelConfirmed(pending) else None
            case OrderType.Market => None // 市价平仓单在途，等成交
        case None =>
          val ot = if stop then OrderType.Market else OrderType.Limit(tpPrice, TimeInForce.PostOnly)
          val tag = if stop then "crash_stop" else "crash_tp"
          Some(place(Side.Long, ot, shortCoin, reduceOnly = true, f"$tag | avg=$avgEntry%.6f mid=$mid%.6f qty=$shortCoin"))

  // ==================== 工具 ====================

  /** 已确认且未在撤单中、且偏离目标价超容差的限价单 -> 撤单一次 */
  private def maybeRepriceLimit(pending: PendingOrder, targetPrice: Price): Option[OutcomeEvent] =
    pending.order.orderType match
      case OrderType.Limit(restingPrice, _)
          if pending.status.isConfirmed
            && !cancelling.contains(pending.order.id)
            && math.abs(restingPrice - targetPrice) / targetPrice > repriceToleranceRatio =>
        cancelConfirmed(pending)
      case _ => None

  /** 撤掉已确认挂单 (Created 无 orderId 不能撤，等确认) */
  private def cancelConfirmed(pending: PendingOrder): Option[OutcomeEvent] =
    if pending.status.isConfirmed && !cancelling.contains(pending.order.id) then
      cancelling += pending.order.id
      Some(OutcomeEvent.CancelOrder(targetExchange, symbol, pending.order.id))
    else None

  private def place(side: Side, orderType: OrderType, coinQty: Quantity, reduceOnly: Boolean, comment: String): OutcomeEvent =
    OutcomeEvent.PlaceOrders(
      Vector(
        Order(
          id = "",
          exchange = targetExchange,
          symbol = symbol,
          side = side,
          orderType = orderType,
          quantity = coinQty,
          reduceOnly = reduceOnly,
          clientOrderId = "", // 由 Runner 生成
        )
      ),
      comment,
    )
