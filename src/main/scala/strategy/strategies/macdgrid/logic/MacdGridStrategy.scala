package strategy.strategies.macdgrid.logic

import hft.strategy.{Strategy, StrategyContext, StrategyHandlers}

import hft.domain.*
import hft.indicator.{Atr, KlineSeries, Macd, Sma}
import hft.event.{AnyEvent, Topics}
import strategy.strategies.macdgrid.logic.MacdGridLogic.{Decision, OrderSpec, Params}

import scala.collection.mutable
import hft.state.{InstrumentView, PendingOrder}

/** MACD 网格策略 (trade-native)。决策逻辑见 [[MacdGridLogic]] (纯函数)。
  *
  * 1h K 线上：DEA 定持仓方向 (水上多/水下空, 反向穿零立刻市价平)、MACD 柱定加仓/减仓、MA20 距离 (ATR) 定超买超卖。
  * 任一时刻至多挂**一张加仓限价单 + 一张止盈限价单 (各一份)**, 价格由**锚价 (最近成交价, 冷启动用现价)** ± ATR 算出;
  * 每次成交后锚价更新、两单按新成交价重挂 (本实现以"按锚价声明式重算 + 仅缺失才补挂、价偏移才撤换"达成, 不逐笔追价)。
  *
  * 一份 = `leverage × 权益 / maxUnits / 价` (满 maxUnits 份 ≈ leverage×权益; 默认 1× 满仓)。开仓时按当时权益冻结,
  * 回到空仓再刷新。加仓/止盈走 maker 限价 (GTC), DEA 转向平仓走 taker 市价。信号全取**已收盘**值 (无前视)。
  */
final class MacdGridStrategy(
    exchange: Exchange,
    symbol: Symbol,
    /** 满仓杠杆: maxUnits 份合计敞口 = leverage × 进场时权益 (默认 1× = 满仓)。 */
    leverage: Double = 1.0,
    params: Params = Params(),
    barIntervalMs: Long = 3_600_000L, // 1h
    maPeriodBars: Int = 20,
    atrPeriodBars: Int = 14,
    /** 最小下单/对账币数 (低于此视为已对齐/空仓)。 */
    minOrderQty: Coin = Coin(0.001),
) extends Strategy:
  private val logger = org.slf4j.LoggerFactory.getLogger(classOf[MacdGridStrategy])
  private val instrument = Instrument(exchange, symbol)

  private val k =
    new KlineSeries(barIntervalMs, maxBars = math.max(maPeriodBars, 26 + 9) + atrPeriodBars + 4)
      with Macd with Sma with Atr:
      override protected def smaPeriod: Int = maPeriodBars
      override protected def atrPeriod: Int = atrPeriodBars

  private var anchorPrice = 0.0 // 挂单锚价 = 最近成交价 (0 = 尚无成交, 用现价)
  private var unitQty = 0.0     // 一份币数 (空仓时按权益刷新, 持仓期冻结)
  private val cancelling = mutable.Set.empty[OrderId] // 已发撤单、等 Cancelled 确认 (防重复撤)

  override def handlers: StrategyHandlers = StrategyHandlers.empty
    .market(Topics.Trade, instrument) { (t, ctx, _) =>
      k.update(t.timestamp, t.price.value, t.qty.value)
      reconcile(t.price, t.timestamp, ctx)
    }
    .own(Topics.Fill) { (f, _, _) =>
      anchorPrice = f.price.value // 成交后锚价更新, 下一拍按新成交价重挂
      Vector.empty
    }

  /** 三周期指标就绪 (macd/sma/atr) -> (dea, bar, ma20, atr)。 */
  private def indicators: Option[(Double, Double, Double, Double)] =
    if !k.macdReady then None
    else
      (k.sma, k.atr) match
        case (Some(ma), Some(a)) if a > 0.0 =>
          Some((k.macdSignalSeries.last.getOrElse(0.0), k.macdHistSeries.last.getOrElse(0.0), ma, a))
        case _ => None

  private def reconcile(price: Price, now: Long, ctx: StrategyContext): Vector[AnyEvent] =
    (ctx.state.instrumentState(instrument), indicators) match
      case (Some(ss), Some((dea, bar, ma20, atr))) =>
        cancelling.filterInPlace(id => ss.pendingOrders.exists(_.order.id == id)) // 清理已消失的撤单追踪
        // 锚价只在"重建挂单时"刷新: 无任何挂单 (冷启动/两单都已离场) -> 锚到现价; 成交时 onEvent 已锚到成交价。
        // 期间锚价固定, 故挂单价稳定 resting、不逐笔追价 (否则单子永远在现价±1ATR 漂移, 永不成交)。
        if ss.pendingOrders.isEmpty then anchorPrice = price.value
        val posCoin = ss.positionSize
        val flat = posCoin.abs < minOrderQty
        if flat then
          // 空仓: 按当时权益刷新"一份"大小 (持仓期冻结)。
          //
          // **拿不到净值就不下单**, 不用任何替代值: 一份的大小 = leverage × 权益 / maxUnits / 价,
          // 从前净值缺失时退回 referenceEquity=100_000, 于是真实权益 5k 的账户首份仓位是
          // **20 倍杠杆**, 而且没有任何报错。`.filter(_ > 0.0)` 更是把 0/负净值也悄悄换成 10 万。
          //
          // 两种失败要分开: 读不到净值是**接线坏了** (契约被破), 净值 <= 0 是**账户爆了** (市场结果)。
          // 从前合成一条 `logger.error + return`: 前者被降级成一条每笔成交都刷一遍的日志、
          // 引擎照跑; 后者则连既有挂单都不再对齐 (跳过了 placeGrid), 单子悬在簿上没人管。
          ctx.state.equity(exchange) match
            case None =>
              // 对齐推 持仓->净值->钱包->挂单 之后才放行行情, 所以这里读不到只可能是柜台接线错了。
              // 立即终止: 引擎会级联停机, 柜台的 onStop 撤掉既有挂单 —— 那才是"安全的不动作"。
              throw IllegalStateException(
                s"[$symbol] $exchange 读不到账户净值 —— 启动对齐本应先推净值再放行策略, 请查柜台接线"
              )
            case Some(equity) if equity <= 0.0 =>
              // 净值归零/为负: 算不出"一份"多大, 因此**不开新仓**; 但下面的 placeGrid 照跑,
              // 把既有挂单该撤的撤掉 (unitQty 保持 0 -> posUnits=0 -> 不期望任何加仓单)。
              logger.error(f"!!! [$symbol] $exchange 净值 $equity%.2f <= 0 -> 停止开新仓, 撤掉既有挂单")
              unitQty = 0.0
            case Some(equity) =>
              unitQty = math.max(minOrderQty.value, leverage * equity / params.maxUnits / price.value)
        // 真实持仓 (≥minOrderQty) 至少记为 ±1 份: 避免不足一份的残仓/dust 被四舍五入成 0,
        // 导致 DEA 反向时不平仓 (flatten 需 posUnits≠0)、也不挂止盈, 残仓长期挂账。
        val posUnits =
          if unitQty <= 0.0 || flat then 0
          else
            val u = (posCoin.value / unitQty).round.toInt
            if u == 0 then posCoin.signum else u
        val level = if anchorPrice > 0.0 then anchorPrice else price.value

        val decision = MacdGridLogic.decide(dea, bar, price.value, level, ma20, atr, posUnits, unitQty, params)
        if decision.flatten then flattenAll(ss, posCoin, dea, price, ctx)
        else placeGrid(ss, decision, posUnits, price, now, ctx)
      case _ => Vector.empty

  /** DEA 转向: 撤掉所有挂单 + 市价 reduceOnly 平掉整个仓位。
    *
    * 时序说明: 撤单异步 (等 Cancelled 确认), 市价平即时。撤单确认前, 若旧 resting 加仓限价单恰被成交价穿越成交,
    * 会多开一份逆 DEA 方向的单; 但下一拍 posUnits 仍与 DEA 冲突 -> 再次 flatten, 最终收敛 (至多多一次 taker 往返)。
    * 加仓单非 reduceOnly、平仓单 reduceOnly, 不会出现"平仓反向开仓"。 */
  private def flattenAll(ss: InstrumentView, posCoin: Coin, dea: Double, price: Price, ctx: StrategyContext): Vector[AnyEvent] =
    val cancels = ss.pendingOrders.flatMap(cancelConfirmed(_, ctx)).toVector
    val close =
      if posCoin.abs >= minOrderQty then
        val side = if posCoin > Coin.Zero then Side.Short else Side.Long
        logger.info(f"[MacdGrid $symbol] DEA转向平仓 dea=$dea%.4f pos=$posCoin%.4f px=$price%.2f")
        ctx.place(
          Order("", exchange, symbol, side, OrderType.Market, posCoin.abs, reduceOnly = true, ""),
          f"macdgrid:flatten $side qty=${posCoin.abs.value}%.4f dea=$dea%.4f px=$price%.2f",
        )
      else Vector.empty
    cancels ++ close

  /** 把加仓单 / 止盈单两个槽对齐到期望 (仅缺失才补挂, 价偏移/不再期望才撤)。
    * 静态单 (加仓单、网格态止盈单) 仅锚价移动才撤换; **追价止盈单 (被动平仓) 按 ChaseIntervalMs 节奏撤单追挂**。 */
  private def placeGrid(ss: InstrumentView, d: Decision, posUnits: Int, price: Price, now: Long, ctx: StrategyContext): Vector[AnyEvent] =
    // 现有挂单按槽分类: reduceOnly=止盈槽, 否则=加仓槽 (每槽留首张)
    var addPending: Option[PendingOrder] = None
    var closePending: Option[PendingOrder] = None
    ss.pendingOrders.foreach { p =>
      if p.order.reduceOnly then { if closePending.isEmpty then closePending = Some(p) }
      else if addPending.isEmpty then addPending = Some(p)
    }
    val actions = mutable.ArrayBuffer.empty[AnyEvent]
    reconcileSlot(d.add, addPending, "add", posUnits, price, now, actions, ctx)
    reconcileSlot(d.close, closePending, "close", posUnits, price, now, actions, ctx)
    actions.toVector

  /** 单槽对齐:
    *   - 期望且缺失 -> 挂 (追价单顺带记录追价时刻)。
    *   - 期望且已挂: **追价单** (spec.trail) 每 ChaseIntervalMs 且价偏移才撤 (撤后下拍按新现价重挂, 追着市场);
    *     **静态单** 仅价偏移 (锚价移动) 即撤。
    *   - 不再期望 -> 撤。
    */
  private def reconcileSlot(
      desired: Option[OrderSpec], present: Option[PendingOrder], tag: String,
      posUnits: Int, price: Price, now: Long, actions: mutable.ArrayBuffer[AnyEvent], ctx: StrategyContext,
  ): Unit =
    (desired, present) match
      case (Some(spec), None) if spec.qty >= minOrderQty.value =>
        actions ++= ctx.place(
          Order("", exchange, symbol, spec.side, OrderType.Limit(Price(spec.price), TimeInForce.GTC), Coin(spec.qty), spec.reduceOnly, ""),
          f"macdgrid:$tag${if spec.trail then ":trail" else ""} ${spec.side} px=${spec.price}%.2f qty=${spec.qty}%.4f units=$posUnits mark=$price%.2f",
        )
      case (Some(spec), Some(p)) if spec.trail =>
        // 被动平仓追价: 节流绑定到**该单存活时长** (now − createdAt), 重挂的新单自带新 createdAt -> 重新计时,
        // 故 trail↔静态 来回切换不会因游离全局态而"刚挂就撤"。仅存活≥ChaseIntervalMs 且现价已使挂单价偏移才撤。
        if now - p.createdAt >= MacdGridStrategy.ChaseIntervalMs && priceDrifted(p, spec.price) then
          cancelConfirmed(p, ctx).foreach(actions += _) // 撤后下拍按新现价重挂, 追着市场跑
      case (Some(spec), Some(p)) if priceDrifted(p, spec.price) =>
        cancelConfirmed(p, ctx).foreach(actions += _) // 静态单: 锚价已移 -> 撤旧, 下拍按新价重挂
      case (None, Some(p)) =>
        cancelConfirmed(p, ctx).foreach(actions += _) // 不再期望该槽 -> 撤
      case _ => () // 期望且已挂且 (静态未偏移 / 追价未到点) -> 保持 resting

  /** resting 限价单价相对期望价是否已偏移 (锚价移动后触发撤换)。 */
  private def priceDrifted(p: PendingOrder, desired: Double): Boolean =
    p.order.orderType match
      case OrderType.Limit(restPx, _) => math.abs(restPx.value - desired) > desired * MacdGridStrategy.PriceDriftRel
      case _                          => true

  private def cancelConfirmed(p: PendingOrder, ctx: StrategyContext): Option[AnyEvent] =
    if p.status.isConfirmed && !cancelling.contains(p.order.id) then
      cancelling += p.order.id
      Some(ctx.cancel(exchange, symbol, OrderRef.ByExchangeId(p.order.id)))
    else None

object MacdGridStrategy:
  /** 限价单价相对偏移阈值: |resting价 − 期望价| / 期望价 超过即撤换重挂 (锚价移动后)。 */
  private val PriceDriftRel: Double = 1e-4
  /** 被动平仓追价节奏: 单侧挂单 (止盈单) 每隔该毫秒数撤单追着现价重挂一次。 */
  private val ChaseIntervalMs: Long = 10_000
