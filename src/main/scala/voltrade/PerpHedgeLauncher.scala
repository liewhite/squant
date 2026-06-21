package voltrade

import hft.domain.*
import hft.exchange.bybit.{BybitClient, BybitCredentials}
import hft.indicator.{Atr, KlineSeries, Sma}
import org.slf4j.LoggerFactory
import sttp.client4.DefaultSyncBackend

/** 永续 delta 对冲实盘启动器 (Bybit linear)。轮询: 期权账户净 delta + 永续持仓 -> MaAsym 带 ->
  * 在永续上挂被动单 (现价外 0.02%, 5s 重挂) 把账户对冲到 delta 中性。与卖方期权腿 [[VolSellLauncher]] 配套。
  *
  * **安全默认 dry-run** (只算+打日志不下单)。`VOLHEDGE_LIVE=1`+BYBIT_API_KEY/SECRET 才真实在永续下单。
  * 读期权持仓/永续持仓需 API key (即便 dry-run)。`VOLHEDGE_TESTNET=1` 走 testnet。
  *
  * 注: 实盘前必须 testnet 验证 (永续精度/最小量/撤单回报/限频)。框架风格 fail-fast, 但对冲循环对**单轮**错误
  * log+跳过下一轮 (避免一次抖动就裸敞口), 持续失败由外层监控/重启兜底。
  *
  * 运行: VOLHEDGE_LIVE=1 BYBIT_API_KEY=.. BYBIT_API_SECRET=.. sbt "runMain voltrade.PerpHedgeLauncher"
  */
@main def PerpHedgeLauncher(): Unit =
  val logger = LoggerFactory.getLogger("PerpHedgeLauncher")
  val symbol = sys.env.getOrElse("VOLHEDGE_SYMBOL", "ETHUSDT")
  val pollMs = sys.env.get("VOLHEDGE_POLL_MS").map(_.toLong).getOrElse(3000L)
  val atrP = sys.env.get("VOLHEDGE_ATR_PERIOD").map(_.toInt).getOrElse(14)
  val smaP = sys.env.get("VOLHEDGE_SMA_PERIOD").map(_.toInt).getOrElse(20)
  val live = sys.env.get("VOLHEDGE_LIVE").contains("1")
  val testnet = sys.env.get("VOLHEDGE_TESTNET").contains("1")
  val credentials = for k <- sys.env.get("BYBIT_API_KEY"); s <- sys.env.get("BYBIT_API_SECRET") yield BybitCredentials(k, s)
  val backend = DefaultSyncBackend()
  val opt: OptionsExchange = BybitOptionsClient(backend, credentials, dryRun = !live, testnet = testnet)
  val perp = BybitClient(backend, credentials) // 永续下单/查仓 (linear)

  // 对冲需读期权/永续持仓 (签名), 无 key 直接退出 (不空转刷屏)
  if credentials.isEmpty then
    logger.error("缺 BYBIT_API_KEY/SECRET, 对冲腿需读持仓, 无法运行。退出。")
    sys.exit(1)

  // 永续精度过滤器 (qtyStep/minQty/tickSize) —— 实盘下单必须对齐, 否则被拒
  val meta = perp.fetchAllSymbolMetas().toOption.flatMap(_.find(_.symbol == symbol))
    .getOrElse { logger.error(s"取不到 $symbol 永续精度, 退出"); sys.exit(1) }
  val p = PerpHedger.Params(
    offsetPct = sys.env.get("VOLHEDGE_OFFSET").map(_.toDouble).getOrElse(0.0002),
    requoteMs = sys.env.get("VOLHEDGE_REQUOTE_MS").map(_.toLong).getOrElse(5000L),
    minHedge = sys.env.get("VOLHEDGE_MIN").map(_.toDouble).getOrElse(math.max(0.01, meta.minOrderSize)),
    tightAtr = sys.env.get("VOLHEDGE_TIGHT_ATR").map(_.toDouble).getOrElse(1.0),
    looseAtr = sys.env.get("VOLHEDGE_LOOSE_ATR").map(_.toDouble).getOrElse(2.0),
    qtyStep = meta.sizeStep, minQty = meta.minOrderSize, tickSize = meta.tickSize,
  )

  logger.warn(s"PerpHedge 启动: symbol=$symbol poll=${pollMs}ms 带=均线上 上${p.tightAtr}ATR/下${p.looseAtr}ATR minHedge=${p.minHedge}")
  logger.warn(s"永续精度: qtyStep=${p.qtyStep} minQty=${p.minQty} tickSize=${p.tickSize}")
  logger.warn(s"模式: ${if live then "*** 实盘 LIVE ***" else "dry-run (不下单)"}  ${if testnet then "testnet" else "mainnet"}")

  /** 把预成形 OHLC 喂进 KlineSeries(用 h/l/c 当三笔 tick)算 ATR/SMA, 复用已测指标 */
  def indicators(bars: Vector[(Double, Double, Double)]): (Option[Double], Option[Double]) =
    val k = new KlineSeries(3_600_000L, 256) with Atr with Sma:
      override protected def atrPeriod: Int = atrP
      override protected def smaPeriod: Int = smaP
    bars.zipWithIndex.foreach { case ((h, l, c), i) =>
      val t = i.toLong * 3_600_000L; k.update(t, h); k.update(t, l); k.update(t, c)
    }
    (k.atr, k.sma)

  var st = PerpHedger.State()

  def perpPosition(): Either[String, Double] =
    perp.fetchPositions().left.map(_.message).map(_.find(_.symbol == symbol).map(_.size).getOrElse(0.0))

  /** 返回 true=本轮正常完成, false=因取数失败跳过 (供连续失败告警) */
  def cycle(): Boolean =
    val now = System.currentTimeMillis
    val r =
      for
        bars <- opt.linearKlines(symbol, "60", math.max(atrP, smaP) + 8)
        mid <- opt.underlyingSpot(symbol)
        optDelta <- opt.optionAccountDelta()
        perpPos <- perpPosition()
        pending <- (if live then perp.fetchPendingOrders(symbol).left.map(_.message) else Right(Vector.empty))
      yield (bars, mid, optDelta, perpPos, pending)
    r match
      case Left(err) => logger.error(s"本轮跳过 (取数失败): $err"); false
      case Right((bars, mid, optDelta, perpPos, pending)) =>
        val (atrO, smaO) = indicators(bars)
        // 对账仅在 live: 我方挂单已不在 pending -> 已了结 -> 清 restingId (center 由 decide 的中性判定驱动, 不在此重置)
        if live then st.restingId.foreach(id => if !pending.exists(_.orderId == id) then st = st.copy(restingId = None))
        atrO match
          case None => logger.warn(s"ATR 预热不足 (bars=${bars.size}), 本轮不动作"); true
          case Some(atr) =>
            val maBias = smaO.fold(0)(m => math.signum(mid - m).toInt)
            val netDelta = optDelta + perpPos
            val (action, st1) = PerpHedger.decide(mid, atr, maBias, netDelta, now, st, p)
            st = st1
            logger.info(f"mid=$mid%.2f atr=$atr%.2f maBias=$maBias 期权Δ=$optDelta%.4f 永续=$perpPos%.4f 净Δ=$netDelta%.4f -> $action")
            action match
              case PerpHedger.Action.Hold => ()
              case PerpHedger.Action.Cancel(id) =>
                if live then
                  perp.cancelOrder(symbol, id) match
                    case Right(_) => st = st.copy(restingId = None) // 撤成功才清, 失败保留下轮重试 (防双倍敞口)
                    case Left(e)  => logger.error(s"撤单失败 (保留挂单下轮重试): ${e.message}")
                else { st = st.copy(restingId = None); logger.warn(s"[DRY] 撤单 $id (重挂)") }
              case PerpHedger.Action.Place(side, price, qty) =>
                val link = s"ph-$now"
                val order = Order("", Exchange.Bybit, symbol, side, OrderType.Limit(price, TimeInForce.PostOnly), qty, reduceOnly = false, link)
                if live then
                  perp.placeOrder(order) match
                    case Right(oid) => st = st.copy(restingId = Some(oid), restingAt = now); logger.warn(f"对冲挂单 $side qty=$qty%.4f @$price%.2f -> $oid")
                    case Left(e)    => logger.error(s"对冲下单失败 (下轮重试): ${e.message}")
                else
                  st = st.copy(restingId = Some(link), restingAt = now) // dry-run 用 link 占位模拟"已挂"
                  logger.warn(f"[DRY] 对冲挂单 $side qty=$qty%.4f @$price%.2f PostOnly link=$link")
            true

  // 连续取数失败 = 对冲腿可能长时间裸敞口, 计数到阈值升级告警
  var consecutiveFails = 0
  val failAlertThreshold = sys.env.get("VOLHEDGE_FAIL_ALERT").map(_.toInt).getOrElse(5)
  while true do
    val ok = try cycle() catch { case e: Throwable => logger.error(s"cycle 异常: ${e.getMessage}"); false }
    if ok then consecutiveFails = 0
    else
      consecutiveFails += 1
      if consecutiveFails >= failAlertThreshold then
        logger.error(s"!!! 对冲腿已连续 $consecutiveFails 轮失败, 可能存在未对冲的裸方向敞口, 请人工介入 !!!")
    Thread.sleep(pollMs)
