package voltrade

import hft.exchange.bybit.BybitCredentials
import org.slf4j.LoggerFactory
import sttp.client4.DefaultSyncBackend

/** 卖方期权**实盘**启动器 (Bybit, 隔离 package)。**无 dry-run, 启动即真实下单** —— 用小资金测试。
  *
  * 每**北京时间周五 15:00** 决策一次: 取标的最近 2 周 5min K 线算 RV → 本周较上周升则卖 2×、降则 1× →
  * 选 ~21 天到期 ATM 跨式 → 以最优卖价 PostOnly 卖出 call+put。
  *
  * **安全护栏** (替代 dry-run): ① 必须有 API key; ② `VOLSELL_QTY` 小仓 (默认1); ③ `VOLSELL_MAX_QTY`
  * 单腿张数硬上限 (默认=基准×3, 超出整体跳过+告警, 防 scale bug 误下巨单); ④ 幂等 orderLinkId + 当周防重。
  * `VOLSELL_TESTNET=1` 走 testnet (real order, 非 dry-run)。`VOLSELL_NOW=1` 立即决策一次 (不等周五)。
  *
  * 运行: BYBIT_API_KEY=.. BYBIT_API_SECRET=.. sbt "runMain voltrade.VolSellLauncher"
  */
@main def VolSellLauncher(): Unit =
  val logger = LoggerFactory.getLogger("VolSellLauncher")
  val symbol = sys.env.getOrElse("VOLSELL_SYMBOL", "ETHUSDT")
  val baseCoin = sys.env.getOrElse("VOLSELL_BASECOIN", "ETH")
  val targetDays = sys.env.get("VOLSELL_TENOR_DAYS").map(_.toInt).getOrElse(21)
  val gridHigh = sys.env.get("VOLSELL_GRID_HIGH").map(_.toDouble).getOrElse(2.0)
  val gridLow = sys.env.get("VOLSELL_GRID_LOW").map(_.toDouble).getOrElse(1.0)
  val baseQty = sys.env.get("VOLSELL_QTY").map(_.toDouble).getOrElse(1.0) // 基准张数, 默认 1 (小仓)
  val maxQty = sys.env.get("VOLSELL_MAX_QTY").map(_.toDouble).getOrElse(baseQty * 3) // 单腿硬上限 (sanity)
  val bars2w = sys.env.get("VOLSELL_RV_BARS").map(_.toInt).getOrElse(2 * 7 * 24 * 12) // 2周5min=4032
  val testnet = sys.env.get("VOLSELL_TESTNET").contains("1")
  val runNow = sys.env.get("VOLSELL_NOW").contains("1")

  val credentials =
    for k <- sys.env.get("BYBIT_API_KEY"); s <- sys.env.get("BYBIT_API_SECRET") yield BybitCredentials(k, s)
  if credentials.isEmpty then
    logger.error("缺 BYBIT_API_KEY/SECRET, 实盘下单需签名, 退出")
    sys.exit(1)
  val backend = DefaultSyncBackend()
  val ex: OptionsExchange = BybitOptionsClient(backend, credentials, testnet = testnet)
  val cfg = VolSell.Config(symbol, baseCoin, targetDays, gridHigh, gridLow, baseQty, bars2w, maxQty)

  logger.warn(s"VolSell *** 实盘 LIVE *** (无 dry-run, 真实下单): $cfg")
  logger.warn(s"${if testnet then "testnet" else "mainnet"}  单腿硬上限=$maxQty")

  // 幂等防护: 记录上次成功决策的周期锚点, 同一周不重复决策 (叠加 orderLinkId 幂等双保险)
  var lastPeriod = -1L

  def decideOnce(now: Long): Unit =
    val period = SellVolPlan.currentDecisionTime(now)
    if period == lastPeriod then
      logger.warn(s"本周期 (${java.time.Instant.ofEpochMilli(period)}) 已决策过, 跳过 (防重复下单)")
    else
      VolSell.plan(ex, cfg, now) match
        case Left(err) => logger.error(s"本次决策跳过: $err")
        case Right(d) =>
          logger.warn(f"决策: spot=${d.spot}%.2f 上周RV=${d.rvPrev}%.3f 本周RV=${d.rvThis}%.3f -> ${if d.rvThis > d.rvPrev then "↑卖" else "↓卖"} ${d.mult}×")
          logger.warn(s"跨式 (到期=${java.time.Instant.ofEpochMilli(d.expiryMs)}): ${d.legs.map(l => s"${l.symbol} qty=${l.qty}@${l.price}").mkString(" + ")}")
          val results = VolSell.execute(ex, d)
          results.foreach {
            case (l, Right(id)) => logger.warn(s"卖出 ${l.symbol} qty=${l.qty} @${l.price} PostOnly -> $id")
            case (l, Left(e))   => logger.error(s"卖出 ${l.symbol} 失败: $e")
          }
          val ok = results.count(_._2.isRight)
          if ok == results.size then lastPeriod = period // 仅两腿全成才标记完成
          else if ok > 0 then logger.error(s"!!! 跨式不完整: ${ok}/${results.size} 腿成交 -> 存在裸方向敞口, 请人工处理 (撤/补另一腿)")

  if runNow then decideOnce(System.currentTimeMillis)

  // 常驻: 每周五 15:00 北京时间决策
  while true do
    val now = System.currentTimeMillis
    val next = SellVolPlan.nextDecisionTime(now)
    logger.warn(s"下次决策: ${java.time.Instant.ofEpochMilli(next)} (北京周五15:00), 等待 ${(next - now) / 60000} 分钟")
    Thread.sleep(math.max(1000L, next - now))
    decideOnce(System.currentTimeMillis)
