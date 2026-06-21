package voltrade

import hft.exchange.bybit.BybitCredentials
import org.slf4j.LoggerFactory
import sttp.client4.DefaultSyncBackend

/** 卖方期权实盘启动器 (Bybit, 隔离 package)。
  *
  * 每**北京时间周五 15:00** 决策一次: 取标的最近 2 周 5min K 线算 RV → 本周较上周升则卖 2×、降则 1× →
  * 选 ~21 天到期 ATM 跨式 → 以最优买价 PostOnly 卖出 call+put。
  *
  * **安全默认**: dryRun (只打日志不下单)。设 `VOLSELL_LIVE=1` + BYBIT_API_KEY/SECRET 才真实下单;
  * `VOLSELL_TESTNET=1` 走 testnet (上线前务必先 testnet 跑通: 符号/张数单位/精度/最小量)。
  * `VOLSELL_NOW=1` 立即决策一次 (便于 dry-run 观察, 不等周五)。
  *
  * 运行: VOLSELL_NOW=1 sbt "runMain voltrade.VolSellLauncher"
  */
@main def VolSellLauncher(): Unit =
  val logger = LoggerFactory.getLogger("VolSellLauncher")
  val symbol = sys.env.getOrElse("VOLSELL_SYMBOL", "ETHUSDT")
  val baseCoin = sys.env.getOrElse("VOLSELL_BASECOIN", "ETH")
  val targetDays = sys.env.get("VOLSELL_TENOR_DAYS").map(_.toInt).getOrElse(21)
  val gridHigh = sys.env.get("VOLSELL_GRID_HIGH").map(_.toDouble).getOrElse(2.0)
  val gridLow = sys.env.get("VOLSELL_GRID_LOW").map(_.toDouble).getOrElse(1.0)
  val baseQty = sys.env.get("VOLSELL_QTY").map(_.toDouble).getOrElse(1.0) // 基准张数, 默认 1 (小仓)
  val bars2w = sys.env.get("VOLSELL_RV_BARS").map(_.toInt).getOrElse(2 * 7 * 24 * 12) // 2周5min=4032
  val live = sys.env.get("VOLSELL_LIVE").contains("1")
  val testnet = sys.env.get("VOLSELL_TESTNET").contains("1")
  val runNow = sys.env.get("VOLSELL_NOW").contains("1")

  val credentials =
    for k <- sys.env.get("BYBIT_API_KEY"); s <- sys.env.get("BYBIT_API_SECRET") yield BybitCredentials(k, s)
  val backend = DefaultSyncBackend()
  val ex: OptionsExchange = BybitOptionsClient(backend, credentials, dryRun = !live, testnet = testnet)
  val cfg = VolSell.Config(symbol, baseCoin, targetDays, gridHigh, gridLow, baseQty, bars2w)

  logger.warn(s"VolSell 启动: $cfg")
  logger.warn(s"模式: ${if live then "*** 实盘 LIVE ***" else "dry-run (不下单)"}  ${if testnet then "testnet" else "mainnet"}  credentials=${credentials.isDefined}")
  if live && credentials.isEmpty then logger.error("VOLSELL_LIVE=1 但缺少 API key, 将无法下单")

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
