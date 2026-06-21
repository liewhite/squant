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

  logger.warn(s"VolSell 启动: symbol=$symbol baseCoin=$baseCoin tenor=${targetDays}d 网格=$gridHigh/$gridLow 基准张数=$baseQty")
  logger.warn(s"模式: ${if live then "*** 实盘 LIVE ***" else "dry-run (不下单)"}  ${if testnet then "testnet" else "mainnet"}  credentials=${credentials.isDefined}")
  if live && credentials.isEmpty then logger.error("VOLSELL_LIVE=1 但缺少 API key, 将无法下单")

  def decideOnce(): Unit =
    val now = System.currentTimeMillis
    val outcome =
      for
        closes <- ex.underlyingCloses5m(symbol, bars2w)
        _ <- Either.cond(closes.sizeIs >= 4, (), s"K线不足: ${closes.size}")
        spot <- closes.lastOption.toRight("无现价")
        chain <- ex.optionChain(baseCoin)
        straddle <- SellVolPlan.selectStraddle(chain, now, spot, targetDays).toRight("未找到 ~21天 ATM 跨式")
      yield
        val (mult, rvPrev, rvThis) = SellVolPlan.decideMultiplier(closes, gridHigh, gridLow)
        val qty = baseQty * mult
        val (call, put) = straddle
        logger.warn(f"决策: spot=$spot%.2f 上周RV=$rvPrev%.3f 本周RV=$rvThis%.3f -> ${if rvThis > rvPrev then "↑卖" else "↓卖"} ${mult}× (qty=$qty)")
        logger.warn(s"标的跨式: ${call.symbol} + ${put.symbol} (到期=${java.time.Instant.ofEpochMilli(call.expiryMs)})")
        Seq(call, put).foreach { inst =>
          val bid = ex.optionBestBid(inst.symbol).toOption.flatten // PostOnly 卖挂最优买价
          val link = s"vs-${now}-${if inst.right == OptionRight.Call then "c" else "p"}".take(36)
          ex.sellOption(inst.symbol, qty, bid, link) match
            case Right(id) => logger.warn(s"卖出 ${inst.symbol} qty=$qty @${bid.fold("MKT")(_.toString)} -> orderId=$id")
            case Left(err) => logger.error(s"卖出 ${inst.symbol} 失败: $err")
        }
    outcome.left.foreach(err => logger.error(s"本次决策跳过: $err"))

  if runNow then decideOnce()

  // 常驻: 每周五 15:00 北京时间决策
  while true do
    val now = System.currentTimeMillis
    val next = SellVolPlan.nextDecisionTime(now)
    logger.warn(s"下次决策: ${java.time.Instant.ofEpochMilli(next)} (北京时间周五15:00), 等待 ${(next - now) / 1000 / 60} 分钟")
    Thread.sleep(math.max(1000L, next - now))
    decideOnce()
