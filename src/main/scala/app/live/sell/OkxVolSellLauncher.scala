package app.live.sell
import app.live.option.*

import hft.exchange.okx.OkxCredentials
import org.slf4j.LoggerFactory
import sttp.client4.DefaultSyncBackend

/** 卖方期权**实盘**启动器 (OKX)。与 [[VolSellLauncher]] (Bybit) 同一决策编排 [[VolSell]], 仅交易所实现不同
  * (注入 [[OkxOptionsClient]])——策略逻辑零改动, 体现 [[OptionsExchange]] 抽象。**无 dry-run, 启动即真实下单**。
  *
  * 每**北京时间周五 15:00** 决策一次: 取标的最近 2 周 5min K 线算 RV → 本周较上周升则卖 2×、降则 1× →
  * 选 ~21 天到期 ATM 跨式 → 以最优卖价 PostOnly 卖出 call+put。
  *
  * **安全护栏** (替代 dry-run): ① 必须有 API key+passphrase; ② `VOLSELL_QTY` 小仓 (默认1); ③ `VOLSELL_MAX_QTY`
  * 单腿张数硬上限; ④ 幂等 clOrdId + 当周防重。`VOLSELL_SIMULATED=1` 走 OKX 模拟盘 (real order, 非 dry-run)。
  * `VOLSELL_NOW=1` 立即决策一次 (不等周五)。
  *
  * **OKX 符号约定**: 标的用基础币 (VOLSELL_SYMBOL=ETH), 内部拼 `ETH-<quote>-SWAP` 取永续 K 线; 期权为币本位
  * (instFamily=ETH-USD)。
  *
  * 运行: OKX_API_KEY=.. OKX_API_SECRET=.. OKX_PASSPHRASE=.. sbt "runMain app.live.OkxVolSellLauncher"
  */
@main def OkxVolSellLauncher(): Unit =
  val logger = LoggerFactory.getLogger("OkxVolSellLauncher")
  val symbol = sys.env.getOrElse("VOLSELL_SYMBOL", "ETH") // OKX 标的=基础币
  val baseCoin = sys.env.getOrElse("VOLSELL_BASECOIN", "ETH")
  val quote = sys.env.getOrElse("VOLSELL_QUOTE", "USDT")
  val targetDays = sys.env.get("VOLSELL_TENOR_DAYS").map(_.toInt).getOrElse(21)
  val gridHigh = sys.env.get("VOLSELL_GRID_HIGH").map(_.toDouble).getOrElse(2.0)
  val gridLow = sys.env.get("VOLSELL_GRID_LOW").map(_.toDouble).getOrElse(1.0)
  val baseQty = sys.env.get("VOLSELL_QTY").map(_.toDouble).getOrElse(1.0) // 基准张数, 默认 1 (小仓)
  val maxQty = sys.env.get("VOLSELL_MAX_QTY").map(_.toDouble).getOrElse(baseQty * 3) // 单腿硬上限 (sanity)
  val bars2w = sys.env.get("VOLSELL_RV_BARS").map(_.toInt).getOrElse(2 * 7 * 24 * 12) // 2周5min=4032
  val simulated = sys.env.get("VOLSELL_SIMULATED").contains("1")
  val runNow = sys.env.get("VOLSELL_NOW").contains("1")

  val credentials =
    for k <- sys.env.get("OKX_API_KEY"); s <- sys.env.get("OKX_API_SECRET"); p <- sys.env.get("OKX_PASSPHRASE")
    yield OkxCredentials(k, s, p)
  if credentials.isEmpty then
    logger.error("缺 OKX_API_KEY/SECRET/PASSPHRASE, 实盘下单需签名, 退出")
    sys.exit(1)
  val backend = DefaultSyncBackend()
  val ex: OptionsExchange = OkxOptionsClient(backend, credentials, quote = quote, simulated = simulated)
  val cfg = VolSell.Config(symbol, baseCoin, targetDays, gridHigh, gridLow, baseQty, bars2w, maxQty)

  logger.warn(s"OkxVolSell *** 实盘 LIVE *** (无 dry-run, 真实下单): $cfg")
  logger.warn(s"${if simulated then "模拟盘(simulated)" else "主网(mainnet)"}  单腿硬上限=$maxQty")

  // 幂等防护: 记录上次成功决策的周期锚点, 同一周不重复决策 (叠加 clOrdId 幂等双保险)
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
