package app.live.sell

import app.live.option.OptionsExchange
import org.slf4j.LoggerFactory

/** 卖方腿的**调度编排** (交易所无关, Bybit/OKX 两个启动器共用)：每北京周五 17:00 决策一次,
  * 取数+定量+选腿由 [[VolSell.plan]]、下单由 [[VolSell.execute]]。幂等防重 (当周锚点 + clOrdId 双保险),
  * 两腿其一失败显式告警裸敞口。把这段循环抽出来, 两个启动器只负责构造交易所实现, 不重复编排逻辑。 */
object SellRunner:
  private val logger = LoggerFactory.getLogger("SellRunner")

  /** @param ex     注入的期权交易所实现 (Bybit/OKX)
    * @param cfg    决策配置
    * @param runNow true=启动即决策一次 (不等周五) */
  def run(ex: OptionsExchange, cfg: VolSell.Config, runNow: Boolean): Unit =
    // 幂等防护: 记录上次成功决策的周期锚点, 同一周不重复决策 (叠加 clOrdId 幂等双保险)
    var lastPeriod = -1L

    def decideOnce(now: Long, runNow: Boolean): Unit =
      val anchor = SellVolPlan.decisionAnchor(now, runNow) // runNow=上周五, 常规=本周五; 与 plan 一致
      if anchor == lastPeriod then
        logger.warn(s"本周期 (${java.time.Instant.ofEpochMilli(anchor)}) 已决策过, 跳过 (防重复下单)")
      else
        VolSell.plan(ex, cfg, now, runNow) match
          case Left(err) => logger.error(s"本次决策跳过: $err")
          case Right(d) =>
            logger.warn(f"决策: spot=${d.spot}%.2f 上周RV=${d.rvPrev}%.3f 本周RV=${d.rvThis}%.3f -> ${if d.rvThis > d.rvPrev then "↑卖" else "↓卖"} ${d.mult}×")
            // 列出目标到期下全部可选行权价, 印证选了最近 ATM
            logger.warn(s"期权链 @到期 ${java.time.Instant.ofEpochMilli(d.expiryMs)}: 可选行权 [${d.candidateStrikes.mkString(",")}] (${d.candidateStrikes.size}个), 现价=${d.spot} -> 选中 ATM=${d.atmStrike}")
            logger.warn(s"跨式: ${d.legs.map(l => s"${l.symbol} qty=${l.qty}@${l.price}").mkString(" + ")}")
            val results = VolSell.execute(ex, d)
            results.foreach {
              case (l, Right(id)) => logger.warn(s"卖出 ${l.symbol} qty=${l.qty} @${l.price} PostOnly -> $id")
              case (l, Left(e))   => logger.error(s"卖出 ${l.symbol} 失败: $e")
            }
            val ok = results.count(_._2.isRight)
            if ok == results.size then lastPeriod = anchor // 仅两腿全成才标记完成
            else if ok > 0 then logger.error(s"!!! 跨式不完整: ${ok}/${results.size} 腿成交 -> 存在裸方向敞口, 请人工处理 (撤/补另一腿)")

    if runNow then decideOnce(System.currentTimeMillis, runNow = true) // 以上周五为基准

    // 常驻: 每周五 17:00 北京时间决策 (以本周五为基准)
    while true do
      val now = System.currentTimeMillis
      val next = SellVolPlan.nextDecisionTime(now)
      logger.warn(s"下次决策: ${java.time.Instant.ofEpochMilli(next)} (北京周五17:00), 等待 ${(next - now) / 60000} 分钟")
      Thread.sleep(math.max(1000L, next - now))
      decideOnce(System.currentTimeMillis, runNow = false)
