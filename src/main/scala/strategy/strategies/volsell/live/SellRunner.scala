package strategy.strategies.volsell.live
import strategy.strategies.volsell.logic.{SellVolPlan, VolSell}

import strategy.utils.option.OptionsExchange
import org.slf4j.LoggerFactory
import ox.sleep

import scala.concurrent.duration.*

/** 卖方腿的**调度编排** (交易所无关, Bybit/OKX 两个启动器共用)：每北京周五 17:00 决策一次,
  * 取数+定量+选腿由 [[VolSell.plan]]、下单由 [[VolSell.execute]]、结局判定由 [[VolSell.Outcome]]。
  * 幂等防重 (当周锚点 + clOrdId 双保险)。把这段循环抽出来, 两个启动器只负责构造交易所实现, 不重复编排逻辑。
  *
  * 这里**只有调度与日志**：什么算"本周做完"、部分成交是不是敞口，都在 [[VolSell.Outcome]] 里,
  * 那份判定可以单测, 这个 `while true` 不能。
  */
object SellRunner:
  private val logger = LoggerFactory.getLogger("SellRunner")

  /** @param ex     注入的期权交易所实现 (Bybit/OKX)
    * @param cfg    决策配置
    * @param runNow true=启动即决策一次 (不等周五) */
  def run(ex: OptionsExchange, cfg: VolSell.Config, runNow: Boolean): Unit =
    // 幂等防护: 记录上次**两腿全成**的周期锚点, 同一周不重复决策 (叠加 clOrdId 幂等双保险)
    var completedPeriod: Option[Long] = None

    def decideOnce(now: Long, runNow: Boolean): Unit =
      val anchor = SellVolPlan.decisionAnchor(now, runNow) // runNow=上周五, 常规=本周五; 与 plan 一致
      if completedPeriod.contains(anchor) then
        logger.warn(s"本周期 (${java.time.Instant.ofEpochMilli(anchor)}) 已决策过, 跳过 (防重复下单)")
      else
        VolSell.plan(ex, cfg, now, runNow) match
          case Left(err) => logger.error(s"本次决策跳过: $err")
          case Right(d) =>
            logger.warn(f"决策: spot=${d.spot}%.2f 上周RV=${d.rvPrev}%.3f 本周RV=${d.rvThis}%.3f -> ${if d.rvThis > d.rvPrev then "↑卖" else "↓卖"} ${d.mult}×")
            // 列出目标到期下全部可选行权价, 印证宽跨选了贴近现价两侧
            logger.warn(s"期权链 @到期 ${java.time.Instant.ofEpochMilli(d.expiryMs)}: 可选行权 [${d.candidateStrikes.mkString(",")}] (${d.candidateStrikes.size}个), 现价=${d.spot} -> 宽跨 put=${d.putStrike}/call=${d.callStrike}")
            logger.warn(s"宽跨: ${d.legs.map(l => s"${l.symbol} qty=${l.qty}@${l.price}${if l.postOnly then "(maker)" else "(taker)"}").mkString(" + ")}")
            val results = VolSell.execute(ex, d)
            results.foreach {
              case (l, Right(id)) => logger.warn(s"卖出 ${l.symbol} qty=${l.qty} @${l.price} PostOnly -> $id")
              case (l, Left(e))   => logger.error(s"卖出 ${l.symbol} 失败: $e")
            }
            VolSell.Outcome.of(results) match
              case VolSell.Outcome.Complete =>
                completedPeriod = Some(anchor)
              case VolSell.Outcome.Naked(submitted, total) =>
                logger.error(s"!!! 跨式不完整: $submitted/$total 腿已提交 -> 存在裸方向敞口, 请人工处理 (撤/补另一腿)")
              case VolSell.Outcome.AllFailed(total) =>
                logger.error(s"$total 腿全部提交失败 -> 无敞口, 等下一个周期重试")

    if runNow then decideOnce(System.currentTimeMillis, runNow = true) // 以上周五为基准

    // 常驻: 每周五 17:00 北京时间决策 (以本周五为基准)
    while true do
      val now = System.currentTimeMillis
      val next = SellVolPlan.nextDecisionTime(now)
      // nextDecisionTime 的契约是"from 之后最近的周五 17:00", 严格晚于 from。从前这里写
      // `math.max(1000L, next - now)`: 一旦那个契约破了 (时区/夏令时算错), 循环会以 1 秒一轮
      // 空转并每秒重决策一次, 而唯一的症状是日志变多。
      val waitMs = next - now
      require(waitMs > 0, s"下次决策时点 $next 未晚于当前 $now —— nextDecisionTime 契约被破坏")
      logger.warn(s"下次决策: ${java.time.Instant.ofEpochMilli(next)} (北京周五17:00), 等待 ${waitMs / 60000} 分钟")
      sleep(waitMs.millis)
      decideOnce(System.currentTimeMillis, runNow = false)
