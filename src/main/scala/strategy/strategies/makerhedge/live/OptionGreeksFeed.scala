package strategy.strategies.makerhedge.live
import strategy.utils.option.*

import hft.domain.Exchange
import hft.exchange.{AccountFeed, AccountReport}
import org.slf4j.LoggerFactory

/** 把**期权账户净 greeks** 作为 [[AccountFeed]] 注入柜台的汇报面 (与私有推送同一条通道)：
  * 周期性经 [[OptionsExchange]] 查期权净 (delta,gamma) -> 发 [[Topics.Greeks]] 事件。引擎里的对冲策略经
  * StateManager 读到, 与回测里 BsGreeksSource 喂 greeks 完全同机制。**交易所无关** (Bybit/OKX 共用), 对冲腿
  * 不必二次订阅行情——BBO 复用引擎已有的行情流, 这里只补一个期权 greeks 源。
  *
  * 只发 greeks, 不碰余额: "该币种余额为 0"这件事由**启动对齐的全量钱包快照**给出
  * (见 [[hft.domain.Wallet]]), 不需要这里注入一条假的。 */
final class OptionGreeksFeed(opt: OptionsExchange, exch: Exchange, ccy: String, pollMs: Long = 3000L) extends AccountFeed:
  private val logger = LoggerFactory.getLogger(classOf[OptionGreeksFeed])
  override def exchange: Exchange = exch

  override def connect(
      report: AccountReport => Unit,
      spawn: (=> Unit) => Unit,
      sleepUnlessStopped: Long => Boolean,
  ): Unit =
    // 这里**不再注入一条假的 ccy 余额**。
    //
    // 从前的做法是同步发一条 `BalanceChanged(ccy, 0.0)` 把 cashBalances 的键占上, 否则
    // StateManager.greeks 恒为 None、对冲静默不触发。它解决的是真问题 (OKX 余额为 0 时不下发
    // 该币种行), 但用的是一句谎话: 真实余额非零的账户在钱包快照到达前会按 0 对冲, 而
    // "现货余额"正是总敞口的一部分。
    // 现在"没列出即为 0"有真凭据: 启动对齐会拉一次 REST 钱包 (整份返回) 并发 Topics.Wallet,
    // 之后由逐币种的 Balance 推送维持。三家的钱包 WS 通道都只覆盖变动币种, 给不出这个事实,
    // 详见 hft.domain.Wallet。
    spawn {
      var fails = 0
      // 先拉一次再进等待: 否则启动后第一份 greeks 被推迟一个轮询周期, 策略在此期间
      // 按"读数陈旧"暂停对冲。协作式睡眠使停机请求立即唤醒 (见 AccountFeed.connect 契约)。
      var running = true
      while running do
        try
          opt.optionAccountGreeks() match
            case Right((delta, gamma)) =>
              fails = 0
              report(AccountReport.GreeksChanged(ccy, delta = delta, gamma = gamma, theta = 0.0, vega = 0.0, timestamp = System.currentTimeMillis))
            case Left(e) =>
              fails += 1
              if fails >= OptionGreeksFeed.StaleAfterFailures then
                // 策略在 greeks 陈旧时会暂停对冲 (maxGreeksStaleMs), 因此这里的后果是"暂停", 不是"用陈旧 delta"
                logger.error(s"!!! 期权 greeks 已连续 $fails 次轮询失败, 策略将因读数陈旧暂停对冲, 期权敞口无人管, 请人工介入: $e")
              else logger.error(s"期权 greeks 轮询失败 (本轮无新读数): $e")
        // 只捕 NonFatal: InterruptedException 必须让循环终止, 否则这条受管任务停不下来,
        // 框架只能把组件隔离 (Quarantined) 并终结进程。
        catch
          case scala.util.control.NonFatal(e) =>
            logger.error(s"期权 greeks 轮询异常 (跳过本轮): ${e.getMessage}", e)
        running = !sleepUnlessStopped(pollMs)
    }

/** 把多个汇报面并成一个 (一个柜台只接一个 [[AccountFeed]])：永续私有流 + 期权 greeks 流。
  * 按传入顺序连接。顺序不再承载任何约定 —— 从前 greeks 流会先同步发一条假的 ccy 余额把键
  * "占上", 那条约束现在由启动对齐的全量钱包快照如实给出 (见 hft.domain.Wallet)。 */
final class CompositeAccountFeed(exch: Exchange, feeds: Seq[AccountFeed]) extends AccountFeed:
  require(feeds.forall(_.exchange == exch), s"CompositeAccountFeed 的成员必须同属 $exch")
  override def exchange: Exchange = exch
  override def connect(
      report: AccountReport => Unit,
      spawn: (=> Unit) => Unit,
      sleepUnlessStopped: Long => Boolean,
  ): Unit =
    feeds.foreach(_.connect(report, spawn, sleepUnlessStopped))

object OptionGreeksFeed:
  /** 连续轮询失败多少次后升级为 error —— 到这个次数时读数已足够陈旧, 策略会暂停对冲。 */
  val StaleAfterFailures: Int = 3
