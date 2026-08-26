package strategy.strategies.makerhedge.live
import strategy.utils.option.*

import hft.domain.{AccountId, Balance, Exchange, Greeks}
import hft.exchange.AccountFeed
import hft.event.{AnyEvent, Event, Topics}
import org.slf4j.LoggerFactory

/** 把**期权账户净 greeks** 作为 [[AccountFeed]] 注入柜台的汇报面 (与私有推送同一条通道)：
  * 周期性经 [[OptionsExchange]] 查期权净 (delta,gamma) -> 发 [[Topics.Greeks]] 事件。引擎里的对冲策略经
  * StateManager 读到, 与回测里 BsGreeksSource 喂 greeks 完全同机制。**交易所无关** (Bybit/OKX 共用), 对冲腿
  * 不必二次订阅行情——BBO 复用引擎已有的行情流, 这里只补一个期权 greeks 源。
  *
  * **关键**: `StateManager.greeks` 要求 greeks 与 `cashBalances(ccy)` 同时存在才返回总 delta; live 下
  * 若交易所钱包帧无该 ccy 条目 (余额 0 常不下发), greeks 永远读不到 -> 对冲静默不触发 -> 期权裸敞口!
  * 故 start 时**同步先发一条 ccy 余额 0** 兜底 (在永续账户流之前 -> 真实现货余额到达会覆盖, 无竞态)。 */
final class OptionGreeksStream(opt: OptionsExchange, exch: Exchange, ccy: String, pollMs: Long = 3000L) extends AccountFeed:
  private val logger = LoggerFactory.getLogger(classOf[OptionGreeksStream])
  override def exchange: Exchange = exch

  override def connect(account: AccountId, publish: AnyEvent => Unit, spawn: (=> Unit) => Unit): Unit =
    // 同步兜底: 保证 cashBalances(ccy) 存在, 否则 StateManager.greeks 恒为 None -> 不对冲。真实现货余额(若有)随后覆盖。
    publish(Event.local(Topics.Balance, Balance(account, exch, ccy, 0.0, System.currentTimeMillis)))
    spawn {
      var fails = 0
      while true do
        try
          opt.optionAccountGreeks() match
            case Right((delta, gamma)) =>
              fails = 0
              publish(Event.local(Topics.Greeks, Greeks(account, exch, ccy, delta = delta, gamma = gamma, theta = 0.0, vega = 0.0, timestamp = System.currentTimeMillis)))
            case Left(e) =>
              fails += 1
              if fails >= 3 then logger.error(s"!!! 期权 greeks 已连续 $fails 次轮询失败, 对冲在用陈旧 delta, 期权敞口可能失真, 请人工介入: $e")
              else logger.error(s"期权 greeks 轮询失败 (沿用上次值): $e")
        catch case t: Throwable => logger.error(s"期权 greeks 轮询异常 (跳过本轮, 不拖垮引擎): ${t.getMessage}")
        Thread.sleep(pollMs)
    }

/** 把多个汇报面并成一个 (一个柜台只接一个 [[AccountFeed]])：永续私有流 + 期权 greeks 流。
  * 按传入顺序连接 (把 greeks 流的余额兜底排在永续私有流之前)。 */
final class CompositeAccountFeed(exch: Exchange, feeds: Seq[AccountFeed]) extends AccountFeed:
  require(feeds.forall(_.exchange == exch), s"CompositeAccountFeed 的成员必须同属 $exch")
  override def exchange: Exchange = exch
  override def connect(account: AccountId, publish: AnyEvent => Unit, spawn: (=> Unit) => Unit): Unit =
    feeds.foreach(_.connect(account, publish, spawn))
