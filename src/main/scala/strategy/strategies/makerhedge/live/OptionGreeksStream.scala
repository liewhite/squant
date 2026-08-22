package strategy.strategies.makerhedge.live
import strategy.utils.option.*

import hft.domain.{AccountId, Balance, Exchange, Greeks}
import hft.exchange.AccountStream
import hft.event.{Event, EventBus, Topics}
import org.slf4j.LoggerFactory
import ox.{Ox, fork}
import hft.state.{StateManager}

/** 把**期权账户净 greeks** 作为 [[AccountStream]] 注入实盘引擎的 事件总线 (与时钟/账户流同一条 bus)：
  * 周期性经 [[OptionsExchange]] 查期权净 (delta,gamma) -> 发 [[Topics.Greeks]] 事件。引擎里的对冲策略经
  * StateManager 读到, 与回测里 BsGreeksSource 喂 greeks 完全同机制。**交易所无关** (Bybit/OKX 共用), 对冲腿
  * 不必二次订阅行情——BBO 复用引擎已有的行情流, 这里只补一个期权 greeks 源。
  *
  * **关键**: `StateManager.greeks` 要求 greeks 与 `cashBalances(ccy)` 同时存在才返回总 delta; live 下
  * 若交易所钱包帧无该 ccy 条目 (余额 0 常不下发), greeks 永远读不到 -> 对冲静默不触发 -> 期权裸敞口!
  * 故 start 时**同步先发一条 ccy 余额 0** 兜底 (在永续账户流之前 -> 真实现货余额到达会覆盖, 无竞态)。 */
final class OptionGreeksStream(opt: OptionsExchange, exch: Exchange, ccy: String, pollMs: Long = 3000L) extends AccountStream:
  private val logger = LoggerFactory.getLogger(classOf[OptionGreeksStream])
  override def exchange: Exchange = exch

  override def start(eventBus: EventBus)(using Ox): Unit =
    // 同步兜底: 保证 cashBalances(ccy) 存在, 否则 StateManager.greeks 恒为 None -> 不对冲。真实现货余额(若有)随后覆盖。
    eventBus.publish(Event.local(Topics.Balance, Balance(AccountId.Live, exch, ccy, 0.0, System.currentTimeMillis)))
    fork {
      var fails = 0
      while true do
        try
          opt.optionAccountGreeks() match
            case Right((delta, gamma)) =>
              fails = 0
              eventBus.publish(Event.local(Topics.Greeks, Greeks(AccountId.Live, exch, ccy, delta = delta, gamma = gamma, theta = 0.0, vega = 0.0, timestamp = System.currentTimeMillis)))
            case Left(e) =>
              fails += 1
              if fails >= 3 then logger.error(s"!!! 期权 greeks 已连续 $fails 次轮询失败, 对冲在用陈旧 delta, 期权敞口可能失真, 请人工介入: $e")
              else logger.error(s"期权 greeks 轮询失败 (沿用上次值): $e")
        catch case t: Throwable => logger.error(s"期权 greeks 轮询异常 (跳过本轮, 不拖垮引擎): ${t.getMessage}")
        Thread.sleep(pollMs)
    }

/** 组合多个账户流为一个 (引擎每交易所只接一个 accountStream)：Bybit 永续账户流 + 期权 greeks 流。
  * 按传入顺序 start (把 greeks 流的余额兜底排在永续账户流之前)。 */
final class CompositeAccountStream(exch: Exchange, streams: Seq[AccountStream]) extends AccountStream:
  override def exchange: Exchange = exch
  override def start(eventBus: EventBus)(using Ox): Unit = streams.foreach(_.start(eventBus))
