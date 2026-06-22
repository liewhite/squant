package voltrade

import hft.domain.{Exchange, Greeks}
import hft.exchange.AccountStream
import hft.messaging.{EventBus, EventData, IncomeEvent}
import org.slf4j.LoggerFactory
import ox.{Ox, fork}

/** 把**期权账户净 greeks** 作为 [[AccountStream]] 注入实盘引擎的 income 总线 (与时钟/账户流同一条 bus)：
  * 周期性查 Bybit 期权持仓 -> Σ(delta,gamma) -> 发 [[EventData.GreeksUpdate]]。引擎里的对冲策略经
  * StateManager 读到, 与回测里 BsGreeksSource 喂 greeks 完全同机制。这样**对冲腿不必二次订阅行情**——
  * BBO 复用引擎已有的 [[hft.exchange.bybit.BybitMarketStream]], 这里只补一个期权 greeks 源。 */
final class OptionGreeksStream(opt: OptionsExchange, exch: Exchange, ccy: String, pollMs: Long = 3000L) extends AccountStream:
  private val logger = LoggerFactory.getLogger(classOf[OptionGreeksStream])
  override def exchange: Exchange = exch
  override def start(incomeBus: EventBus[IncomeEvent])(using Ox): Unit =
    fork {
      while true do
        opt.optionAccountGreeks() match
          case Right((delta, gamma)) =>
            incomeBus.publish(IncomeEvent.local(EventData.GreeksUpdate(
              Greeks(exch, ccy, delta = delta, gamma = gamma, theta = 0.0, vega = 0.0, timestamp = System.currentTimeMillis)
            )))
          case Left(e) => logger.error(s"期权 greeks 轮询失败 (对冲腿将沿用上次值): $e")
        Thread.sleep(pollMs)
    }

/** 组合多个账户流为一个 (引擎每交易所只接一个 accountStream)：Bybit 永续账户流 + 期权 greeks 流。 */
final class CompositeAccountStream(exch: Exchange, streams: Seq[AccountStream]) extends AccountStream:
  override def exchange: Exchange = exch
  override def start(incomeBus: EventBus[IncomeEvent])(using Ox): Unit = streams.foreach(_.start(incomeBus))
