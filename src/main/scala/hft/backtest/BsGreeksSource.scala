package hft.backtest

import hft.domain.*
import hft.messaging.{EventData, IncomeEvent}
import hft.option.{BlackScholes, OptionPosition}

/** BS 合成希腊字母配置。
  *
  * @param exchange        希腊字母事件标记的交易所 (策略据 (exchange, ccy) 查询)；回测中可设为
  *                        Exchange.Okx 以模拟实盘 OKX 账户 greeks，或与标的所在交易所一致
  * @param ccy             币种, e.g. "BTC"
  * @param underlyingSymbol 读取标的价 S 的 symbol (取最新成交价), e.g. "BTCUSDT"
  * @param positions       期权持仓组合 (按持仓数量聚合为账户级希腊字母)
  * @param impliedVol      年化隐含波动率 (BS 输入假设)
  * @param riskFreeRate    无风险年化利率
  * @param spotHolding     现货持有量 (币本位)，作为 cashBal 发布；StateManager 据此修正 delta。
  *                        纯期权 + 永续对冲场景通常为 0
  * @param emitIntervalMs  希腊字母发射间隔 (虚拟时间, ms)，默认 1000 对齐实盘 OKX 轮询节奏，
  *                        避免每个 L1 tick 都重算发布
  */
final case class BsGreeksConfig(
    exchange: Exchange,
    ccy: String,
    underlyingSymbol: Symbol,
    positions: Vector[OptionPosition],
    impliedVol: Double,
    riskFreeRate: Double = 0.0,
    spotHolding: Double = 0.0,
    emitIntervalMs: Long = 1000,
)

/** 回测用 BS 合成希腊字母数据源装饰器。
  *
  * 监听上游标的 [[EventData.MarketTradeUpdate]] (真实逐笔成交价 S)，按虚拟时间间隔用 Black-Scholes
  * 计算各期权持仓的希腊字母、**按数量聚合为与 OKX `account/greeks` 同形态的 per-ccy 账户级 [[Greeks]]**，
  * 紧随该 trade 以**相同 exchangeTs** 注入 [[EventData.GreeksUpdate]]。同时一次性注入现货 cashBal
  * ([[EventData.BalanceUpdate]])，使 [[hft.messaging.StateManager.greeks]] 的 delta 修正得以生效。
  *
  * 由此回测与实盘喂入同一 Greeks 通道，策略代码对两者无感 (实盘走 OKX REST 轮询)。
  *
  * 时间衰减 (theta/剩余期限) 随事件 exchangeTs 自然推进；其余事件原样透传。
  */
object BsGreeksSource:
  /** 与 [[hft.option.BlackScholes.MillisPerYear]] 一致的天数基准，用于 theta 每年->每日换算 */
  val DaysPerYear: Double = 365.0

final class BsGreeksSource(underlying: MarketDataSource, config: BsGreeksConfig) extends MarketDataSource:

  override def events(): Iterator[IncomeEvent] =
    var lastEmit: Timestamp = Long.MinValue
    var balanceEmitted = false

    underlying.events().flatMap { ev =>
      ev.data match
        case EventData.MarketTradeUpdate(t) if t.symbol == config.underlyingSymbol && shouldEmit(ev.exchangeTs, lastEmit) =>
          lastEmit = ev.exchangeTs
          val greeksEv = ev.copy(data = EventData.GreeksUpdate(computeGreeks(t.price, ev.exchangeTs)))
          // cashBal 只在首次发布 (回测中现货持有量恒定)，使 greeks() 修正项就绪
          if balanceEmitted then Iterator(ev, greeksEv)
          else
            balanceEmitted = true
            val balanceEv = ev.copy(
              data = EventData.BalanceUpdate(Balance(config.exchange, config.ccy, config.spotHolding, ev.exchangeTs))
            )
            Iterator(ev, balanceEv, greeksEv)
        case _ => Iterator.single(ev)
    }

  private def shouldEmit(now: Timestamp, lastEmit: Timestamp): Boolean =
    lastEmit == Long.MinValue || now - lastEmit >= config.emitIntervalMs

  /** 按持仓聚合各期权的 BS 希腊字母为账户级 [[Greeks]]。
    *
    * BS 输出为数学约定 (theta 每年、vega 对 1.0 波动率)，此处换算到 Greeks **通道规范单位**
    * (theta 每日、vega 对 1%)，使实盘 OKX 与回测喂入同一通道时单位一致。
    */
  private def computeGreeks(s: Double, now: Timestamp): Greeks =
    var delta = 0.0
    var gamma = 0.0
    var thetaPerYear = 0.0
    var vegaPer1 = 0.0
    config.positions.foreach { pos =>
      val tYears = (pos.spec.expiry - now) / BlackScholes.MillisPerYear
      val g = BlackScholes.greeks(pos.spec.right, s, pos.spec.strike, tYears, config.impliedVol, config.riskFreeRate)
      delta += pos.quantity * g.delta
      gamma += pos.quantity * g.gamma
      thetaPerYear += pos.quantity * g.theta
      vegaPer1 += pos.quantity * g.vega
    }
    Greeks(
      exchange = config.exchange,
      ccy = config.ccy,
      delta = delta,
      gamma = gamma,
      theta = thetaPerYear / BsGreeksSource.DaysPerYear, // 每年 -> 每日
      vega = vegaPer1 / 100.0,                           // 对 1.0 -> 对 1%
      timestamp = now,
    )
