package hft.backtest

import hft.domain.*
import hft.messaging.{EventData, IncomeEvent}
import hft.option.{BlackScholes, OptionRight}

/** 单只 ATM 跨式的 BS 合成希腊字母配置 (不滚动，持有到 [[expiry]])。
  *
  * 在首笔成交时以当时价为行权价开一份 [[straddles]] 份的 ATM 长跨式 (long call + long put)，
  * 持有至 [[expiry]]。回测时把 expiry 设为**回测结束日**即可让期权全程存活、无需滚动，
  * 同时避免期权中途到期后 BS 退化。注意临近 expiry 时 ATM gamma/theta 上升 (持有到期固有特性)。
  *
  * @param straddles      跨式份数 (N 份 = long N call + N put ATM)
  * @param expiry         到期时间 (ms epoch)，回测中设为回测结束日 -> tenor ≈ 回测周期
  * @param emitIntervalMs 希腊字母发射间隔 (虚拟时间)，默认 1000 对齐实盘 OKX 轮询节奏
  */
final case class BsGreeksConfig(
    exchange: Exchange,
    ccy: String,
    underlyingSymbol: Symbol,
    straddles: Double,
    impliedVol: Double,
    expiry: Timestamp,
    riskFreeRate: Double = 0.0,
    spotHolding: Double = 0.0,
    emitIntervalMs: Long = 1000,
    /** 剩余期限下限 (天)：临近到期按此钳制，避免到期日 ATM gamma/theta 奇点 (T->0 时发散)。
      * 末段不再衰减 (保留约 minTenorDays 的时间价值)，对长周期影响可忽略。 */
    minTenorDays: Double = 1.0,
)

object BsGreeksSource:
  /** 与 [[hft.option.BlackScholes.MillisPerYear]] 一致的天数基准，用于 theta 每年->每日换算 */
  val DaysPerYear: Double = 365.0

/** 回测用 BS 合成希腊字母数据源装饰器 (单只 ATM 跨式，持有到期，不滚动)。
  *
  * 监听上游标的 [[EventData.MarketTradeUpdate]] (真实逐笔成交价 S)，首笔成交开一份 ATM 长跨式，
  * 按虚拟时间间隔聚合为与 OKX `account/greeks` 同形态的 per-ccy 账户级 [[Greeks]]，紧随该 trade 以
  * **相同 exchangeTs** 注入 [[EventData.GreeksUpdate]]；并一次性注入现货 cashBal 使 delta 修正生效。
  *
  * **期权腿 P&L** = 当前跨式价值 − 进场权利金 (单只持仓，[[optionPnl]] 供 demo 取完整期权腿损益)。
  * 单位约定同 Greeks 通道 (theta 每日、vega 对 1%)。
  */
final class BsGreeksSource(underlying: MarketDataSource, config: BsGreeksConfig) extends MarketDataSource:
  import BsGreeksSource.DaysPerYear

  private var strike = 0.0
  private var entryPremium = 0.0
  private var inited = false

  override def events(): Iterator[IncomeEvent] =
    var lastEmit: Timestamp = Long.MinValue
    var balanceEmitted = false

    underlying.events().flatMap { ev =>
      ev.data match
        case EventData.MarketTradeUpdate(t) if t.symbol == config.underlyingSymbol =>
          val now = ev.exchangeTs
          val s = t.price
          if !inited then
            strike = s // ATM = 首笔成交价
            entryPremium = straddleValue(s, now)
            inited = true

          if shouldEmit(now, lastEmit) then
            lastEmit = now
            val greeksEv = ev.copy(data = EventData.GreeksUpdate(greeksAt(s, now)))
            if balanceEmitted then Iterator(ev, greeksEv)
            else
              balanceEmitted = true
              val balanceEv = ev.copy(
                data = EventData.BalanceUpdate(Balance(config.exchange, config.ccy, config.spotHolding, now))
              )
              Iterator(ev, balanceEv, greeksEv)
          else Iterator.single(ev)
        case _ => Iterator.single(ev)
    }

  private def shouldEmit(now: Timestamp, lastEmit: Timestamp): Boolean =
    lastEmit == Long.MinValue || now - lastEmit >= config.emitIntervalMs

  /** 剩余年限，带下限钳制 (避免到期日 gamma/theta 奇点) */
  private def tYears(now: Timestamp): Double =
    math.max((config.expiry - now) / BlackScholes.MillisPerYear, config.minTenorDays / DaysPerYear)

  /** 跨式在 (s, now) 的理论价值 */
  private def straddleValue(s: Double, now: Timestamp): Double =
    val tY = tYears(now)
    val call = BlackScholes.greeks(OptionRight.Call, s, strike, tY, config.impliedVol, config.riskFreeRate).price
    val put = BlackScholes.greeks(OptionRight.Put, s, strike, tY, config.impliedVol, config.riskFreeRate).price
    config.straddles * (call + put)

  /** 跨式聚合为账户级 Greeks (单位: theta 每日、vega 对 1%，与通道约定一致) */
  private def greeksAt(s: Double, now: Timestamp): Greeks =
    val tY = tYears(now)
    val call = BlackScholes.greeks(OptionRight.Call, s, strike, tY, config.impliedVol, config.riskFreeRate)
    val put = BlackScholes.greeks(OptionRight.Put, s, strike, tY, config.impliedVol, config.riskFreeRate)
    val n = config.straddles
    Greeks(
      exchange = config.exchange,
      ccy = config.ccy,
      delta = n * (call.delta + put.delta),
      gamma = n * (call.gamma + put.gamma),
      theta = n * (call.theta + put.theta) / DaysPerYear, // 每年 -> 每日
      vega = n * (call.vega + put.vega) / 100.0,           // 对 1.0 -> 对 1%
      timestamp = now,
    )

  /** 期权腿 P&L = 当前跨式价值 − 进场权利金 (单只持仓，供 demo 在 run 后查询) */
  def optionPnl(s: Double, now: Timestamp): Double = straddleValue(s, now) - entryPremium

  /** ATM 行权价 (首笔成交价) */
  def strikePrice: Double = strike
