package hft.backtest

import hft.domain.*
import hft.messaging.{EventData, IncomeEvent}
import hft.option.{BlackScholes, OptionRight}

/** 滚动 ATM 跨式的 BS 合成希腊字母配置。
  *
  * 持有 [[straddles]] 份 ATM 长跨式 (long call + long put)，临近到期 ([[rollBeforeDays]] 天内) 滚动到
  * 以当前价为行权价的新近月跨式 —— 保证 gamma 全程存活 (避免期权中途到期后 BS 退化为阶跃 delta)，
  * 并天然包含滚动的 theta 成本。
  *
  * @param straddles      跨式份数 (N 份 = long N call + N put ATM)
  * @param tenorDays      新开期权的期限 (天)
  * @param rollBeforeDays 剩余期限 <= 此值即滚动 (避开到期日 gamma 奇点)
  * @param emitIntervalMs 希腊字母发射间隔 (虚拟时间)，默认 1000 对齐实盘 OKX 轮询节奏
  */
final case class BsGreeksConfig(
    exchange: Exchange,
    ccy: String,
    underlyingSymbol: Symbol,
    straddles: Double,
    impliedVol: Double,
    tenorDays: Double = 30.0,
    rollBeforeDays: Double = 3.0,
    riskFreeRate: Double = 0.0,
    spotHolding: Double = 0.0,
    emitIntervalMs: Long = 1000,
):
  def tenorMs: Long = (tenorDays * 86_400_000L).toLong
  def rollBeforeMs: Long = (rollBeforeDays * 86_400_000L).toLong

object BsGreeksSource:
  /** 与 [[hft.option.BlackScholes.MillisPerYear]] 一致的天数基准，用于 theta 每年->每日换算 */
  val DaysPerYear: Double = 365.0

/** 回测用 BS 合成希腊字母数据源装饰器 (滚动 ATM 跨式)。
  *
  * 监听上游标的 [[EventData.MarketTradeUpdate]] (真实逐笔成交价 S)，维护一个滚动 ATM 长跨式，按虚拟时间
  * 间隔聚合为与 OKX `account/greeks` 同形态的 per-ccy 账户级 [[Greeks]]，紧随该 trade 以**相同
  * exchangeTs** 注入 [[EventData.GreeksUpdate]]；并一次性注入现货 cashBal 使 delta 修正生效。
  *
  * **期权腿 P&L** 由本源按滚动核算 (实盘/回测策略对此无感)：每次滚动实现 (到期跨式价值 − 进场权利金)，
  * 末期叠加当前未实现。供 demo 取 [[optionRealizedPnl]] + [[optionUnrealized]] 得完整期权腿损益。
  * 单位约定同 Greeks 通道 (theta 每日、vega 对 1%)。
  */
final class BsGreeksSource(underlying: MarketDataSource, config: BsGreeksConfig) extends MarketDataSource:
  import BsGreeksSource.DaysPerYear

  private var strike = 0.0
  private var expiry = 0L
  private var entryPremium = 0.0
  private var optionRealized = 0.0
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
            openAt(s, now)
            inited = true
          else if now >= expiry - config.rollBeforeMs then roll(s, now) // 临近到期滚动到新 ATM

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

  /** 以当前价开一份新 ATM 跨式 (记进场权利金) */
  private def openAt(s: Double, now: Timestamp): Unit =
    strike = s
    expiry = now + config.tenorMs
    entryPremium = straddleValue(s, now)

  /** 滚动：实现旧跨式 (当前价值 − 进场权利金) 入已实现，再开新 ATM */
  private def roll(s: Double, now: Timestamp): Unit =
    optionRealized += straddleValue(s, now) - entryPremium
    openAt(s, now)

  /** 当前 (strike/expiry) 跨式在 (s, now) 的理论价值 */
  private def straddleValue(s: Double, now: Timestamp): Double =
    val tY = (expiry - now) / BlackScholes.MillisPerYear
    val call = BlackScholes.greeks(OptionRight.Call, s, strike, tY, config.impliedVol, config.riskFreeRate).price
    val put = BlackScholes.greeks(OptionRight.Put, s, strike, tY, config.impliedVol, config.riskFreeRate).price
    config.straddles * (call + put)

  /** 当前跨式聚合为账户级 Greeks (单位: theta 每日、vega 对 1%，与通道约定一致) */
  private def greeksAt(s: Double, now: Timestamp): Greeks =
    val tY = (expiry - now) / BlackScholes.MillisPerYear
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

  // ==================== 期权腿 P&L (供 demo 在 run 后查询) ====================

  /** 累计已实现期权腿 P&L (历次滚动实现之和) */
  def optionRealizedPnl: Double = optionRealized

  /** 当前持有跨式在 (s, now) 的未实现 P&L (现价值 − 进场权利金) */
  def optionUnrealized(s: Double, now: Timestamp): Double = straddleValue(s, now) - entryPremium

  /** 当前 (最近一次滚动的) ATM 行权价 */
  def currentStrike: Double = strike
