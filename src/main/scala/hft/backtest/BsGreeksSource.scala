package hft.backtest

import hft.domain.*
import hft.event.{AnyEvent, Event, Topics}
import hft.option.{BlackScholes, Straddle}

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
    /** 宽跨 (strangle) 价外宽度：0=ATM 跨式 (call/put 同行权=首价)；>0=宽跨, call 行权=首价·(1+w)、put 行权=首价·(1−w)。 */
    strangleWidthPct: Double = 0.0,
)

/** 回测用 BS 合成希腊字母数据源装饰器 (单只 ATM 跨式，持有到期，不滚动)。
  *
  * 监听上游标的 [[Topics.Trade]] (真实逐笔成交价 S)，首笔成交开一份 ATM 长跨式，
  * 按虚拟时间间隔聚合为与 OKX `account/greeks` 同形态的 per-ccy 账户级 [[Greeks]]，紧随该 trade 以
  * **相同 exchangeTs** 注入 [[Topics.Greeks]]；并一次性注入现货 cashBal 使 delta 修正生效。
  *
  * **期权腿 P&L** = 当前跨式价值 − 进场权利金 (单只持仓，[[optionPnl]] 供 demo 取完整期权腿损益)。
  * 单位约定同 Greeks 通道 (theta 每日、vega 对 1%)。
  */
final class BsGreeksSource(underlying: MarketDataSource, account: AccountId, config: BsGreeksConfig) extends MarketDataSource:
  import BlackScholes.DaysPerYear // theta 每年->每日 / tenor 钳制的天数基准 (框架 SSOT)

  /** 建仓后才存在的仓位事实：三个行权价与进场权利金。
    *
    * 从前它们是四个初值 0 的 var 加一个 `inited` 布尔，而 [[optionPnl]]/[[enteredPremium]]/
    * [[strikePrice]] 是公开的、不看 `inited`。于是一段没有任何成交的数据窗 (symbol 拼错、
    * 日期区间空) 会让 run 后的查询读到 strike=0、premium=0，报表里落下一行整洁的 0 而不是报错。
    * 收成一个 Option 之后，"还没建仓"在类型上就无法被当成"仓位值为 0"。 */
  private final case class OpenPosition(strike: Double, callStrike: Double, putStrike: Double, entryPremium: Double)

  private var position: Option[OpenPosition] = None

  private def opened: OpenPosition = position.getOrElse(
    sys.error(
      s"BsGreeksSource 尚未建仓: 上游 ${config.underlyingSymbol} 一笔成交都没有 —— " +
        "检查回测区间与数据源, 不要把空窗口当成零盈亏"
    )
  )

  override def events(): Iterator[AnyEvent] =
    var lastEmit: Timestamp = Long.MinValue
    var balanceEmitted = false

    underlying.events().flatMap { ev =>
      ev.as(Topics.Trade).filter(_.symbol == config.underlyingSymbol) match
        case Some(t) =>
          val now = ev.exchangeTs
          val s = t.price
          if position.isEmpty then
            // ATM = 首笔成交价；宽跨则按 strangleWidthPct 向两侧展开
            val callStrike = s.value * (1.0 + config.strangleWidthPct)
            val putStrike = s.value * (1.0 - config.strangleWidthPct)
            val premium = straddleValueAt(s.value, callStrike, putStrike, now)
            position = Some(OpenPosition(s.value, callStrike, putStrike, premium))

          if shouldEmit(now, lastEmit) then
            lastEmit = now
            val greeksEv = Event.stamped(Topics.Greeks, greeksAt(s.value, now), ev.exchangeTs, ev.localTs)
            if balanceEmitted then Iterator(ev, greeksEv)
            else
              balanceEmitted = true
              val balanceEv = Event.stamped(
                Topics.Balance,
                Balance(account, config.exchange, config.ccy, config.spotHolding, now),
                ev.exchangeTs,
                ev.localTs,
              )
              Iterator(ev, balanceEv, greeksEv)
          else Iterator.single(ev)
        case None => Iterator.single(ev)
    }

  private def shouldEmit(now: Timestamp, lastEmit: Timestamp): Boolean =
    lastEmit == Long.MinValue || now - lastEmit >= config.emitIntervalMs

  /** 剩余年限，带下限钳制 (避免到期日 gamma/theta 奇点) */
  private def tYears(now: Timestamp): Double =
    math.max((config.expiry - now) / BlackScholes.MillisPerYear, config.minTenorDays / DaysPerYear)

  /** 期权结构 (跨式/宽跨) 在给定行权价与 (s, now) 的理论价值 (框架级 [[Straddle]] 复用) */
  private def straddleValueAt(s: Double, callStrike: Double, putStrike: Double, now: Timestamp): Double =
    Straddle.value(config.straddles, s, callStrike, putStrike, tYears(now), config.impliedVol, config.riskFreeRate)

  /** 期权结构聚合为账户级 Greeks (单位: theta 每日、vega 对 1%，与通道约定一致) */
  private def greeksAt(s: Double, now: Timestamp): Greeks =
    val p = opened
    val g = Straddle.greeks(config.straddles, s, p.callStrike, p.putStrike, tYears(now), config.impliedVol, config.riskFreeRate)
    Greeks(
      account = account,
      exchange = config.exchange,
      ccy = config.ccy,
      delta = g.delta,
      gamma = g.gamma,
      theta = g.theta / DaysPerYear, // 每年 -> 每日
      vega = g.vega / 100.0,         // 对 1.0 -> 对 1%
      timestamp = now,
    )

  /** 期权腿 P&L = 当前跨式价值 − 进场权利金 (单只持仓，供回测在 run 后查询)。
    * 未建仓即抛 —— 见 [[OpenPosition]] 的说明。 */
  def optionPnl(s: Double, now: Timestamp): Double =
    val p = opened
    straddleValueAt(s, p.callStrike, p.putStrike, now) - p.entryPremium

  /** 进场权利金 (首笔成交时按期初 ATM/IV/tenor 定价的跨式价值)，作为占比基准的单一数据源——
    * 与 [[optionPnl]] 同源，避免消费侧重算定价口径不一致 (run 后可查)。 */
  def enteredPremium: Double = opened.entryPremium

  /** ATM 行权价 (首笔成交价) */
  def strikePrice: Double = opened.strike
