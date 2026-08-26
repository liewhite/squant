package strategy.strategies.ivsellhedge.logic

import strategy.utils.option.{OptionHolding, OptionInstrument, OptionMark}

import hft.domain.{Coin, Timestamp}
import hft.option.BlackScholes

/** 一条参与 delta 计算的持仓腿：合约定义 + 带符号张数 + 标记 IV */
final case class PricedLeg(inst: OptionInstrument, contracts: Double, markVol: Double)

/** **期权组合 delta / gamma 的纯计算** —— 由每腿标记 IV 与标的最新价经 Black-Scholes 算出。
  *
  * 为什么自己算而不用交易所推的 `deltaBS`：那个值的刷新节奏由交易所决定 (OKX 账户级 greeks
  * 是秒级快照)，而 delta 主要随**标的价**变化。自己算的话，价格一变就能立刻重算 ——
  * IV 与持仓这类慢变量按较低频率刷新即可，delta 的新鲜度由价格的新鲜度决定。
  *
  * 单位：每腿贡献 = 带符号张数 × ctVal × BS delta，故结果是**币本位**敞口 ([[Coin]])。
  *
  * 已知近似：币本位期权 (以标的币结算、报价也是标的币的比例) 的真实币本位敞口，比裸 BS delta
  * 多一个报价计价单位换算带来的修正项。这里与参考实现 (直接用交易所 `deltaBS`) 取同一口径 ——
  * 两边都是裸 BS delta，故偏差一致、可比。
  */
object PortfolioDelta:
  /** 无风险利率默认取 0：加密期权没有可直接引用的无风险曲线，而对 delta 的影响远小于 IV 的
    * 不确定性。需要时由调用方显式给出，不在这里猜。 */
  val DefaultRate: Double = 0.0

  /** 把"持仓 / 期权链 / 标记 IV"三份数据按 instId 连接的结果。
    *
    * @param legs        配得上的腿 (可定价)
    * @param unpriceable 有持仓却配不上合约定义或标记 IV 的 instId —— **非空即表示这一轮的 delta
    *                    不可信**，调用方应整轮作废而不是按 0 计入那些腿
    */
  final case class Resolved(legs: Vector[PricedLeg], unpriceable: Vector[String]):
    def complete: Boolean = unpriceable.isEmpty

  /** 一次连接同时给出两个结果 —— 它们出自同一次匹配，分两个方法算就是同一份工作做两遍，
    * 而且留下"两次调用之间输入变了"的口子。
    *
    * 内连接而非左连接是有意的：缺链 (合约不在期权链里) 或缺 IV 的腿**算不出 delta**，
    * 悄悄按 0 计入等于把一份真实敞口当成不存在。
    *
    * 零张持仓直接丢掉 —— 它对敞口没有贡献，留着只会让"缺 IV"的告警噪声化。
    */
  def resolve(
      holdings: Seq[OptionHolding],
      chain: Seq[OptionInstrument],
      marks: Seq[OptionMark],
  ): Resolved =
    val bySymbol = chain.iterator.map(i => i.symbol -> i).toMap
    val volBySymbol = marks.iterator.map(m => m.symbol -> m.markVol).toMap
    val held = holdings.iterator.filter(_.contracts != 0.0).toVector
    val legs = held.flatMap(h =>
      for i <- bySymbol.get(h.symbol); v <- volBySymbol.get(h.symbol) yield PricedLeg(i, h.contracts, v)
    )
    val ok = legs.iterator.map(_.inst.symbol).toSet
    Resolved(legs, held.iterator.map(_.symbol).filterNot(ok.contains).toVector.distinct)

  /** 组合的**代表性年化 IV**，按 `|每腿 gamma 贡献|` 加权。
    *
    * 按 gamma 加权而不是按张数：对冲关心的是**敞口扩散得多快**，而那由 gamma 大的腿主导 ——
    * 一条深度价外、gamma 近零的腿，它的 IV 对"敞口下一分钟会漂多少"几乎没有发言权。
    *
    * 权重全为 0 (无持仓 / 全部到期) -> None。
    */
  def weightedMarkVol(legs: Seq[PricedLeg], spot: Double, nowMs: Timestamp, rate: Double = DefaultRate): Option[Double] =
    var wSum = 0.0
    var vSum = 0.0
    legs.foreach { l =>
      val tYears = (l.inst.expiryMs - nowMs).toDouble / BlackScholes.MillisPerYear
      val g = BlackScholes.greeks(l.inst.right, spot, l.inst.strike, tYears, l.markVol, rate)
      val w = math.abs(l.inst.toCoin(l.contracts) * g.gamma)
      wSum += w
      vSum += w * l.markVol
    }
    if wSum > 0.0 then Some(vSum / wSum) else None

  /** 组合的 (delta, gamma)，币本位。已到期/剩余期限 ≤ 0 的腿由 [[BlackScholes]] 退化为内在价值，
    * delta 取 0/±1、gamma 取 0 —— 那正是到期时的真实敞口形态。 */
  def greeks(legs: Seq[PricedLeg], spot: Double, nowMs: Timestamp, rate: Double = DefaultRate): (Coin, Coin) =
    var delta = 0.0
    var gamma = 0.0
    legs.foreach { l =>
      val tYears = (l.inst.expiryMs - nowMs).toDouble / BlackScholes.MillisPerYear
      val g = BlackScholes.greeks(l.inst.right, spot, l.inst.strike, tYears, l.markVol, rate)
      val scale = l.inst.toCoin(l.contracts) // 张 -> 币, 换算的唯一出处
      delta += scale * g.delta
      gamma += scale * g.gamma
    }
    (Coin(delta), Coin(gamma))
