package strategy.strategies.ivsellhedge.logic

import hft.domain.*
import hft.event.Topic

/** **账户在某币上的期权敞口读数** —— 由 [[strategy.strategies.ivsellhedge.live.OptionSellerActor]]
  * 按秒发布，对冲策略据此决策。
  *
  * 单位一律币本位 ([[Coin]])：每腿贡献 = 带符号张数 × ctVal × BS delta。
  *
  * ## 为什么现货余额也在里面
  *
  * 币本位期权 (OKX `ETH-USD-*`) 以 ETH 作保证金、权利金以 ETH 收取，账户里这部分 ETH 是
  * **真实的裸多头**。只对冲期权 delta 的话，"delta 中性"名不副实 —— 账户实际净多一个保证金
  * 的量。取的是**现金**余额而非币本位权益：期权未实现盈亏随价格的变化率已经由 delta 描述了，
  * 把它再当成一笔静态余额对冲就是同一份敞口算两遍。
  *
  * 永续持仓**不在**这里：那是对冲腿自己的仓位，由框架的 `StateManager` 持有 (私有流推送的
  * 事实)。让发布方也报一份就有两个数据源说同一件事，而它们更新节奏还不同。
  *
  * @param optionDelta 期权组合 delta (币本位)
  * @param optionGamma 期权组合 gamma
  * @param coinBalance 该币现金余额 (裸多头)
  * @param spot        本次计算用的标的价 (诊断用：delta 是它的函数)
  * @param legs        参与计算的持仓腿数 (诊断用：突然变 0 说明持仓拉取或 IV 匹配出了问题)
  */
final case class OptionExposure(
    exchange: Exchange,
    ccy: String,
    optionDelta: Coin,
    optionGamma: Coin,
    coinBalance: Coin,
    spot: Price,
    legs: Int,
    timestamp: Timestamp,
):
  /** 账户在该币上的**总敞口** = 期权 delta + 现货余额。
    *
    * "账户敞口为 0"只该有一个定义，所以这里不留开关 —— 加开关就是两种目标语义。
    */
  def delta: Coin = optionDelta + coinBalance

/** [[OptionExposure]] 的事件族。
  *
  * **按交易所路由，币种在载荷里** —— 与框架内置的 `Topics.Greeks` 同一形状，消费侧自行判 ccy。
  *
  * 为什么不复用 `Topics.Greeks`：那个 topic 在 OKX 上已经有一个发布者 (`OkxAccountStream` 轮询
  * `/account/greeks`)。同一个 topic 两个发布者、两套算法、两个节奏，就是"最后写的赢"，而赢者
  * 随时序变化 —— 对冲会在两个不同的 delta 定义之间跳。各用自己的 topic，两条链互不干扰，
  * 装配时选订哪一条即可，也不必去动一个别的启动器还在依赖的框架组件。
  *
  * 为什么 key 不带账户：期权腿完全旁路框架的下单通道 (OKX 适配层只认永续 instId)，因此
  * **没有影子盘对应物** —— 这条读数天生只有实盘一份。key 里放账户会让对冲策略必须知道自己
  * 跑在哪个账户上，那恰恰是框架刻意不让策略知道的事。
  */
object OptionExposureTopic extends Topic[Exchange, OptionExposure]("optionExposure"):
  def keyOf(p: OptionExposure): Exchange = p.exchange
