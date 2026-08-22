package hft.event

import hft.domain.{Exchange, Instrument}

/** 一个订阅者的完整订阅范围：若干 [[Interest]] 的聚合，以及"这条事件归不归它"的判据。
  *
  * 同一份声明有三个下游，这正是它必须是**数据**的原因 (见 [[Interest]])：
  *   - [[EventBus]] 据此建投递索引；
  *   - 回测在单线程循环里直接用 [[accepts]] 过滤 (没有总线，但判据必须与实盘同一份)；
  *   - 引擎据 [[instruments]] 汇总出要向交易所订阅的行情流。
  */
final case class Subscription(interests: Set[Interest]):
  /** 这条事件是否落在本范围内 */
  def accepts(event: AnyEvent): Boolean = interests.exists(_.accepts(event))

  /** 本订阅者**交易**的标的。
    *
    * 只从框架内置的按标的路由 topic 派生 (行情 + 私有回报)，而不是"声明里出现过的任何
    * Instrument"。两者是不同的事实：用户自定义的按标的路由 topic (例如订阅另一个策略在某
    * 标的上的指标) 表示**关注**，不表示**交易**。
    *
    * 混为一谈的后果是补过头 —— 一个只看指标的监控/元策略会被补上该标的的私有回报订阅，
    * 进而被引擎拉去做持仓对齐、要求 SymbolMeta，而它根本不交易那个标的。
    */
  def instruments: Set[Instrument] =
    keysOf(Topics.market) ++ keysOf(Topics.instrumentPrivate).map(_.instrument)

  /** 涉及的全部交易所：交易标的所属的，加上账户级声明直接指名的 */
  def exchanges: Set[Exchange] =
    instruments.map(_.exchange) ++ keysOf(Topics.account).map(_.exchange)

  private def keysOf[K](topics: Set[Topic[K, ?]]): Set[K] =
    topics.flatMap(t => interests.flatMap(_.keysOf(t)))

object Subscription:
  val empty: Subscription = Subscription(Set.empty)
