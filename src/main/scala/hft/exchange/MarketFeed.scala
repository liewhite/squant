package hft.exchange

import hft.actor.{Actor, ActorContext}
import hft.domain.{Exchange, SubscriptionKind, Timestamp}
import hft.event.Commands.MarketSubscription
import hft.event.{AnyEvent, Interest}
import org.slf4j.LoggerFactory

/** 行情插件：接**行情订阅指令**，把对应的公共行情发布到总线。
  *
  * 装到总线上它就开始工作 —— 谁请求的、有几个策略在等，它一概不需要知道。
  * 引擎因此不再持有"交易所 -> 行情流"的表，接入任意数据源不用改它一行。
  *
  * ## 数据源不必是交易所
  *
  * 本类只要求两件事：接自己那个交易所键上的订阅指令、产出行情事件。历史回放、
  * 第三方数据商、另一个进程的桥接都可以是它的实现。
  *
  * ## 一个交易所可以有多个行情插件
  *
  * 它们订阅同一个指令键 (总线按键投递给全部订阅者)，各自认领自己认得的流类型、
  * 忽略其余 —— 例如一个接盘口与成交，另一个接期权希腊值。
  *
  * ## Fail-fast
  *
  * 连接断开、解析失败、不理解的消息一律抛异常终止引擎作用域，由进程重启后的启动对齐
  * 保证状态正确。行情流虽是公共数据，但"重连期间静默缺一段"同样会让基于窗口的指标
  * 得出错误结论，而没有任何症状。
  */
abstract class MarketFeed extends Actor:
  private val feedLogger = LoggerFactory.getLogger(classOf[MarketFeed])

  /** 本插件服务的交易所 —— 行情订阅指令的路由键 */
  def exchange: Exchange

  override def name: String = s"market-feed@$exchange"

  /** 已经向交易所下发过的流。
    *
    * 只由 actor 事件循环这一个线程读写 (订阅指令经邮箱串行到达)，无需同步。
    */
  private var subscribed: Set[SubscriptionKind] = Set.empty

  /** 由框架在 [[onStart]] 注入，之后只读 */
  @volatile private var ctx: ActorContext = scala.compiletime.uninitialized

  final override def interests: Set[Interest] = Set(Interest.Keyed(MarketSubscription, Set(exchange)))

  final override def onStart(context: ActorContext): Unit =
    ctx = context
    connect()

  /** 订阅指令是**增量、幂等**的：只把没订过的流下发给交易所。
    *
    * 幂等不是优化。策略是动态添加的，后来者声明的流集合与先前的必然重叠，
    * 重复下发轻则浪费一次往返，重则被交易所判为异常订阅行为。
    */
  final override def onEvent(event: AnyEvent, now: Timestamp): Vector[AnyEvent] =
    event.as(MarketSubscription).foreach { request =>
      val fresh = request.kinds -- subscribed
      if fresh.nonEmpty then
        subscribed ++= fresh
        feedLogger.info(s"$exchange 新增行情订阅 ${fresh.size} 条: ${fresh.map(_.subscribedSymbol).mkString(",")}")
        subscribeToExchange(fresh)
    }
    Vector.empty

  // ==================== 子类实现 ====================

  /** 建立连接、fork 常驻收流线程。此时 [[publish]] / [[fork]] 已可用。
    *
    * 这里 fork 的线程**不受停机控制**，随进程结束 —— 一个阻塞在 socket 读上的线程没有
    * 办法被协作式地叫停，而假装能停会让停机链在那里静默地等下去。
    */
  protected def connect(): Unit

  /** 向交易所下发订阅。只会收到**尚未订阅过**的流，实现方不必再去重。 */
  protected def subscribeToExchange(kinds: Set[SubscriptionKind]): Unit

  // ==================== 子类可用的能力 ====================

  /** 把一条行情发布到总线 (从收流线程调用) */
  protected final def publish(event: AnyEvent): Unit = ctx.publish(event)

  /** 在本插件的作用域内 fork 一条线程 */
  protected final def fork(body: => Unit): Unit = ctx.fork(body)
