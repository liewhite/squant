package hft.exchange

import hft.actor.{Actor, ActorContext}
import hft.domain.{Exchange, InstrumentKind, SubscriptionKind, Timestamp}
import hft.event.Commands.MarketSubscription
import hft.event.{AnyEvent, CommandHandler}
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

  final override def commandHandlers: Set[CommandHandler] = Set(CommandHandler.command(MarketSubscription, exchange))

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
        // 品种守卫收在基类, 不是让每个子类各写一句: 四个行情源里有三个写了同一句话,
        // 而 OKX 那个漏了 —— 订一个期权盘口会订阅成功、然后在第一条推送上抛
        // "Unknown OKX instId", 错误指向报文而不是"这个品种没接"。
        fresh.foreach(requireSupported)
        subscribed ++= fresh
        feedLogger.info(s"$exchange 新增行情订阅 ${fresh.size} 条: ${fresh.map(_.subscribedInstrument).mkString(",")}")
        subscribeToExchange(fresh)
    }
    Vector.empty

  // ==================== 子类实现 ====================

  /** 建立连接、fork 常驻收流线程。此时 [[publish]] / [[fork]] 已可用。
    *
    * 这里 fork 的线程归属行情组件：停止时会收到中断并等待退出。连接本身若需要显式关闭，
    * 实现应通过组件作用域登记释放动作，以确保阻塞读能够被唤醒。
    */
  protected def connect(): Unit

  /** 向交易所下发订阅。只会收到**尚未订阅过**的流，实现方不必再去重，
    * 也不必再判品种 —— 都是本适配层接了的（见 [[supportedKinds]]）。 */
  protected def subscribeToExchange(kinds: Set[SubscriptionKind]): Unit

  /** 本行情源接了哪些品种。
    *
    * 与 [[ExchangeClient.supportedKinds]] 是同一件事的**行情侧**，但两者分开声明而不是
    * 共用一个：它们是不同的接入面，可以不一致。Bybit 的期权 REST 端点与永续形状相同、
    * 而公共行情走的是另一条 WS 地址；一个所完全可能先通了行情、还没通下单。
    *
    * 订不了的品种在**下发订阅时**失败，而不是等第一条推送 —— 交易所对不认识的流名
    * 未必给回执，"订了个空"是没有症状的。
    */
  protected def supportedKinds: Set[InstrumentKind]

  private def requireSupported(kind: SubscriptionKind): Unit =
    val instrument = kind.subscribedInstrument
    require(
      supportedKinds.contains(instrument.kind),
      s"$exchange 行情源未接入 ${instrument.kind} (已接: ${supportedKinds.mkString("/")}): $instrument",
    )

  // ==================== 子类可用的能力 ====================

  /** 把一条行情发布到总线 (从收流线程调用) */
  protected final def publish(event: AnyEvent): Unit = ctx.publish(event)

  /** 在本插件的作用域内 fork 一条线程 */
  protected final def fork(body: => Unit): Unit = ctx.fork(body)

  /** 协作式睡眠：停机请求会立即唤醒并返回 true。心跳一类的常驻循环用它，不要用裸 `Thread.sleep`
    * —— 后者要靠中断打断，停机时便多一次"能不能按时退出"的不确定 */
  protected final def sleepUnlessStopped(ms: Long): Boolean = ctx.sleepUnlessStopped(ms)

  /** 把连接等外部资源登记到本插件作用域 */
  protected final def manage[A](resource: A)(release: A => Unit): A = ctx.manage(resource)(release)
