package hft.engine

import hft.actor.{Actor, ActorContext}
import hft.domain.*
import hft.event.Commands.{AccountSync, AccountSyncRequest, AccountSynced, MarketSubscription, MarketSubscriptionRequest}
import hft.event.{AnyEvent, Event, Interest, Subscription}
import hft.strategy.Strategy
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{CountDownLatch, TimeUnit}

/** 一个策略实例的生命周期所有者：独占租约、启动对齐和 Executor 子组件同生共死。 */
private[engine] final class StrategySession(
    strategy: Strategy,
    account: AccountId,
    claims: InstrumentClaims,
    syncTimeoutMs: Long = Engine.SyncTimeoutMs,
) extends Actor:
  require(syncTimeoutMs > 0, "syncTimeoutMs 必须大于 0")
  private val logger = LoggerFactory.getLogger(classOf[StrategySession])
  /** 本会话这一轮对齐的编号。**先于 Executor 生成** —— 闸门要拿它去认领属于自己的应答。 */
  private val requestId = StrategySession.nextRequestId()
  private val executor = Executor(strategy, account, requestId)
  private val subscription: Subscription = executor.subscription
  private val targets = subscription.alignmentTargets(account)
  private val symbolsByExchange = subscription.instruments.groupMap(_.exchange)(_.symbol)
  private val readiness = AlignmentReadiness(targets, requestId)
  private var ctx: ActorContext = scala.compiletime.uninitialized

  override def name: String = s"strategy-session(${strategy.getClass.getSimpleName}@$account)"

  /** 独占租约里指认本会话的唯一身份 (同类同账户可以有多个实例)。 */
  private def claimOwner: String = s"$name#$requestId"

  override def interests: Set[Interest] = Set(Interest.Keyed(AccountSynced, targets))

  override def onPrepare(context: ActorContext): Unit =
    ctx = context
    val keys = subscription.instruments.map(AccountInstrument(account, _))
    // 租约的 owner 要能**唯一指认这个会话实例**: `name` 对同类同账户的两个实例是同一个字符串,
    // 于是冲突信息会写成"已被 X 占用, X 不能重复接管"这种自指的话。
    context.manage(claims.acquire(claimOwner, keys))(_.close())
    context.spawn(executor)

  override def onStart(context: ActorContext): Unit =
    if targets.isEmpty then readiness.completeIfEmpty(activateMarketStreams())
    else
      context.fork {
        if !readiness.await(syncTimeoutMs) then
          readiness.timeout(syncTimeoutMs)(context.reportFailure)
      }
      targets.foreach { target =>
        val symbols = symbolsByExchange.getOrElse(target.exchange, Set.empty)
        context.publish(Event.local(AccountSync, AccountSyncRequest(account, target.exchange, requestId, symbols)))
      }

  override def onEvent(event: AnyEvent, now: Timestamp): Vector[AnyEvent] =
    event.as(AccountSynced).foreach { report =>
      if readiness.acknowledge(report)(activateMarketStreams()) then
        logger.info(s"策略会话就绪: $name targets=${targets.mkString(",")} req=$requestId")
    }
    Vector.empty

  override def onStop(now: Timestamp): Vector[AnyEvent] =
    readiness.fail(IllegalStateException(s"$name 在启动对齐完成前停止"))
    Vector.empty

  /** Engine 只等待领域层的“策略可交易”条件，不再参与其实现步骤。 */
  def awaitReady(): Unit = readiness.awaitReady(name)

  private def activateMarketStreams(): Unit =
    subscription.marketRequests.foreach { (exchange, kinds) =>
      ctx.publish(Event.local(MarketSubscription, MarketSubscriptionRequest(exchange, kinds)))
    }

private[engine] object StrategySession:
  /** 对齐请求号的**唯一序列**。
    *
    * `Executor` 靠 requestId 认领"这是我那一轮对齐的应答" (见 `Executor.alignmentRequestId`),
    * 所以只要有第二个发号的地方, 就必须共用这一个 —— 各自从 0 计数迟早撞上, 而撞上的后果是
    * 某个策略**误以为自己的对齐已完成**并开始交易, 危险侧且没有任何症状。
    * 目前只有本类发号 (只读监控走 `hft.exchange.AccountMonitor`, 不需要对齐)。 */
  private val sequence = AtomicLong(0L)
  private def nextRequestId(): Long = sequence.incrementAndGet()

/** 对齐与超时共用一个线性化点：最后一个 ACK 和超时只能有一个完成会话。 */
private[engine] final class AlignmentReadiness(targets: Set[AccountExchange], requestId: Long):
  private val remaining = scala.collection.mutable.Set.from(targets)
  private val ready = CountDownLatch(1)
  @volatile private var completion: Either[Throwable, Unit] = null

  def acknowledge(report: hft.event.Commands.AccountSyncReport)(activate: => Unit): Boolean = synchronized {
    if completion != null || report.requestId != requestId || !remaining.remove(report.target) then false
    else if remaining.nonEmpty then false
    else complete(activate)
  }

  def completeIfEmpty(activate: => Unit): Boolean = synchronized {
    if completion != null || remaining.nonEmpty then false else complete(activate)
  }

  def timeout(timeoutMs: Long)(reportFailure: Throwable => Unit): Boolean =
    val failure = synchronized {
      if completion != null then None
      else
        val cause = IllegalStateException(
          s"启动对齐超时 (${timeoutMs}ms, req=$requestId): ${remaining.mkString(",")} 中有柜台没有回应。" +
            "对齐未完成就放行会让策略基于空仓位决策"
        )
        completion = Left(cause)
        Some(cause)
    }
    failure match
      case Some(cause) =>
        // 等待者只能在核心已经记录并传播组件失败后醒来，避免 Engine.stop 抢先把 watchdog 当成停机噪声。
        try reportFailure(cause)
        finally ready.countDown()
        true
      case None => false

  def fail(failure: Throwable): Boolean = synchronized {
    if completion != null then false
    else
      completion = Left(failure)
      ready.countDown()
      true
  }

  def await(timeoutMs: Long): Boolean = ready.await(timeoutMs, TimeUnit.MILLISECONDS)

  def awaitReady(owner: String): Unit =
    ready.await()
    completion match
      case Right(()) => ()
      case Left(e)   => throw e
      case null      => throw IllegalStateException(s"$owner 就绪信号缺少结果")

  def pending: Set[AccountExchange] = synchronized(remaining.toSet)

  private def complete(activate: => Unit): Boolean =
    try
      activate
      completion = Right(())
      ready.countDown()
      true
    catch
      case e: Throwable =>
        completion = Left(e)
        ready.countDown()
        throw e
