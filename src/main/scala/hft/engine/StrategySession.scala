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
) extends Actor:
  private val logger = LoggerFactory.getLogger(classOf[StrategySession])
  private val executor = Executor(strategy, account)
  private val subscription: Subscription = executor.subscription
  private val targets = subscription.alignmentTargets(account)
  private val symbolsByExchange = subscription.instruments.groupMap(_.exchange)(_.symbol)
  private val requestId = StrategySession.nextRequestId()
  private val alignment = AlignmentTracker(targets, requestId)
  private val completionLock = new Object
  @volatile private var completion: Either[Throwable, Unit] = null
  private val ready = CountDownLatch(1)
  private var ctx: ActorContext = scala.compiletime.uninitialized

  override def name: String = s"strategy-session(${strategy.getClass.getSimpleName}@$account)"

  override def interests: Set[Interest] = Set(Interest.Keyed(AccountSynced, targets))

  override def onPrepare(context: ActorContext): Unit =
    ctx = context
    val keys = subscription.instruments.map(AccountInstrument(account, _))
    context.manage(claims.acquire(name, keys))(_.close())
    context.spawn(executor)

  override def onStart(context: ActorContext): Unit =
    if targets.isEmpty then becomeReady()
    else
      context.fork {
        if !ready.await(Engine.SyncTimeoutMs, TimeUnit.MILLISECONDS) then
          val failure = alignmentTimeout()
          failReadiness(failure)
          throw failure
      }
      targets.foreach { target =>
        val symbols = symbolsByExchange.getOrElse(target.exchange, Set.empty)
        context.publish(Event.local(AccountSync, AccountSyncRequest(account, target.exchange, requestId, symbols)))
      }

  override def onEvent(event: AnyEvent, now: Timestamp): Vector[AnyEvent] =
    event.as(AccountSynced).foreach { report =>
      if alignment.acknowledge(report) then becomeReady()
    }
    Vector.empty

  override def onStop(now: Timestamp): Vector[AnyEvent] =
    failReadiness(IllegalStateException(s"$name 在启动对齐完成前停止"))
    Vector.empty

  /** Engine 只等待领域层的“策略可交易”条件，不再参与其实现步骤。 */
  def awaitReady(): Unit =
    ready.await()
    completion match
      case Right(()) => ()
      case Left(e)   => throw e
      case null      => throw IllegalStateException(s"$name 就绪信号缺少结果")

  private def becomeReady(): Unit = completionLock.synchronized {
    if completion == null then
      try
        subscription.marketStreams.groupMap(_._1)(_._2).foreach { (exchange, kinds) =>
          ctx.publish(Event.local(MarketSubscription, MarketSubscriptionRequest(exchange, kinds.toSet)))
        }
        completion = Right(())
        ready.countDown()
        logger.info(s"策略会话就绪: $name targets=${targets.mkString(",")} req=$requestId")
      catch
        case e: Throwable =>
          completion = Left(e)
          ready.countDown()
          throw e
  }

  private def failReadiness(failure: Throwable): Unit = completionLock.synchronized {
    if completion == null then
      completion = Left(failure)
      ready.countDown()
  }

  private def alignmentTimeout(): IllegalStateException =
    IllegalStateException(
      s"启动对齐超时 (${Engine.SyncTimeoutMs}ms, req=$requestId): ${alignment.pending.mkString(",")} 中有柜台没有回应。" +
        "对齐未完成就放行会让策略基于空仓位决策"
    )

private[engine] object StrategySession:
  private val sequence = AtomicLong(0L)
  private def nextRequestId(): Long = sequence.incrementAndGet()

/** 对齐应答的纯状态机：重复、过期和未知 target 都不能推动完成。 */
private[engine] final class AlignmentTracker(targets: Set[AccountExchange], requestId: Long):
  private val remaining = scala.collection.mutable.Set.from(targets)

  def acknowledge(report: hft.event.Commands.AccountSyncReport): Boolean =
    report.requestId == requestId && remaining.remove(report.target) && remaining.isEmpty

  def pending: Set[AccountExchange] = remaining.toSet
