package hft.actor

import org.slf4j.Logger
import ox.OxUnsupervised

import java.util.concurrent.{CountDownLatch, TimeUnit}

/** 无界邮箱的统一健康监督；只读取快照，不参与装配或停止决策。
  *
  * 一条 [[ManagedTask]] —— 与框架其余线程同属 [[ActorSystem]] 的 ox 作用域，`allStopped`
  * 落下即自行退出。 */
private[actor] object MailboxMonitor:
  def start(handles: () => Vector[ActorHandle], stopped: CountDownLatch, logger: Logger)(using
      OxUnsupervised
  ): ManagedTask =
    ManagedTask.start("actor-system-health") {
      while !stopped.await(ActorSystem.HealthCheckIntervalMs, TimeUnit.MILLISECONDS) do
        val now = System.nanoTime()
        handles().foreach { handle =>
          val health = handle.mailboxHealth
          val unhealthy =
            health.queued >= ActorSystem.MailboxWarnDepth ||
              (health.queued > 0 && health.oldestEventAgeMs >= ActorSystem.MailboxWarnOldestMs) ||
              health.inFlightAgeMs >= ActorSystem.MailboxWarnOldestMs
          val last = handle.lastMailboxWarningNanos.get
          if unhealthy && now - last >= ActorSystem.HealthWarningIntervalNanos &&
              handle.lastMailboxWarningNanos.compareAndSet(last, now)
          then
            logger.warn(
              s"组件 ${handle.name} 邮箱滞后: queued=${health.queued} highWater=${health.highWaterMark} " +
                s"oldestMs=${health.oldestEventAgeMs} inFlightMs=${health.inFlightAgeMs} " +
                s"lastProcessNs=${health.lastProcessingNanos} " +
                s"maxProcessNs=${health.maxProcessingNanos} state=${handle.state}"
            )
        }
    }
