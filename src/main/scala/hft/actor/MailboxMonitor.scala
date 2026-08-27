package hft.actor

import org.slf4j.Logger

import java.util.concurrent.{CountDownLatch, TimeUnit}

/** 无界邮箱的统一健康监督；只读取快照，不参与装配或停止决策。 */
private[actor] object MailboxMonitor:
  def start(handles: () => Vector[ActorHandle], stopped: CountDownLatch, logger: Logger): Thread =
    Thread
      .ofVirtual()
      .name("actor-system-health")
      .start { () =>
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
                handle.lastMailboxWarningNanos.compareAndSet(last, now) then
              logger.warn(
                s"组件 ${handle.name} 邮箱滞后: queued=${health.queued} highWater=${health.highWaterMark} " +
                  s"oldestMs=${health.oldestEventAgeMs} inFlightMs=${health.inFlightAgeMs} " +
                  s"lastProcessNs=${health.lastProcessingNanos} " +
                  s"maxProcessNs=${health.maxProcessingNanos} state=${handle.state}"
              )
          }
      }
