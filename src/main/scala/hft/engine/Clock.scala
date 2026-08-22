package hft.engine

import hft.actor.{Actor, ActorContext}
import hft.event.{Event, Topics}

/** 时钟：周期性发布 [[Topics.Clock]]，驱动订单超时检测等定时任务。
  *
  * 用 [[ActorContext.sleepUnlessStopped]] 而非裸 `Thread.sleep`，于是它也能被协作式地叫停
  * —— 一个停不下来的时钟会让停机链在最后一步空转。
  */
final class Clock(intervalMs: Long) extends Actor:
  override def name: String = "clock"

  override def onStart(ctx: ActorContext): Unit =
    ctx.fork {
      while !ctx.sleepUnlessStopped(intervalMs) do ctx.publish(Event.local(Topics.Clock, ()))
    }
