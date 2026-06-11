package hft.messaging

import ox.channels.{Channel, Source}

import java.util.concurrent.CopyOnWriteArrayList

/** 发布-订阅事件总线。
  *
  * 每个订阅者持有一条独立的无界 channel，publish 对无界 channel 不阻塞，
  * 慢消费者不会拖慢发布方 (对应参考实现中的 unbounded mailbox + BestEffort 投递)。
  */
final class EventBus[E]:
  private val subscribers = CopyOnWriteArrayList[Channel[E]]()

  /** 订阅总线，返回事件流。订阅者在自己的虚拟线程中消费 */
  def subscribe(): Source[E] =
    val ch = Channel.unlimited[E]
    subscribers.add(ch)
    ch

  /** 发布事件到所有订阅者 */
  def publish(event: E): Unit =
    subscribers.forEach(ch => ch.send(event))
