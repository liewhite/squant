package hft.event

import ox.channels.{Channel, Source}

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicLong
import java.util.{Collections, IdentityHashMap}
import scala.jdk.CollectionConverters.*

/** 按 (topic, key) 建索引的发布订阅总线。
  *
  * ## 投递是查表，不是遍历
  *
  * 一条事件先按 topic 查到该族的索引，再按 key 查到订阅者 —— 两次哈希，与订阅者总数无关。
  * "广播给所有人、各自过滤"的写法在多策略部署下每条 BBO 都要白发若干份，而行情是热路径。
  *
  * ## 邮箱无界
  *
  * 每个订阅者一条 [[Channel.unlimited]]，发布方不因慢消费者阻塞。**不要换成有界邮箱**：
  * 有界 + 尽力投递会在队列满时静默丢事件，而丢掉一条成交回报的后果是本地仓位与交易所
  * 长期不一致，且没有任何外在症状。真正的背压问题应当在消费者侧暴露 (滞后告警)，
  * 而不是靠丢事件掩盖。
  *
  * ## 并发
  *
  * 发布远多于订阅，故索引用 [[ConcurrentHashMap]] + [[CopyOnWriteArrayList]]。普通事件的热路径
  * 无锁；命令发布是低频控制路径，会与处理者注册串行化，确保“检查基数”和“交付命令”看到
  * 同一个处理者集合。
  */
final class EventBus:
  /** 一个 topic 的投递索引：按 key 定向的订阅者 + 该 topic 的全量订阅者 */
  private final class TopicIndex:
    val byKey: ConcurrentHashMap[Any, CopyOnWriteArrayList[EventBus.Mailbox]] = ConcurrentHashMap()
    val handlersByKey: ConcurrentHashMap[Any, CopyOnWriteArrayList[EventBus.Mailbox]] = ConcurrentHashMap()
    val all: CopyOnWriteArrayList[EventBus.Mailbox] = CopyOnWriteArrayList()

  private val topics: ConcurrentHashMap[Topic[?, ?], TopicIndex] = ConcurrentHashMap()

  /** 无差别收下一切的订阅者，见 [[subscribeAll]]。通常为空，热路径上只是一次长度检查 */
  private val everything: CopyOnWriteArrayList[EventBus.Mailbox] = CopyOnWriteArrayList()

  /** 订阅/退订的互斥锁。
    *
    * 普通事件的 [[publish]] 不参与；命令发布参与，以消除基数检查与退订之间的竞态。
    *
    * 没有它会丢注册：B 的 subscribe 取到某个 key 槽的列表、尚未 add，A 的退订恰好把这个
    * 已空的槽从索引里摘掉，B 随后 add 进一个孤儿列表 —— B 永远收不到该 key 的事件，
    * 且没有任何症状。"停旧策略、起新策略于同一标的"正是这个序列。
    */
  private val registrationLock = new Object

  private def indexOf(topic: Topic[?, ?]): TopicIndex =
    topics.computeIfAbsent(topic, _ => TopicIndex())

  /** 按订阅声明订阅，返回该订阅者的专属事件流。
    *
    * 登记前先按 topic 归并：[[Interest.All]] 吞掉同 topic 的 [[Interest.Keyed]] (前者是后者的
    * 超集)，同 topic 的多条 Keyed 取 key 并集。
    *
    * **归并不是优化，是正确性**。同一条 channel 若被登记进同一个 (topic, key) 槽两次，
    * publish 就会投它两次，而一条 Fill 被消费两次就是本地仓位翻倍。声明重叠很容易发生：
    * 策略自己声明了某标的的私有回报，框架又按 [[hft.engine.StrategyRunner.subscriptionFor]]
    * 补了一条覆盖全部标的的 —— 两个 Interest 不相等 (Set 去重不了) 但 key 相交。
    * 更隐蔽的是它会让实盘与回测分叉：回测走 [[Subscription.accepts]]，那是布尔判定、天然幂等。
    */
  def subscribe(interests: Set[Interest]): EventBus.Mailbox = subscribeObservations(interests)

  /** 测试与框架内部使用：建立观察邮箱后立即激活命令处理。 */
  private[hft] def subscribe(
      interests: Set[Interest],
      commandHandlers: Set[CommandHandler],
  ): EventBus.Mailbox =
    val mailbox = subscribeObservations(interests)
    try
      activateHandlers(Vector(mailbox -> commandHandlers))
      mailbox
    catch
      case e: Throwable =>
        mailbox.close()
        mailbox.done()
        throw e

  private[hft] def subscribeObservations(interests: Set[Interest]): EventBus.Mailbox = registrationLock.synchronized {
    val ch = Channel.unlimited[AnyEvent]
    val allTopics: Set[Topic[?, ?]] = interests.collect { case Interest.All(t) => t }
    val keyedByTopic: Map[Topic[?, ?], Set[Any]] =
      interests.foldLeft(Map.empty[Topic[?, ?], Set[Any]]) {
        case (acc, Interest.Keyed(t, keys)) if !allTopics.contains(t) =>
          acc.updated(t, acc.getOrElse(t, Set.empty) ++ keys.toSet[Any])
        case (acc, _) => acc
      }
    lazy val mailbox: EventBus.Mailbox = EventBus.Mailbox(
      ch,
      () => registrationLock.synchronized(remove(mailbox, allTopics, keyedByTopic, mailbox.handledByTopic)),
    )
    allTopics.foreach(t => indexOf(t).all.add(mailbox))
    keyedByTopic.foreach { (t, keys) =>
      val idx = indexOf(t)
      keys.foreach(k => idx.byKey.computeIfAbsent(k, _ => CopyOnWriteArrayList()).add(mailbox))
    }
    mailbox
  }

  /** 一批组件的 `onStart` 全部成功后，原子激活其命令处理能力。
    *
    * 启动钩子运行期间邮箱已经能接观察事件，但命令处理者尚不可见；因此外部发布者不会把一条
    * 命令成功交给随后回滚的组件。整批先校验再登记，也不会暴露半激活状态。
    */
  private[hft] def activateHandlers(bindings: Seq[(EventBus.Mailbox, Set[CommandHandler])]): Unit =
    registrationLock.synchronized {
      val requested = bindings.flatMap { (mailbox, handlers) =>
        if mailbox.handledByTopic.nonEmpty then
          throw IllegalStateException("同一个邮箱不能重复激活命令处理能力")
        handlers.map(handler => ((handler.topic, handler.key), mailbox))
      }
      requested.groupMap(_._1)(_._2).foreach { case ((topic, key), mailboxes) =>
        val added = mailboxes.distinct.size
        val total = Option(topics.get(topic))
          .flatMap(idx => Option(idx.handlersByKey.get(key)))
          .fold(added)(_.size + added)
        if !topic.cardinality.accepts(total) then
          throw IllegalStateException(
            s"命令 $topic@$key 要求${topic.cardinality.explain}处理者, 激活后将有 $total 个"
          )
      }
      bindings.foreach { (mailbox, handlers) =>
        val handledByTopic = handlers.groupMap(_.topic)(_.key)
        handledByTopic.foreach { (topic, keys) =>
          val idx = indexOf(topic)
          keys.foreach(key => idx.handlersByKey.computeIfAbsent(key, _ => CopyOnWriteArrayList()).add(mailbox))
        }
        mailbox.handledByTopic = handledByTopic
      }
    }

  /** 无差别订阅本总线上的**一切** —— 给**中继**用，不给业务组件用。
    *
    * 业务组件一律声明 [[Interest]]：那样投递才是两次哈希 (而不是每条事件都发给你再自己筛)，
    * 引擎也才自省得出该向交易所订哪些流。全收会把这两样一起丢掉。
    *
    * 存在的理由只有一个：**中继不知道、也不该知道会有哪些 topic 流过**。虚拟柜台把上游
    * 行情源装在自己的私有总线上，再延迟转发到主总线 —— 若它按内置行情 topic 枚举着收，
    * 用户自定义的行情源就会在那里静默消失 (契约校验通过、订阅指令也转下去了，
    * 数据却没人接着往外送)，而这正是本架构最忌讳的失效形态。
    *
    * 中继务必跳过 [[CommandTopic]]：指令是从主总线**流进来**的，
    * 原样转回去会让它在两条总线之间无限弹跳。
    */
  def subscribeAll(): EventBus.Mailbox = registrationLock.synchronized {
    val ch = Channel.unlimited[AnyEvent]
    lazy val mailbox: EventBus.Mailbox =
      EventBus.Mailbox(ch, () => registrationLock.synchronized(everything.remove(mailbox): Unit))
    everything.add(mailbox)
    mailbox
  }

  /** 某个 (topic, key) 槽上的订阅者数 —— 供观测与测试确认退订确实摘干净了。
    *
    * key 的类型由 topic 给出 (而不是 `Any`)：查一个类型对不上的 key 永远得 0，
    * 那正是"订阅者数为零"这类校验最不该出现的静默假象。
    */
  def subscriberCount[K](topic: Topic[K, ?], key: K): Int =
    Option(topics.get(topic)).fold(0) { idx =>
      Option(idx.byKey.get(key)).fold(0)(_.size) + idx.all.size
    }

  /** 显式声明会处理这个命令键的组件数；普通订阅者不计入。 */
  def handlerCount[K](topic: CommandTopic[K, ?], key: K): Int =
    Option(topics.get(topic)).fold(0)(idx => Option(idx.handlersByKey.get(key)).fold(0)(_.size))

  /** 把一条 channel 从它登记过的每个槽里摘除。
    *
    * 只摘自己登记过的位置 (而不是遍历全索引)，因此代价与本订阅者的声明规模成正比，
    * 与总订阅者数无关。空掉的 key 槽一并删除，否则长期起停会攒下一堆空列表。
    */
  private def remove(
      mailbox: EventBus.Mailbox,
      allTopics: Set[Topic[?, ?]],
      keyedByTopic: Map[Topic[?, ?], Set[Any]],
      handledByTopic: Map[CommandTopic[?, ?], Set[Any]],
  ): Unit =
    allTopics.foreach(t => Option(topics.get(t)).foreach(_.all.remove(mailbox)))
    keyedByTopic.foreach { (t, keys) =>
      Option(topics.get(t)).foreach { idx =>
        keys.foreach { k =>
          Option(idx.byKey.get(k)).foreach { subscribers =>
            subscribers.remove(mailbox)
            if subscribers.isEmpty then idx.byKey.remove(k, subscribers)
          }
        }
      }
    }
    handledByTopic.foreach { (topic, keys) =>
      Option(topics.get(topic)).foreach { idx =>
        keys.foreach { key =>
          Option(idx.handlersByKey.get(key)).foreach { handlers =>
            handlers.remove(mailbox)
            if handlers.isEmpty then idx.handlersByKey.remove(key, handlers)
          }
        }
      }
    }

  /** 发布一条事件给关心它的订阅者。无人订阅该 topic 时是一次哈希查找后返回。
    *
    * 用 `sendOrClosed` 而非 `send`：订阅者正在停机时，退订与本次投递可能重叠 (投递已拿到
    * 订阅者快照、对方随即关闭邮箱)。向一个正在退出的订阅者投递失败是正常的停机竞态，
    * 不该把发布方 (另一个 actor 的事件循环) 炸掉、进而级联终止整个引擎。
    */
  def publish(event: AnyEvent): Unit =
    event.topic match
      case command: CommandTopic[?, ?] =>
        // 命令的基数检查与向这批确定接收者入队在同一个注册锁临界区内，避免检查后退订。
        registrationLock.synchronized {
          val idx = topics.get(command)
          val handlers =
            Option(idx)
              .flatMap(i => Option(i.handlersByKey.get(event.key)))
              .fold(Vector.empty[EventBus.Mailbox])(_.asScala.toVector)
          if !command.cardinality.accepts(handlers.size) then
            throw IllegalStateException(
              s"命令 $command@${event.key} 要求${command.cardinality.explain}处理者, 实际 ${handlers.size} 个"
            )
          val recipientMap = IdentityHashMap[EventBus.Mailbox, java.lang.Boolean]()
          val recipients = Collections.newSetFromMap(recipientMap)
          everything.forEach(ch => recipients.add(ch): Unit)
          if idx != null then
            idx.all.forEach(ch => recipients.add(ch): Unit)
            Option(idx.byKey.get(event.key)).foreach(_.forEach(ch => recipients.add(ch): Unit))
          handlers.foreach(ch => recipients.add(ch): Unit)
          recipients.forEach(_.offer(event))
        }
      case _ =>
        if !everything.isEmpty then everything.forEach(_.offer(event))
        val idx = topics.get(event.topic)
        if idx != null then
          idx.all.forEach(_.offer(event))
          val keyed = idx.byKey.get(event.key)
          if keyed != null then keyed.forEach(_.offer(event))

object EventBus:
  /** 无界邮箱的只读健康快照。`oldestEventAgeMs` 是保守上界：队列未清空时不为每条热路径
    * 事件额外分配时间戳对象，因此它可能高估、绝不会低估最老消息的等待时间。 */
  final case class MailboxHealth(
      queued: Long,
      highWaterMark: Long,
      oldestEventAgeMs: Long,
      inFlightAgeMs: Long,
      processed: Long,
      lastProcessingNanos: Long,
      maxProcessingNanos: Long,
  )

  /** 一个订阅者的邮箱：事件流 + 退订句柄。
    *
    * 退订不是可选的收尾动作 —— 订阅者停掉后若不从索引摘除，它那条无界 channel 会继续
    * 累积事件直到进程退出。动态起停策略的场景下这就是一条稳定的内存泄漏。
    */
  final class Mailbox private[event] (private[event] val channel: Channel[AnyEvent], remove: () => Unit) extends AutoCloseable:
    private var removed = false
    private[event] var handledByTopic: Map[CommandTopic[?, ?], Set[Any]] = Map.empty
    private val queued = AtomicLong(0)
    private val highWaterMark = AtomicLong(0)
    private val oldestEnqueueNanos = AtomicLong(0)
    private val processed = AtomicLong(0)
    private val inFlightStartedNanos = AtomicLong(0)
    private val lastProcessingNanos = AtomicLong(0)
    private val maxProcessingNanos = AtomicLong(0)
    private val queueHealthLock = new Object

    /** 本邮箱的事件流。消费到 [[done]] 之后排空为止 (`Source.foreach` 正是这个语义) */
    def events: Source[AnyEvent] = channel

    /** 直接投一条进本邮箱，**不经总线**。
      *
      * 给的是"把外部线程的输入串行化到 actor 线程"这一件事 (见 [[hft.actor.ActorContext.tell]])：
      * 柜台的私有推送在 WS 连接线程上解析出来，而账本的写者必须只有 actor 线程一个。
      * 借道总线也能做到，但那会让一件纯内部的事在总线上流动 —— 不需要暴露的就不该暴露。
      *
      * 用 `sendOrClosed`：邮箱可能正在停机，向一个正在退出的 actor 投递失败是正常竞态。
      */
    private[hft] def offer(event: AnyEvent): Unit =
      queueHealthLock.synchronized {
        val depth = queued.incrementAndGet()
        if depth == 1 then oldestEnqueueNanos.set(System.nanoTime())
        highWaterMark.accumulateAndGet(depth, Math.max)
        channel.sendOrClosed(event) match
          case _: ox.channels.ChannelClosed => markConsumedLocked()
          case _                            => ()
      }

    /** 由 ActorSystem 消费并统一记录处理耗时；插件不能绕过该入口消费 actor 邮箱。 */
    private[hft] def consumeEach(process: AnyEvent => Unit): Unit =
      channel.foreach { event =>
        markConsumed()
        val started = System.nanoTime()
        inFlightStartedNanos.set(started)
        try process(event)
        finally
          val elapsed = System.nanoTime() - started
          inFlightStartedNanos.compareAndSet(started, 0L)
          processed.incrementAndGet()
          lastProcessingNanos.set(elapsed)
          maxProcessingNanos.accumulateAndGet(elapsed, Math.max)
      }

    private def markConsumed(): Unit =
      queueHealthLock.synchronized(markConsumedLocked())

    private def markConsumedLocked(): Unit =
      val remaining = queued.decrementAndGet()
      if remaining <= 0 then
        queued.set(0)
        oldestEnqueueNanos.set(0)

    private[hft] def health: MailboxHealth = queueHealthLock.synchronized {
      val depth = queued.get
      val oldest = oldestEnqueueNanos.get
      val inFlight = inFlightStartedNanos.get
      val now = System.nanoTime()
      val ageMs = if depth == 0 || oldest == 0 then 0L else (now - oldest) / 1_000_000L
      val inFlightAgeMs = if inFlight == 0 then 0L else (now - inFlight) / 1_000_000L
      MailboxHealth(
        depth,
        highWaterMark.get,
        ageMs,
        inFlightAgeMs,
        processed.get,
        lastProcessingNanos.get,
        maxProcessingNanos.get,
      )
    }

    /** 从总线摘除，不再有新事件进来。幂等 —— 停机路径上重复调用是常态。
      *
      * 与 [[done]] 是两件事：这一步只断开投递源，邮箱里已经排队的事件仍在。
      */
    override def close(): Unit = synchronized {
      if !removed then
        removed = true
        remove()
    }

    /** 关闭邮箱：已缓冲的事件仍会被投递完，消费者随后收到流结束。
      *
      * 停机时先 [[close]] 再 `done`，消费者就能把积压事件处理完再退出 —— 丢一条成交回报
      * 就是本地仓位与交易所发散。
      *
      * 幂等 (`doneOrClosed` 对已关闭的邮箱返回而不抛)：与 [[close]] 一样，
      * 停机路径上重复调用是常态。
      */
    def done(): Unit = channel.doneOrClosed(): Unit
