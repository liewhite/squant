package hft.event

import ox.channels.{Channel, Source}

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList

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
  * 发布远多于订阅，故索引用 [[ConcurrentHashMap]] + [[CopyOnWriteArrayList]]：
  * 投递路径无锁、无分配；订阅路径 (罕见) 复制一次数组。
  */
final class EventBus:
  /** 一个 topic 的投递索引：按 key 定向的订阅者 + 该 topic 的全量订阅者 */
  private final class TopicIndex:
    val byKey: ConcurrentHashMap[Any, CopyOnWriteArrayList[Channel[AnyEvent]]] = ConcurrentHashMap()
    val all: CopyOnWriteArrayList[Channel[AnyEvent]] = CopyOnWriteArrayList()

  private val topics: ConcurrentHashMap[Topic[?, ?], TopicIndex] = ConcurrentHashMap()

  /** 无差别收下一切的订阅者，见 [[subscribeAll]]。通常为空，热路径上只是一次长度检查 */
  private val everything: CopyOnWriteArrayList[Channel[AnyEvent]] = CopyOnWriteArrayList()

  /** 订阅/退订的互斥锁。
    *
    * 只锁冷路径 —— [[publish]] 不参与，热路径仍然无锁。
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
  def subscribe(interests: Set[Interest]): EventBus.Mailbox = registrationLock.synchronized {
    val ch = Channel.unlimited[AnyEvent]
    val allTopics: Set[Topic[?, ?]] = interests.collect { case Interest.All(t) => t }
    val keyedByTopic: Map[Topic[?, ?], Set[Any]] =
      interests.foldLeft(Map.empty[Topic[?, ?], Set[Any]]) {
        case (acc, Interest.Keyed(t, keys)) if !allTopics.contains(t) =>
          acc.updated(t, acc.getOrElse(t, Set.empty) ++ keys.toSet[Any])
        case (acc, _) => acc
      }
    allTopics.foreach(t => indexOf(t).all.add(ch))
    keyedByTopic.foreach { (t, keys) =>
      val idx = indexOf(t)
      keys.foreach(k => idx.byKey.computeIfAbsent(k, _ => CopyOnWriteArrayList()).add(ch))
    }
    EventBus.Mailbox(ch, () => registrationLock.synchronized(remove(ch, allTopics, keyedByTopic)))
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
    * 中继务必跳过指令面事件 (见 [[Commands.all]])：指令是从主总线**流进来**的，
    * 原样转回去会让它在两条总线之间无限弹跳。
    */
  def subscribeAll(): EventBus.Mailbox = registrationLock.synchronized {
    val ch = Channel.unlimited[AnyEvent]
    everything.add(ch)
    EventBus.Mailbox(ch, () => registrationLock.synchronized(everything.remove(ch): Unit))
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

  /** 这条 `(topic, key)` 上有没有人接。
    *
    * **指令面契约校验的唯一依据**（见 [[Commands]]）：引擎在装配期用它确认策略将要发出的
    * 每一条指令都有接单者，没有就拒绝启动。校验的是订阅事实本身，因此不需要任何插件
    * 额外声明"我提供什么" —— 订阅是它为了工作本来就必须做的事，多一份声明就多一处
    * 会写错、会漏写的事实。
    */
  def hasSubscriber[K](topic: Topic[K, ?], key: K): Boolean = subscriberCount(topic, key) > 0

  /** 定向订阅了这个 `(topic, key)` 的订阅者数 —— **不含**该 topic 的全量订阅者。
    *
    * 指令面的契约校验用的是它，而不是 [[subscriberCount]]：两者的差别正是
    * **接单者与旁观者的差别**。一个订了 `Interest.All(OrderIntent)` 的意图记录器是旁观者，
    * 它不会执行任何订单；若把它算作接单者，柜台没装也能通过校验，而订单永远发不出去 ——
    * 那恰是这道校验存在的全部理由要防的那种零症状失效。
    *
    * 反过来，接单者**必然**是定向订阅的：它服务的是某个确定的账户/交易所，
    * 全量订阅意味着它连"哪些单归自己"都没想清楚。
    */
  def directSubscriberCount[K](topic: Topic[K, ?], key: K): Int =
    Option(topics.get(topic)).fold(0)(idx => Option(idx.byKey.get(key)).fold(0)(_.size))

  /** 擦除了 key 类型的同一个查询 —— 只给"从一份 [[Interest]] 声明反查"用
    * (那里的 key 类型已被擦除)。限定 `private[hft]`: 公开面留给类型安全的那一个,
    * 免得业务代码拿一个类型对不上的 key 查出个永远为假的答案。 */
  private[hft] def hasAnySubscriber(topic: Topic[?, ?], key: Any): Boolean =
    Option(topics.get(topic)).exists { idx =>
      !idx.all.isEmpty || Option(idx.byKey.get(key)).exists(!_.isEmpty)
    }

  /** [[directSubscriberCount]] 的擦除版，理由同上 */
  private[hft] def hasAnyDirectSubscriber(topic: Topic[?, ?], key: Any): Boolean =
    Option(topics.get(topic)).exists(idx => Option(idx.byKey.get(key)).exists(!_.isEmpty))

  /** 把一条 channel 从它登记过的每个槽里摘除。
    *
    * 只摘自己登记过的位置 (而不是遍历全索引)，因此代价与本订阅者的声明规模成正比，
    * 与总订阅者数无关。空掉的 key 槽一并删除，否则长期起停会攒下一堆空列表。
    */
  private def remove(
      ch: Channel[AnyEvent],
      allTopics: Set[Topic[?, ?]],
      keyedByTopic: Map[Topic[?, ?], Set[Any]],
  ): Unit =
    allTopics.foreach(t => Option(topics.get(t)).foreach(_.all.remove(ch)))
    keyedByTopic.foreach { (t, keys) =>
      Option(topics.get(t)).foreach { idx =>
        keys.foreach { k =>
          Option(idx.byKey.get(k)).foreach { subscribers =>
            subscribers.remove(ch)
            if subscribers.isEmpty then idx.byKey.remove(k, subscribers)
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
    if !everything.isEmpty then everything.forEach(ch => ch.sendOrClosed(event): Unit)
    val idx = topics.get(event.topic)
    if idx != null then
      idx.all.forEach(ch => ch.sendOrClosed(event): Unit)
      val keyed = idx.byKey.get(event.key)
      if keyed != null then keyed.forEach(ch => ch.sendOrClosed(event): Unit)

object EventBus:
  /** 一个订阅者的邮箱：事件流 + 退订句柄。
    *
    * 退订不是可选的收尾动作 —— 订阅者停掉后若不从索引摘除，它那条无界 channel 会继续
    * 累积事件直到进程退出。动态起停策略的场景下这就是一条稳定的内存泄漏。
    */
  final class Mailbox private[event] (channel: Channel[AnyEvent], remove: () => Unit) extends AutoCloseable:
    private var removed = false

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
    private[hft] def offer(event: AnyEvent): Unit = channel.sendOrClosed(event): Unit

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
