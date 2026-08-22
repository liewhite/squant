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
  def subscribe(interests: Set[Interest]): Source[AnyEvent] =
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
    ch

  /** 发布一条事件给关心它的订阅者。无人订阅该 topic 时是一次哈希查找后返回。 */
  def publish(event: AnyEvent): Unit =
    val idx = topics.get(event.topic)
    if idx != null then
      idx.all.forEach(_.send(event))
      val keyed = idx.byKey.get(event.key)
      if keyed != null then keyed.forEach(_.send(event))
