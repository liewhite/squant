package hft.event

import hft.domain.Timestamp

/** 订阅声明与事件处理的**绑定** —— 一处声明，两件事都定了。
  *
  * ## 为什么合一
  *
  * 分开写时，"订阅什么"（`interests`）与"怎么处理"（`onEvent` 里的一串 `as`）是同一事实的
  * 两处表达，而没有任何东西保证它们一致：漏订了就静默收不到，漏处理了就静默丢弃，
  * 两种失效都没有外在症状。合一之后 [[interests]] 从处理器派生，
  * "订阅了不处理"和"处理了没订阅"在结构上不可能发生。
  *
  * ## 处理器拿到的是具体类型
  *
  * `on(Topics.Bbo, ...) { (bbo, ctx) => ... }` 里的 `bbo` 就是 `BBO`，不需要 `as`、
  * 不需要模式匹配、也不会漏掉 `getOrElse(Vector.empty)`。类型由 [[Topic]] 的载荷参数给出。
  *
  * ## 分发判据与总线同源
  *
  * [[dispatch]] 用 [[Interest.accepts]] 过滤，与总线的投递索引、回测的过滤是同一份判据 ——
  * 实盘（总线已过滤）与回测（直接 dispatch）因此不会分叉。
  *
  * @tparam C 处理器的上下文类型（策略是状态视图，普通 actor 是 [[hft.actor.ActorContext]]）
  */
final class Handlers[C] private (private val entries: Vector[Handlers.Entry[?, ?, C]]):

  /** 订阅该 topic 下指定 key 的事件并处理 */
  def on[K, P](topic: Topic[K, P], keys: Set[K])(f: (P, C, Timestamp) => Vector[AnyEvent]): Handlers[C] =
    Handlers(entries :+ Handlers.Entry(topic, Interest.Keyed(topic, keys), f))

  /** 订阅该 topic 的全部事件并处理 */
  def onAll[K, P](topic: Topic[K, P])(f: (P, C, Timestamp) => Vector[AnyEvent]): Handlers[C] =
    Handlers(entries :+ Handlers.Entry(topic, Interest.All(topic), f))

  /** 只关心"事件到了"而不关心载荷（如时钟） */
  def onTick[K, P](topic: Topic[K, P])(f: (C, Timestamp) => Vector[AnyEvent]): Handlers[C] =
    onAll(topic)((_, c, now) => f(c, now))

  def ++(other: Handlers[C]): Handlers[C] = Handlers(entries ++ other.entries)

  /** 订阅声明 —— 从处理器派生，不是另写一份 */
  lazy val interests: Set[Interest] = entries.map(_.interest).toSet

  /** 按 topic 建索引：分发是一次哈希查找，不是逐个 `as` 试探 */
  private lazy val byTopic: Map[Topic[?, ?], Vector[Handlers.Entry[?, ?, C]]] =
    entries.groupBy(_.topic)

  /** 把事件交给关心它的处理器。无人处理时返回空 */
  def dispatch(event: AnyEvent, ctx: C, now: Timestamp): Vector[AnyEvent] =
    byTopic.get(event.topic) match
      case None          => Vector.empty
      case Some(matched) => matched.flatMap(_.run(event, ctx, now))

object Handlers:
  def empty[C]: Handlers[C] = new Handlers(Vector.empty)

  private final case class Entry[K, P, C](
      topic: Topic[K, P],
      interest: Interest,
      f: (P, C, Timestamp) => Vector[AnyEvent],
  ):
    def run(event: AnyEvent, ctx: C, now: Timestamp): Vector[AnyEvent] =
      // 判据用 Interest.accepts，与总线索引同源 —— 回测直接 dispatch 时同样要过滤 key
      if !interest.accepts(event) then Vector.empty
      else event.as(topic).map(p => f(p, ctx, now)).getOrElse(Vector.empty)
