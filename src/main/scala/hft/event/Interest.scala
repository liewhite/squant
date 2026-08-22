package hft.event

/** 订阅声明：一个订阅者要收某个 [[Topic]] 的哪些事件。
  *
  * ## 为什么是数据而不是谓词
  *
  * 写成 `Event => Boolean` 看起来更灵活，但会同时丢掉两样东西：
  *   - **可索引**：谓词无法建索引，每条 BBO 都要对全部订阅者求值一遍；声明成数据后
  *     投递是两次哈希查找，只发给真正关心的订阅者。
  *   - **可自省**：引擎要汇总所有订阅者的 [[Keyed]] 才知道该向交易所订阅哪些行情流。
  *     谓词读不出这个信息，就得让每个策略再声明一遍订阅范围 —— 同一事实两处写，
  *     两处必然会错开。
  */
sealed trait Interest:
  def topic: Topic[?, ?]

  /** 若本声明是针对 `t` 的 [[Interest.Keyed]]，以 `t` 的**静态 key 类型**取回订阅的 key
    * 集合；否则空集。
    *
    * 与 [[Event.as]] 是同一个还原手法、同一个安全依据 ([[Topic]] 的"必须声明为 object"
    * 约定)。框架的类型擦除只出现在这两处。
    */
  def keysOf[K](t: Topic[K, ?]): Set[K]

  /** 这条事件是否落在本声明内。
    *
    * 判据的唯一出处 —— [[EventBus]] 的投递索引是它的**扇出优化**而非第二份判据
    * (索引的键由同一批声明灌入)。两者若各写各的，恰好等价也只是巧合：
    * 改了其中一处，另一处不会编译失败，失效方式是某个订阅者静默收不到事件。
    */
  def accepts(event: AnyEvent): Boolean

object Interest:
  /** 订阅该 topic 的**全部**事件。
    *
    * 用于监控/记录类订阅者，以及本就没有路由维度的 topic (如 [[Topics.Clock]])。
    */
  final case class All(topic: Topic[?, ?]) extends Interest:
    /** 全量订阅不枚举 key —— 它压根没有 key 集合可给 */
    def keysOf[K](t: Topic[K, ?]): Set[K] = Set.empty
    def accepts(event: AnyEvent): Boolean = event.topic eq topic

  /** 只订阅该 topic 下指定 key 的事件 —— 策略的常态。
    *
    * 空 `keys` 等价于不订阅 (不会退化成全收)：订阅范围为空就该什么都收不到。
    */
  final case class Keyed[K](topic: Topic[K, ?], keys: Set[K]) extends Interest:
    def keysOf[K2](t: Topic[K2, ?]): Set[K2] =
      if topic eq t then keys.asInstanceOf[Set[K2]] else Set.empty

    // keys 视作 Set[Any] 只为调用 contains —— 它只做 equals 比较，类型不符即 false
    def accepts(event: AnyEvent): Boolean =
      (event.topic eq topic) && keys.asInstanceOf[Set[Any]].contains(event.key)
