package hft.event

/** 事件族 —— 框架面向用户的**开放扩展点**。
  *
  * 一个 Topic 同时钉死两件事：这一族事件按什么**路由** (`K`)、载荷是什么**类型** (`P`)。
  * 新增事件类型只需在自己的代码里声明一个 object，框架一行不改 —— 这正是封闭 enum
  * 做不到的：那种写法每加一个变体都要回来改路由表与各处 match。
  *
  * {{{
  * object AlphaSignal extends Topic[Symbol, Score]("alphaSignal"):
  *   def keyOf(p: Score): Symbol = p.symbol
  * }}}
  *
  * 需要"按名字参数化"的场景 (如一族自定义指标) 不要为此造多个 topic：把名字放进 `K`
  * (例如 `Topic[MetricKey, Double]`，`MetricKey` 含指标名)，路由粒度一样精确。
  *
  * ## 身份即引用
  *
  * 框架按**引用**判别 topic 身份 —— 投递索引的键、[[Event.as]] 的类型还原都依赖它。
  * [[equals]]/[[hashCode]] 在此固定为引用语义且声明为 `final`，任何子类都改不掉
  * (`case class` 子类也不例外：Scala 见父类已有 equals 便不再合成)。
  *
  * 这条保证挡住的是最危险的失效：两个结构相等、类型参数不同的 topic 实例撞进同一个索引槽，
  * 事件被投给错误的订阅者，而消费侧 [[Event.as]] 因引用不等静默返回 `None` —— 没有任何症状。
  * 有了它，投递索引 (走 `equals`/`hashCode`) 与类型还原 (走 `eq`) 永远是同一个判据。
  *
  * ## 仍然要声明为 object
  *
  * 身份既是引用，`MyTopic("x")` 造出的两个实例就是两个不同的 topic —— 发布方和订阅方各造
  * 一个，谁也匹配不上谁。这个失效是可见的 (完全收不到事件)，但没有理由去踩：
  * 单例是唯一正确的声明方式。
  *
  * ## 路由键由载荷派生，不单独传
  *
  * [[keyOf]] 是路由键的唯一出处，事件构造时算一次并缓存在 [[Event.key]]。若允许 key 独立
  * 传入，就会出现"key 说 BTCUSDT、载荷里却是 ETHUSDT"的静默错投 —— 那种 bug 没有任何
  * 外在症状，只表现为某个订阅者莫名其妙收不到或收错事件。
  *
  * @param name 诊断用名字 (日志/错误信息)；**不作类型判别**，判别一律用引用或 [[Event.as]]
  */
abstract class Topic[K, P](val name: String):
  /** 从载荷取路由键 */
  def keyOf(payload: P): K

  // 身份即引用，见类文档。final 使任何子类都改不掉这个判据。
  final override def equals(that: Any): Boolean = that match
    case other: AnyRef => this eq other
    case _             => false

  final override def hashCode: Int = System.identityHashCode(this)

  override def toString: String = name
