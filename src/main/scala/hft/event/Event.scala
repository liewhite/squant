package hft.event

import hft.domain.{Timestamp, nowMs}

/** 擦除了类型参数的事件 —— 总线与订阅者邮箱里流动的形态。
  *
  * 消费侧用 [[Event.as]] 按 topic 还原静态类型，不要 `asInstanceOf`。
  */
type AnyEvent = Event[?, ?]

/** 总线上流动的一条事件：一个 [[Topic]] 的一份载荷，附两个时间戳。
  *
  * 主构造器私有 —— 只能经伴生对象构造，从而保证 [[key]] 恒等于 `topic.keyOf(payload)`
  * (同理 `copy` 也被禁用：改了载荷却留着旧 key 会静默错投)。
  *
  * @param exchangeTs 交易所推送的时间戳 (本地产生的事件同 localTs)
  * @param localTs    本地接收/产生时刻
  */
final case class Event[K, P] private (
    topic: Topic[K, P],
    key: K,
    payload: P,
    exchangeTs: Timestamp,
    localTs: Timestamp,
):
  /** 本事件是否属于 `t` */
  def is(t: Topic[?, ?]): Boolean = topic eq t

  /** 若本事件属于 `t`，以 `t` 的**静态载荷类型**取回载荷，否则 `None`。
    *
    * 这是消费侧还原类型的唯一入口。cast 的依据是 [[Topic]] 的"必须声明为 object"约定：
    * topic 引用相等 ⟹ 类型参数相同 ⟹ `P2` 就是 `P`。框架的类型擦除只出现在"按 topic 还原
    * 类型"的两处 —— 本方法与 [[Interest.keysOf]]，业务代码里不应再出现 `asInstanceOf`。
    */
  def as[K2, P2](t: Topic[K2, P2]): Option[P2] =
    if topic eq t then Some(payload.asInstanceOf[P2]) else None

  override def toString: String = s"Event($topic, key=$key, $payload)"

object Event:
  /** 交易所推送的事件：`exchangeTs` 来自对端，`localTs` 取收到时刻。
    *
    * 时间戳必须在**响应到手之后**盖 —— 盖在发起请求之前会把整段网络往返算进事件年龄，
    * 把"对端慢"误诊成"本机处理不过来"。
    */
  def at[K, P](topic: Topic[K, P], payload: P, exchangeTs: Timestamp): Event[K, P] =
    new Event(topic, topic.keyOf(payload), payload, exchangeTs, nowMs)

  /** 本地产生的事件 (时钟、REST 拉取、合成回报)：没有交易所时间戳，两个戳同取本地时刻 */
  def local[K, P](topic: Topic[K, P], payload: P): Event[K, P] =
    val ts = nowMs
    new Event(topic, topic.keyOf(payload), payload, ts, ts)

  /** 显式指定两个时间戳 (回测虚拟时间：不读墙钟，同一输入必得同一结果) */
  def stamped[K, P](topic: Topic[K, P], payload: P, exchangeTs: Timestamp, localTs: Timestamp): Event[K, P] =
    new Event(topic, topic.keyOf(payload), payload, exchangeTs, localTs)
