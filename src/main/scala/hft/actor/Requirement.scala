package hft.actor

import hft.event.CommandTopic
import hft.kernel.Capability

/** 组件正常运行所必需的一项能力。
  *
  * 依赖与消息订阅是两件事：订阅说明“我想收到什么”，Requirement 说明“缺了谁我就不能正确
  * 工作”。命令能力由处理声明自动提供；存储、时钟等非消息能力用 [[Requirement.capability]]。
  *
  * 构造器私有，业务侧只能经类型安全的工厂创建，避免 key 类型与能力不匹配。
  */
final case class Requirement private (
    capability: Capability[?],
    key: Any,
    description: String,
)

object Requirement:
  def command[K](topic: CommandTopic[K, ?], key: K, description: String = ""): Requirement =
    new Requirement(topic.capability, key, description)

  /** 一份订阅范围所要求的**行情订阅能力**：涉及的每个交易所都得有行情插件接单。
    *
    * 放在这里而不是 `Subscription` 上：那会让 `hft.event` 反向依赖 `hft.actor`
    * (后者本就依赖前者) —— 包级成环。依赖方向固定为 `kernel <- event <- actor`，
    * "装配期谁必须存在"是 actor 层的事实，不该挂到描述"我关心什么"的数据结构上。
    *
    * 收在一处的理由：`Engine.watchMarket` 与 `Executor.requirements` 从前各写一遍同样的
    * map 与同样的错误文案 —— 同一条判据两处表达。 */
  def marketSubscriptions(subscription: hft.event.Subscription): Set[Requirement] =
    subscription.marketStreams.map(_._1).map { exchange =>
      command(hft.event.Commands.MarketSubscription, exchange, s"行情订阅指令 $exchange 无处理者")
    }

  def capability[K](capability: Capability[K], key: K, description: String = ""): Requirement =
    new Requirement(capability, key, description)
