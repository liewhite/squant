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

  def capability[K](capability: Capability[K], key: K, description: String = ""): Requirement =
    new Requirement(capability, key, description)
