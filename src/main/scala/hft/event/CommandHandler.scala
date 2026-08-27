package hft.event

/** 组件对一条命令键的处理声明。
  *
  * 它同时建立命令投递和能力事实；普通 [[Interest]] 即使定向订阅命令，也始终只是观察者。
  * 构造器私有，确保路由键类型与 topic 一致。
  */
final case class CommandHandler private (
    topic: CommandTopic[?, ?],
    key: Any,
)

object CommandHandler:
  def command[K](topic: CommandTopic[K, ?], key: K): CommandHandler = new CommandHandler(topic, key)
