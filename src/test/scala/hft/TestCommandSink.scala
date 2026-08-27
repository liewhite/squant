package hft

import hft.actor.Actor
import hft.event.{CommandHandler, CommandTopic}

/** 测试用显式命令处理者。业务断言可另用 Interest 旁观同一条命令。 */
final class TestCommandSink[K](
    override val name: String,
    topic: CommandTopic[K, ?],
    keys: Set[K],
) extends Actor:
  override def commandHandlers: Set[CommandHandler] = keys.map(CommandHandler.command(topic, _))
