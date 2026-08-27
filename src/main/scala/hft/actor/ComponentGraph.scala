package hft.actor

import hft.event.{CommandHandler, Interest}
import hft.kernel.{Capability, CapabilityProvider, Cardinality}

import scala.jdk.CollectionConverters.*

/** 一次装配只读取一次插件声明；验证、接线、运行和停机共享这份事实。 */
private[actor] final case class ActorSpec(
    actor: Actor,
    name: String,
    interests: Set[Interest],
    commandHandlers: Set[CommandHandler],
    capabilities: Set[CapabilityProvider],
    requirements: Set[Requirement],
)

/** 组件能力图与所有权树上的纯校验/排序算法，不持有运行时锁或生命周期副作用。 */
private[actor] object ComponentGraph:
  def snapshot(actor: Actor): ActorSpec =
    val commandHandlers = actor.commandHandlers
    ActorSpec(
      actor,
      actor.name,
      actor.interests,
      commandHandlers,
      actor.capabilities ++ commandHandlers.map(handler =>
        CapabilityProvider.erased(handler.topic.capability, handler.key)
      ),
      actor.requirements,
    )

  def descendants(handle: ActorHandle): Vector[ActorHandle] =
    handle +: handle.children.asScala.toVector.flatMap(descendants)

  def provides(handle: ActorHandle, capability: Capability[?], key: Any): Boolean =
    handle.capabilities.exists(provider => (provider.capability eq capability) && provider.key == key)

  def validateAddition(
      specs: Vector[ActorSpec],
      existing: Vector[ActorHandle],
      unavailable: Set[ActorHandle],
  ): Unit =
    val plannedCounts = scala.collection.mutable.HashMap.empty[(Capability[?], Any), Int]
    specs.foreach(_.capabilities.foreach { provider =>
      val key = (provider.capability, provider.key)
      plannedCounts.update(key, plannedCounts.getOrElse(key, 0) + 1)
    })

    def availableCount(capability: Capability[?], key: Any): Int =
      existing.count(handle => !unavailable.contains(handle) && provides(handle, capability, key))

    plannedCounts.foreach { case ((capability, key), added) =>
      val total = availableCount(capability, key) + added
      if capability.cardinality == Cardinality.ExactlyOne && total != 1 then
        throw IllegalStateException(
          s"能力 $capability@$key 要求恰好一个提供者, 已有提供者或本批重复提供, 装配后将有 $total 个"
        )
    }

    val missing = specs.flatMap { spec =>
      spec.requirements.flatMap { requirement =>
        val count = availableCount(requirement.capability, requirement.key) +
          plannedCounts.getOrElse((requirement.capability, requirement.key), 0)
        missingRequirement(spec.name, requirement, count)
      }
    }
    if missing.nonEmpty then
      throw IllegalStateException("硬依赖未满足:\n  " + missing.sorted.mkString("\n  "))

  def validateRequirements(
      requirements: Set[Requirement],
      owner: String,
      existing: Vector[ActorHandle],
      unavailable: Set[ActorHandle],
  ): Unit =
    val missing = requirements.flatMap { requirement =>
      val count = existing.count(handle =>
        !unavailable.contains(handle) && provides(handle, requirement.capability, requirement.key)
      )
      missingRequirement(owner, requirement, count).map(_.stripPrefix(s"组件 $owner: "))
    }
    if missing.nonEmpty then
      throw IllegalStateException(s"$owner 的硬依赖未满足:\n  " + missing.toVector.sorted.mkString("\n  "))

  def validateRemoval(
      removing: Set[ActorHandle],
      existing: Vector[ActorHandle],
      alreadyStopping: Set[ActorHandle],
  ): Unit =
    val unavailable = alreadyStopping ++ removing
    val availableProviders = existing.filterNot(unavailable)
    val liveDependents = existing.filter { handle =>
      !removing.contains(handle) && (handle.finished.getCount > 0 || handle.state == ActorState.Quarantined)
    }
    val missing = liveDependents.flatMap { dependent =>
      dependent.requirements.flatMap { requirement =>
        val after = availableProviders.count(candidate => provides(candidate, requirement.capability, requirement.key))
        Option.when(!requirement.capability.cardinality.accepts(after))(
          s"${dependent.name} 依赖 ${requirement.capability}@${requirement.key} " +
            s"(${requirement.capability.cardinality.explain}), 移除后剩 $after 个提供者"
        )
      }
    }
    if missing.nonEmpty then
      throw IllegalStateException("拒绝停止组件，仍存活的组件会丢失硬依赖:\n  " + missing.mkString("\n  "))

  /** 依赖方先于提供方、子组件先于父组件；其余节点保持逆装配序。 */
  def stopOrder(handles: Set[ActorHandle], assemblyOrder: Vector[ActorHandle]): Vector[ActorHandle] =
    if handles.isEmpty then return Vector.empty
    val rank = assemblyOrder.zipWithIndex.toMap.withDefaultValue(-1)
    val outgoing = handles.map(_ -> scala.collection.mutable.HashSet.empty[ActorHandle]).toMap
    val indegree = scala.collection.mutable.HashMap.from(handles.map(_ -> 0))

    def before(first: ActorHandle, second: ActorHandle): Unit =
      if first != second && handles.contains(first) && handles.contains(second) && outgoing(first).add(second) then
        indegree.update(second, indegree(second) + 1)

    handles.foreach(parent => parent.children.asScala.foreach(child => before(child, parent)))
    handles.foreach { dependent =>
      dependent.requirements.foreach { requirement =>
        handles.iterator
          .filter(provider => provides(provider, requirement.capability, requirement.key))
          .foreach(provider => before(dependent, provider))
      }
    }

    val ready = scala.collection.mutable.ArrayBuffer.from(handles.filter(indegree(_) == 0))
    val result = scala.collection.mutable.ArrayBuffer.empty[ActorHandle]
    while ready.nonEmpty do
      val nextIndex = ready.indices.maxBy(i => rank(ready(i)))
      val next = ready.remove(nextIndex)
      result += next
      outgoing(next).foreach { successor =>
        val degree = indegree(successor) - 1
        indegree.update(successor, degree)
        if degree == 0 then ready += successor
      }
    if result.size != handles.size then
      val cycle = handles.diff(result.toSet).toVector.sortBy(_.name).map(_.name).mkString(", ")
      throw IllegalStateException(s"组件依赖与生命周期所有权形成停机环: $cycle")
    result.toVector

  private def missingRequirement(owner: String, requirement: Requirement, count: Int): Option[String] =
    Option.when(!requirement.capability.cardinality.accepts(count)) {
      val detail = if requirement.description.isEmpty then "" else s" (${requirement.description})"
      s"组件 $owner: ${requirement.capability}@${requirement.key} " +
        s"要求${requirement.capability.cardinality.explain}提供者, 实际 $count 个$detail"
    }
