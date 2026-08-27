package hft.kernel

/** 一个能力允许多少个提供者。 */
enum Cardinality:
  case AtLeastOne
  case ExactlyOne

  def accepts(count: Int): Boolean = this match
    case AtLeastOne => count >= 1
    case ExactlyOne => count == 1

  def explain: String = this match
    case AtLeastOne => "至少一个"
    case ExactlyOne => "恰好一个"

/** 生命周期装配器可校验的一类组件能力。
  *
  * 能力只描述“谁必须存在”，不承载调用接口；模块间实际交互仍由各自 trait 或消息协议定义。
  * 实例采用引用身份，同名能力不会被意外合并。
  */
final class Capability[K] private (val name: String, val cardinality: Cardinality):
  final override def equals(that: Any): Boolean = that match
    case other: AnyRef => this eq other
    case _             => false
  final override def hashCode: Int = System.identityHashCode(this)
  override def toString: String = name

object Capability:
  def apply[K](name: String, cardinality: Cardinality = Cardinality.ExactlyOne): Capability[K] =
    new Capability(name, cardinality)

/** 一个组件对某个带键能力的提供声明。构造入口保留键类型检查。 */
final case class CapabilityProvider private (capability: Capability[?], key: Any)

object CapabilityProvider:
  def provide[K](capability: Capability[K], key: K): CapabilityProvider = CapabilityProvider(capability, key)

  private[hft] def erased(capability: Capability[?], key: Any): CapabilityProvider =
    CapabilityProvider(capability, key)
