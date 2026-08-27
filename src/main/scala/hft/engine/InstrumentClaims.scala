package hft.engine

import hft.domain.AccountInstrument

import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable

/** `(账户, 标的)` 的独占租约表：一个键在任意时刻最多归一个策略会话。 */
final class InstrumentClaims:
  private val owners = mutable.Map.empty[AccountInstrument, String]

  /** 原子获取一组键。任一冲突即整组拒绝，不留下部分登记。 */
  def acquire(owner: String, keys: Set[AccountInstrument]): InstrumentClaim = synchronized {
    keys.foreach { key =>
      owners.get(key).foreach { current =>
        throw IllegalStateException(
          s"$key 已被 $current 占用，$owner 不能重复接管: " +
            "同一账户下一个标的最多归一个策略实例 (实盘与影子盘账户不同，可以跑同一标的)"
        )
      }
    }
    keys.foreach(key => owners(key) = owner)
    InstrumentClaim(this, owner, keys)
  }

  private[engine] def release(claim: InstrumentClaim): Unit = synchronized {
    claim.keys.foreach { key =>
      if owners.get(key).contains(claim.owner) then owners.remove(key)
    }
  }

  def ownerOf(key: AccountInstrument): Option[String] = synchronized(owners.get(key))
  def size: Int = synchronized(owners.size)

/** 独占权的唯一释放凭证。关闭幂等，适合作为 Actor 的受管资源。 */
final class InstrumentClaim private[engine] (
    registry: InstrumentClaims,
    private[engine] val owner: String,
    private[engine] val keys: Set[AccountInstrument],
) extends AutoCloseable:
  private val closed = AtomicBoolean(false)
  override def close(): Unit =
    if closed.compareAndSet(false, true) then registry.release(this)
