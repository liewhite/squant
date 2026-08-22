package hft.engine

import hft.domain.AccountInstrument

import scala.collection.mutable

/** (账户, 标的) 的独占登记：一个标的在一个账户下最多归一个策略实例。
  *
  * 不是洁癖。私有回报按 (账户, 标的) 路由，两个实例占同一个键就会互相收到对方的成交与
  * 订单回报 —— 各自把账户总仓位当成自己的敞口，撤下一个还可能撤掉另一个的活单，
  * 而这些都没有外在症状。所以宁可在装配期直接拒绝启动。
  *
  * **实盘与影子盘跑同一标的是允许的**：它们账户不同，键就不同。这正是本约束要保住的场景
  * —— 若把约束定成"一个标的一个策略"，同一份逻辑就没法同时跑实盘与影子盘了。
  *
  * @tparam H 持有者句柄的类型 (引擎里是 actor 句柄)
  */
final class InstrumentClaims[H]:
  private val owners = mutable.Map.empty[AccountInstrument, String]
  private val heldBy = mutable.Map.empty[H, Set[AccountInstrument]]

  /** 只查不落地：任一冲突即抛错。
    *
    * 与 [[claimAll]] 分开，是为了让拒绝发生在**策略启动之前** —— 起来了再拒绝，就得再把
    * 它停回去。同一批内部的互撞也算冲突：两个策略在同一次调用里绑同一个 (账户, 标的) 同样不行。
    */
  def checkAll(entries: Seq[(String, Set[AccountInstrument])]): Unit =
    val incoming = mutable.Map.empty[AccountInstrument, String]
    entries.foreach { (name, keys) =>
      keys.foreach { key =>
        owners.get(key).orElse(incoming.get(key)).foreach { owner =>
          throw IllegalStateException(
            s"$key 已被 $owner 占用，$name 不能重复接管: " +
              "同一账户下一个标的最多归一个策略实例 (实盘与影子盘账户不同，可以跑同一标的)"
          )
        }
        incoming(key) = name
      }
    }

  /** 落地登记。调用方须先经 [[checkAll]] 确认无冲突 */
  def claimAll(entries: Seq[(H, String, Set[AccountInstrument])]): Unit =
    checkAll(entries.map((_, name, keys) => (name, keys)))
    entries.foreach { (holder, name, keys) =>
      keys.foreach(k => owners(k) = name)
      heldBy(holder) = keys
    }

  /** 释放某持有者占用的全部键。幂等 */
  def release(holder: H): Unit =
    heldBy.remove(holder).foreach(_.foreach(owners.remove))

  /** 某个键当前归谁 */
  def ownerOf(key: AccountInstrument): Option[String] = owners.get(key)

  def size: Int = owners.size
