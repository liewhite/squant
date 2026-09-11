package hft.exchange

import hft.domain.{ExchangeError, Instrument, InstrumentKind, SymbolMeta}

import java.util.concurrent.atomic.AtomicReference

/** 一个交易所的合约规格表 —— 按标的索引，只增不减，并且**记得住哪些品种已经拉过**。
  *
  * ## 为什么它是一个对象，而不是接口里的一个字段
  *
  * 从前这张表是 `ExchangeClient` trait 里的 `private val`，配一组 `final` 方法。那让
  * `ExchangeClient` 的文档承诺 ("进程内共享的一份") 变成一句空话，两个方向上都不成立：
  *
  *   - **每个实例一份**。同一个交易所建两个客户端 (只读看板给行情源建 public、给账户轮询建
  *     trading) 就是两张互不相干的空表，谁加载谁自己那份 —— 而它们说的是同一件事。
  *   - **装饰器无法转发**。`private val` + `final` 方法意味着 [[DryRunClient]] 这类包装
  *     必然自带一张独立的空表，且没有任何办法把它接到 delegate 上去。
  *
  * 状态不该住在接口里。现在它是一个可以被持有、被转发、被共享的东西，而 `ExchangeClient`
  * 只声明"我有一张表"。
  *
  * ## 只增不减
  *
  * 规格是合约的静态事实，到期不会改变它。整表替换会在到期日当天把一个仍持有仓位的合约的
  * 规格抹掉，于是记账路径上的查表抛错、进程终止。表只增不减，进程重启即清空。
  *
  * ## "拉过" 与 "表里有" 是两件事
  *
  * [[isLoaded]] 记的是**这个品种的规格端点访问过了**，而不是"表里有这个品种的条目"。
  * 两者会分开：一个所可能确实一张期权合约都没上市，那时拉取成功但一条都不返回 ——
  * 按"表里有没有"判断的话，每个调用点都会为它再打一次 REST，永远打下去。
  */
final class MetaTable:
  private val state = AtomicReference(MetaTable.State(Map.empty, Set.empty))

  /** 已知的合约规格快照 (只读) */
  def known: Map[Instrument, SymbolMeta] = state.get().metas

  def get(instrument: Instrument): Option[SymbolMeta] = state.get().metas.get(instrument)

  /** 这个品种的规格端点是否已经访问过 —— 见类文档"拉过与表里有是两件事" */
  def isLoaded(kind: InstrumentKind): Boolean = state.get().loaded.contains(kind)

  /** 合并一个品种的规格并记下它已加载，返回本次拉到的条目数。
    *
    * 合并而不是整表替换 (见类文档)。并发调用经 CAS 合并，不会互相丢失。
    *
    * `private[exchange]`：写入只有 [[ExchangeClient.ensureMetas]] 一条路径。表是公开的
    * (装饰器要转发它)，但公开的是"读得到"，不是"谁都能往里塞一份没经过 `fetchMetas` 的规格"。
    */
  private[exchange] def merge(kind: InstrumentKind, fetched: Vector[SymbolMeta]): Int =
    val added = fetched.map(m => m.instrument -> m).toMap
    state.updateAndGet(s => MetaTable.State(s.metas ++ added, s.loaded + kind))
    added.size

  /** 这个品种没拉过就拉一次，拉过就什么都不做。返回本次拉到的条目数，`None` 表示已经拉过。
    *
    * **串行化**：两条线程同时为同一个品种调它 (OKX 的行情源与私有流在各自的 actor 线程上
    * 建立连接) 只会打一次 REST，后到的那条等它拉完、看到已加载、直接返回。持锁期间在等一次
    * 网络往返，而这是连接建立时的冷路径 —— 让第二条线程白打一次 REST 才是更差的选择。
    */
  private[exchange] def ensure(kind: InstrumentKind)(
      fetch: => Either[ExchangeError, Vector[SymbolMeta]]
  ): Either[ExchangeError, Option[Int]] =
    // 快路径不进锁: 已加载是绝大多数情况 (每次 REST 响应侧调用都会问一遍), 而锁里可能
    // 正有另一个品种在做网络往返 —— 拿已经加载好的品种去等它没有道理。
    if isLoaded(kind) then Right(None)
    else
      synchronized {
        if isLoaded(kind) then Right(None)
        else fetch.map(fetched => Some(merge(kind, fetched)))
      }

object MetaTable:
  /** 表与"拉过哪些品种"必须一起变 —— 分成两个原子引用就有了一个"已合并、尚未记为已加载"
    * 的中间态，那一刻的并发 [[MetaTable.ensure]] 会多打一次 REST。 */
  private final case class State(metas: Map[Instrument, SymbolMeta], loaded: Set[InstrumentKind])
