package hft.domain

/** 一个 (账户, 交易所) 上**各币种余额的当下状态**，以及"这份表是不是完整的"。
  *
  * ## 为什么是一个类型而不是两段代码
  *
  * 维护它的规则有三条，缺一不可：
  *   1. 收到全量 [[Wallet]] -> **整表替换**，快照里没有的币种余额就是 0（留着旧值会让一个
  *      已经清空的现货仓位继续参与计算）；
  *   2. 收到逐币种 [[Balance]] -> **只覆盖那一个币种**（三家的 WS 推送都只覆盖发生变动的币种，
  *      值是当前余额而非变化量）；
  *   3. **全量到达之前，"某币不在表里"不等于余额为 0** —— 那是 [[known]] 存在的全部理由。
  *
  * 依据见 [[Wallet]]。这三条从前在 `hft.state.StateManager` 与 `hft.dashboard.BoardSnapshot`
  * 各写了一遍，两处都只在注释里指向同一份文档 —— 而文档是依据，不是实现，它保证不了两份实现
  * 同步。改了其中一处不会有任何编译错误，失效形态是两个地方对同一个账户给出不同的现货敞口。
  *
  * 不可变、无 IO、不读墙钟：调用方要记时刻的话自己在外面包一层（看板就是这么做的）。
  *
  * @param balances 币种 -> 余额
  * @param known    是否收到过全量钱包快照。false 时**只能**回答"见过的这些币各有多少"，
  *                 回答不了"某个没见过的币是不是 0"
  */
final case class WalletState(balances: Map[String, Double], known: Boolean):

  /** 全量快照到达：整表替换，并从此可以把"不在表里"读作 0。 */
  def withWallet(snapshot: Map[String, Double]): WalletState = WalletState(snapshot, known = true)

  /** 单币种余额到达：只动这一个键。 */
  def withBalance(currency: String, amount: Double): WalletState =
    copy(balances = balances.updated(currency, amount))

  /** 该币种余额。**未见过该币且尚未收到全量快照时是 `None`** —— 那时给不出这个数，
    * 而 0 是一个合法的余额取值，用它顶替会让"没有现货"与"还不知道有没有现货"合并成同一件事。 */
  def get(currency: String): Option[Double] =
    balances.get(currency).orElse(Option.when(known)(0.0))

object WalletState:
  val empty: WalletState = WalletState(Map.empty, known = false)
