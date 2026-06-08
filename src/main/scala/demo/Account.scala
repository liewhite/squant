package demo

/** 纯领域对象：银行账户。
  *
  * 只表达业务规则，对“并发 / Actor”一无所知。其可变状态 (`balance`)
  * 的线程安全完全由外层 [[ox.channels.Actor]] 的串行化保证 —— 这里不需要
  * 任何 synchronized / Lock / Atomic。这正是 actor model 的核心价值：
  * 领域逻辑保持纯粹，并发只是基础设施关注点，被隐藏在 Actor 抽象之后。
  */
class Account(val id: String, initialBalance: Long):
  private var balance: Long = initialBalance

  def deposit(amount: Long): Long =
    require(amount > 0, s"存款金额必须为正: $amount")
    balance += amount
    balance

  def withdraw(amount: Long): Long =
    require(amount > 0, s"取款金额必须为正: $amount")
    if amount > balance then throw InsufficientFundsException(id, balance, amount)
    balance -= amount
    balance

  def currentBalance: Long = balance

class InsufficientFundsException(accountId: String, balance: Long, requested: Long)
    extends RuntimeException(s"账户 $accountId 余额不足: 余额=$balance, 请求=$requested")
