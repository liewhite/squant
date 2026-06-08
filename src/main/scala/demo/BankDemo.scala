package demo

import ox.{fork, supervised}
import ox.channels.*

/** ox Actor Model 演示。
  *
  * 演示三件事：
  *   1. `tell`  —— fire-and-forget，发出消息立即返回，不等待结果。
  *   2. `ask`   —— request-response，发出消息并阻塞等待返回值/异常。
  *   3. Actor 间通信 + 高并发串行化 —— 多个虚拟线程并发操作同一账户，
  *      无锁却结果精确，账户 Actor 再把事件 `tell` 给审计 Actor。
  */
@main def BankDemo(): Unit =
  supervised:
    // 每个 Actor 封装一个可变领域对象，对它的所有调用都被串行化。
    val auditRef: ActorRef[AuditLog] = Actor.create(new AuditLog)
    val accountRef: ActorRef[Account] = Actor.create(new Account("ACC-001", 1000))

    println(s"初始余额: ${accountRef.ask(_.currentBalance)}")

    // === 1. tell：fire-and-forget ===
    // 在账户 Actor 内部完成存款后，再用 tell 把事件异步推给审计 Actor。
    accountRef.tell { acc =>
      val bal = acc.deposit(500)
      auditRef.tell(_.record(s"存款 +500, 余额=$bal"))
    }

    // === 2. ask：request-response，并演示异常会传播回调用方 ===
    val afterWithdraw = accountRef.ask(_.withdraw(200))
    auditRef.tell(_.record(s"取款 -200, 余额=$afterWithdraw"))
    println(s"存500取200后余额: $afterWithdraw")

    try accountRef.ask(_.withdraw(1_000_000))
    catch case e: InsufficientFundsException => println(s"捕获到预期异常: ${e.getMessage}")

    // === 3. 高并发串行化：5 个虚拟线程，各对同一账户做 200 次 +1 存款 ===
    // 没有任何锁，Actor 保证串行执行 => 1000 次操作不丢不错，结果精确可预测。
    val balanceBefore = accountRef.ask(_.currentBalance)
    val workers = (1 to 5).map { _ =>
      fork {
        (1 to 200).foreach { _ =>
          val bal = accountRef.ask(_.deposit(1))
          auditRef.tell(_.record(s"并发存款 +1, 余额=$bal"))
        }
      }
    }
    workers.foreach(_.join())

    val balanceAfter = accountRef.ask(_.currentBalance)
    println(s"并发 1000 次 +1 后余额: $balanceBefore -> $balanceAfter (期望 +1000)")
    assert(balanceAfter == balanceBefore + 1000, "串行化失败：余额不符合预期！")

    // 审计日志条数 = 1(存) + 1(取) + 1000(并发) = 1002
    println(s"审计日志条数: ${auditRef.ask(_.size)} (期望 1002)")
    println("演示完成：无锁，零竞态。")
