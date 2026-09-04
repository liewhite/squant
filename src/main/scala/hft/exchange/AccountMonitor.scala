package hft.exchange

import hft.actor.{Actor, ActorContext}
import hft.domain.*
import hft.event.{AnyEvent, Event, Topics}
import org.slf4j.LoggerFactory

object AccountMonitor:
  /** 轮询间隔下限。四家的账户接口都有权重限制, 而这里每一拍要拉持仓 + 净值 + 钱包 + 每个标的的
    * 挂单 —— 标的一多就是十几次请求。1 秒是个能站住的下限: 再快对"人看一眼"没有意义,
    * 对限频却是真实压力。 */
  val MinPollMs: Long = 1_000

/** **只读**账户监控 —— 按节拍拉一次账户状态, 原样发到总线。
  *
  * ## 为什么不用柜台
  *
  * [[TradingGateway]] 也能产出这些读数 (启动对齐时发一份), 但它是为**执行**设计的, 拿它来
  * **观察**会撞上三处它自己的前提:
  *
  *   1. **它要求每个标的都有合约规格** (`syncSnapshot` 入口 `symbols.foreach(metaOf)`) ——
  *      那是"我要给它发单"的前提。而账户里完全可能持有没有规格的东西: 币安的 `symbolMetas`
  *      只收 `TRADING` + `PERPETUAL`, 于是一张股票永续 (`AAPLUSDT`) 或季度交割合约
  *      (`BTCUSDT_250926`) 就能让整个只读进程启动失败, 报的还是"无法发单"。
  *   2. **它只管对齐过的标的** (`PositionBook.manages`): 请求里没列的标的, 其回报一律在
  *      入口被丢掉。于是启动之后在手机上新开的仓位**永远不会出现**, 而页面看起来是完整的 ——
  *      一份假账比没有账更坏。
  *   3. **它注册 `OrderIntent` 的处理能力**, 也就是"这个进程能下单"。只读监控不该有这个能力。
  *
  * 本类三条都不占: 不要规格 (它不发单), 不设范围 (账上有什么就报什么), 不注册任何命令处理能力
  * (`commandHandlers` 为空 —— **这是结构保证, 不是约定**: 没有处理者, 下单指令连路由都没有)。
  *
  * ## 它不做的事
  *
  * 不记账、不对账、不管挂单生命周期。它只是把交易所此刻的回答**原样**发出去 ——
  * 那正是"只读"的含义。需要账本与对账的是交易路径, 那是柜台的职责。
  *
  * ## 如实声明的偏差: 它是轮询的
  *
  * 没有私有 WS 推送, 所以成交与仓位变化会在**一个轮询间隔之内**出现, 不是即时。
  * 这个偏差是可见的 —— 看板上每个读数都带年龄, 轮询源的年龄自然在 0 到 `pollMs` 之间摆动。
  * 要即时就得接私有流, 而那是柜台干的事。
  *
  * ## 不能与同账户的柜台共处一个进程
  *
  * 两者都发 [[Topics.Position]]: 柜台发的是**账本**的仓位 (经对账, 权威), 本类发的是 REST 的
  * 读数。同一个 (账户, 交易所) 上两个发布者 = last-write-wins, 策略可能读到本类那份更旧的值
  * 并据此定仓位 —— 危险侧, 且没有症状。
  *
  * 这一条**没有做成结构保证** (框架里没有"我与谁互斥"的声明), 所以写在这里: 只读看板应当
  * 单独一个进程跑。交易进程要看板, 用它自己柜台发的那份数据就够了。
  *
  * @param client  只读地用它 —— 本类只调 fetch*, 一次 placeOrder 都不会发
  * @param account 这些读数属于哪个账户
  * @param symbols **即使空仓也要报一行**的标的。账上实际持有的会自动出现, 不必列在这里;
  *                列在这里的意义是"我在看的就是它", 空仓也要看见那一行
  * @param pollMs  轮询间隔
  */
final class AccountMonitor(
    client: TradingClient,
    account: AccountId,
    symbols: Set[Symbol],
    pollMs: Long,
) extends Actor:
  require(pollMs >= AccountMonitor.MinPollMs, s"轮询间隔 ${pollMs}ms 太快, 下限 ${AccountMonitor.MinPollMs}ms")

  private val logger = LoggerFactory.getLogger(classOf[AccountMonitor])
  private val exchange = client.exchange

  override def name: String = s"account-monitor@$account@$exchange"

  /** **空集合 —— 这是结构保证。** 没有命令处理能力, 下单指令在这个进程里连路由都没有。 */
  override def commandHandlers: Set[hft.event.CommandHandler] = Set.empty

  override def onStart(ctx: ActorContext): Unit =
    // 先拉一次再进等待: 否则页面在第一个间隔内什么都没有, 而"没有"与"还没到点"看起来一样。
    ctx.fork {
      var running = true
      while running do
        pollOnce(ctx)
        running = !ctx.sleepUnlessStopped(pollMs)
    }
    ()

  /** 拉一轮并发布。**失败即抛** —— 拉不到账户状态的看板不是"少一格", 而是在显示一份过期的账,
    * 而页面上那个年龄不会告诉你"其实已经取不到了"。级联停机比静默陈旧诚实。 */
  private def pollOnce(ctx: ActorContext): Unit =
    val positions = orThrow("持仓")(client.fetchPositions())
    val info = orThrow("净值")(client.fetchAccountInfo())
    val wallet = orThrow("钱包")(client.fetchWallet())

    val held = positions.map(_.symbol).toSet
    // 账上持有的 + 点名要看的。点名而空仓的显式推零仓: "空仓"与"这条读数还没来"必须分得开。
    val reported = positions.map(p => p.copy(account = account)) ++
      (symbols -- held).toVector.sorted.map(Position.empty(account, exchange, _))
    reported.foreach(p => ctx.publish(Event.local(Topics.Position, p)))

    ctx.publish(Event.local(Topics.AccountInfo, info.copy(account = account)))
    ctx.publish(Event.local(Topics.Wallet, Wallet(account, exchange, wallet, nowMs)))

    // 挂单按标的查 —— 只查看得见的那些 (持有的 + 点名的)。
    (held ++ symbols).toVector.sorted.foreach { symbol =>
      orThrow(s"$symbol 挂单")(client.fetchPendingOrders(symbol))
        .foreach(o => ctx.publish(Event.local(Topics.OrderUpdate, o.copy(account = account))))
    }

  private def orThrow[A](what: String)(result: Either[ExchangeError, A]): A = result match
    case Right(v) => v
    case Left(e)  => throw IllegalStateException(s"$exchange 拉取${what}失败: ${e.message}")
