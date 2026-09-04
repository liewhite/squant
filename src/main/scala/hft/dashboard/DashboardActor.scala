package hft.dashboard

import hft.actor.{Actor, ActorContext}
import hft.domain.{Timestamp, nowMs}
import hft.event.{AnyEvent, Interest}
import io.helidon.webserver.WebServer
import org.slf4j.LoggerFactory
import sttp.tapir.server.nima.NimaServerInterpreter

import java.util.concurrent.atomic.AtomicReference

/** 实时看板 —— 总线上的一个**只读观察者**, 外加一个 HTTP 面。
  *
  * {{{
  *   总线 ──> onEvent (actor 线程, 唯一写者) ──> AtomicReference[BoardSnapshot]
  *                                                      │ 读
  *                                          HTTP 线程 ──┘──> BoardView ──> JSON / 页面
  * }}}
  *
  * ## 线程模型: 一次引用替换, 没有别的
  *
  * 快照是不可变的, actor 线程算出新的一份就整体换上去。HTTP 线程 (Helidon Nima 自己的线程池)
  * 只读那个引用, 拿到的必然是某一个完整时刻的一致视图 —— 不会读到"仓位已更新、挂单还没更新"
  * 这种中间态。
  *
  * **不去读 [[hft.state.StateManager]]**: 那份状态是可变的、只归策略执行器的 actor 线程所有,
  * 从 HTTP 线程读它是数据竞争; 而且它按策略实例建, 一个进程里有几个策略就有几份, 每份只看得见
  * 自己订阅的标的。代价是这里有第二份"从事件重建状态"的代码 (见 [[BoardSnapshot]] 对这笔代价的
  * 说明), 收益是看板与交易路径完全隔离: 它算错了不会有一分钱因此下错单。
  *
  * ## 失败语义
  *
  * 端口占用在 [[onStart]] 抛 —— 那是装配期的配置错误, 与框架其余部分一致 (启动时大声失败,
  * 而不是让引擎带着一个打不开的看板跑起来)。**这确实意味着看板起不来就不能交易**: 这是有意的,
  * 因为端口冲突通常说明上一个进程还活着, 而两个进程同时管同一批仓位比没有看板危险得多。
  *
  * 运行期 HTTP 请求里的异常由 Nima 处理, 不会回到 actor 线程 —— actor 线程上只跑
  * [[BoardSnapshot.apply]] 那个纯折叠。
  *
  * @param port  监听端口
  * @param host  绑定地址。默认只绑回环: 看板**没有任何鉴权**, 而它把仓位、挂单、净值全都
  *              摊开在页面上。要从别的机器看, 请走 SSH 端口转发, 不要图省事绑 0.0.0.0。
  */
final class DashboardActor(port: Int, host: String = "127.0.0.1") extends Actor:
  private val logger = LoggerFactory.getLogger(classOf[DashboardActor])

  /** actor 线程是唯一写者; HTTP 线程只读。 */
  private val snapshot = AtomicReference(BoardSnapshot.empty)

  override def name: String = s"dashboard@$port"

  /** 订阅由 [[BoardSnapshot.topics]] 派生 —— 订阅什么与怎么折叠是同一张表, 加一个 topic
    * 就必须同时给出它的折叠规则, 两份列表因此不可能错开。 */
  override def interests: Set[Interest] = BoardSnapshot.topics.map(Interest.All(_))

  /** 当前视图。`now` 取读取时刻的本地墙钟 —— 年龄要与事件的 `localTs` 同钟相减。 */
  def view(now: Timestamp = nowMs): BoardView = BoardView.of(snapshot.get, now)

  override def onStart(ctx: ActorContext): Unit =
    val handler = NimaServerInterpreter().toHandler(DashboardApi(() => view()).all)
    val server = WebServer
      .builder()
      .routing { builder =>
        builder.any(handler)
        ()
      }
      .host(host)
      .port(port)
      // **关掉 Helidon 自己的 JVM shutdown hook** (默认开)。开着的话 SIGINT 会让它抢在
      // ActorSystem 的有序停机之前把服务器停掉, 而那段收尾窗口 (撤单、排空邮箱, 最长 30s)
      // 恰恰是最想看看板的时刻。生命周期只归 ctx.manage 一处。
      .shutdownHook(false)
      .build()
    // 先登记再启动: manage 保证组件停止时它一定被关掉, 哪怕后面的启动步骤抛异常。
    ctx.manage(server)(s => s.stop(): Unit)
    server.start()
    // **必须自己确认它真的起来了。** Helidon 的 `start()` 在绑定失败时 (端口被占) 不抛,
    // 只在它自己的 logger 上打一条 ERROR 然后把服务器停掉 —— `start()` 正常返回,
    // `port` 变成 -1。照单全收的话, 装配继续往下走, 日志里还会印出
    // "看板已启动: http://127.0.0.1:-1", 而实盘就这么带着一个根本不存在的看板跑起来了。
    // 这正是本仓库反复在修的那类事: 上游的失败被翻译成一个看着正常的值。
    if !server.isRunning || server.port <= 0 then
      throw IllegalStateException(
        s"看板 HTTP 服务未能绑定 $host:$port (running=${server.isRunning} port=${server.port}) —— " +
          "端口很可能被占用; 若是上一个进程还活着, 那比没有看板危险得多, 先确认它已退出"
      )
    logger.info(s"看板已启动: http://$host:${server.port}  (API: /api/board, 文档: /docs)")

  /** 纯折叠, 不产出任何事件 —— 看板是观察者, 总线上不该因为它多一条消息。 */
  override def onEvent(event: AnyEvent, now: Timestamp): Vector[AnyEvent] =
    snapshot.updateAndGet(_.apply(event)): Unit
    Vector.empty
