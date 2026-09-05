package hft.dashboard

import hft.actor.{Actor, ActorContext}
import hft.domain.{Instrument, Timestamp, nowMs}
import hft.event.{AnyEvent, Interest}
import io.helidon.webserver.WebServer
import org.slf4j.LoggerFactory
import sttp.tapir.server.nima.NimaServerInterpreter

import com.github.plokhotnyuk.jsoniter_scala.core.writeToString
import java.util.concurrent.atomic.AtomicReference
import BoardView.given

object DashboardActor:
  /** SSE 端点。 */
  val StreamPath: String = "/api/stream"

  /** 两次推送之间的最小间隔 —— 见 `streamHandler` 的"为什么要合并"。 */
  val MinPushIntervalMs: Long = 150

  /** 没有任何变化时也要发一下的间隔 (心跳)。 */
  val HeartbeatMs: Long = 15_000

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
  * @param assetOf 标的 -> **资产代码**: 同一个资产在各家交易所上的行会并成一行, 方便横向比价。
  *                各所的 symbol 串本就不同 (`AAPLUSDT` / `AAPL`), 而"哪些是同一个资产"是
  *                **建立标的宇宙的那个组件**才知道的事 (如 crossspread 的 listings 表) ——
  *                看板去猜等于凭空造一份会猜错的知识, 所以由装配方注入。
  *                默认按 symbol 本身分组: 本就同名的自然合并, 不发明任何东西。
  */
final class DashboardActor(
    port: Int,
    host: String = "127.0.0.1",
    assetOf: Instrument => String = _.symbol,
) extends Actor:
  private val logger = LoggerFactory.getLogger(classOf[DashboardActor])

  /** actor 线程是唯一写者; HTTP 线程只读。 */
  private val snapshot = AtomicReference(BoardSnapshot.empty)

  /** 快照版本号 —— SSE 推送靠它判断"有没有变过", 而不是比对整份快照。
    *
    * 与 `snapshot` 分开一个变量是有意的: 两者不需要原子地一起读。SSE 线程先读版本、再读快照,
    * 中间若又变了, 下一轮立刻会再推一次 —— 多推一次没有代价, 漏推才有。 */
  private val version = java.util.concurrent.atomic.AtomicLong(0L)

  /** 有新快照时叫醒等着的 SSE 线程。 */
  private val changed = Object()

  override def name: String = s"dashboard@$port"

  /** 订阅由 [[BoardSnapshot.topics]] 派生 —— 订阅什么与怎么折叠是同一张表, 加一个 topic
    * 就必须同时给出它的折叠规则, 两份列表因此不可能错开。 */
  override def interests: Set[Interest] = BoardSnapshot.topics.map(Interest.All(_))

  /** 当前视图。`now` 取读取时刻的本地墙钟 —— 年龄要与事件的 `localTs` 同钟相减。 */
  def view(now: Timestamp = nowMs): BoardView = BoardView.of(snapshot.get, now, assetOf)

  override def onStart(ctx: ActorContext): Unit =
    val handler = NimaServerInterpreter().toHandler(DashboardApi(() => view()).all)
    val server = WebServer
      .builder()
      .routing { builder =>
        // SSE 在 tapir 的 catch-all 之前注册 —— 路由按注册顺序匹配。
        // 走原生 Helidon 而不是 tapir: tapir 的 Nima 后端没有流式响应, 而 SSE 的全部意义
        // 就是那条不关闭的流。
        builder.get(DashboardActor.StreamPath, streamHandler)
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

  /** SSE: 有新快照就推一份, 没有就睡着等。
    *
    * ## 为什么要合并
    *
    * 实测三家所全量宇宙下总线上有 ~1000 事件/秒。**一条事件推一次是荒谬的** —— 浏览器渲染不过来,
    * 网络也白烧。所以推完一份之后强制静默 [[DashboardActor.MinPushIntervalMs]], 期间攒下的
    * 变化在下一次推送里一次性带走。看板要的是"看起来是活的", 不是每一条都不漏。
    *
    * ## 为什么不是把轮询调快
    *
    * 轮询在没有变化的时候也照发, 而行情安静的时段 (美股闭市) 恰恰是大多数时候。
    * 事件驱动在那段时间里一个字节都不发, 只留心跳。
    */
  private def streamHandler: io.helidon.webserver.http.Handler =
    (req: io.helidon.webserver.http.ServerRequest, res: io.helidon.webserver.http.ServerResponse) =>
      // Host 白名单同样适用 —— SSE 走的是原生路由, 不经过 tapir 那层校验。
      val hostOk = Option(req.headers().value(io.helidon.http.HeaderNames.HOST).orElse(null))
      if !DashboardApi.isLocalHost(hostOk) then
        res.status(io.helidon.http.Status.FORBIDDEN_403).send("看板只接受来自本机的请求")
      else
        val sink = res.sink(io.helidon.webserver.sse.SseSink.TYPE)
        try
          var seen = -1L
          while true do
            val current = awaitChange(seen, DashboardActor.HeartbeatMs)
            // 版本没变也发 —— 那是心跳: 一条长时间静默的连接会被中间的代理掐掉。
            sink.emit(io.helidon.http.sse.SseEvent.create(writeToString(view())))
            seen = current
            Thread.sleep(DashboardActor.MinPushIntervalMs) // 合并: 推完静默一小段
        catch
          // 浏览器关页面 = 写失败, 这是**正常结束**不是故障。让它穿出去会级联停掉整个引擎。
          case _: Exception => logger.debug("SSE 连接结束")
        finally sink.close()

  /** 纯折叠, 不产出任何事件 —— 看板是观察者, 总线上不该因为它多一条消息。 */
  override def onEvent(event: AnyEvent, now: Timestamp): Vector[AnyEvent] =
    snapshot.updateAndGet(_.apply(event)): Unit
    version.incrementAndGet(): Unit
    // 叫醒 SSE 线程。actor 线程在这里**不会阻塞**: notifyAll 只是把等待者移到就绪队列,
    // 而等待者拿到锁之后要做的第一件事就是把锁放掉。
    changed.synchronized(changed.notifyAll())
    Vector.empty

  /** 阻塞到快照版本超过 `seen`，或超时。返回当前版本。
    *
    * **事件驱动而不是轮询**: 有变化就立刻返回, 没变化就一直睡着 (不烧 CPU)。
    * 超时存在的意义是心跳 —— 一条长时间没有任何事件的连接也要定期发一个字节, 否则中间的
    * 代理会把它当死连接掐掉, 而浏览器那边看起来就是"页面不动了"。
    */
  private[dashboard] def awaitChange(seen: Long, timeoutMs: Long): Long =
    changed.synchronized {
      if version.get <= seen then changed.wait(timeoutMs)
      version.get
    }
