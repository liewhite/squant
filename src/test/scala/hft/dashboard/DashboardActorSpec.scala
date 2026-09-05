package hft.dashboard

import hft.actor.{Actor, ActorContext}
import hft.domain.*
import hft.engine.Engine
import hft.event.AnyEvent
import hft.event.{Event, Topics}
import hft.TestUnits.given
import ox.supervised
import sttp.client4.*
import sttp.model.Uri

import java.net.ServerSocket

/** 端到端: 把看板装进引擎、往总线发事件、用真的 HTTP 请求拉一次。
  *
  * 这条用例要证的是把三段接起来之后仍然成立的事 —— 订阅声明覆盖了该覆盖的 topic、
  * actor 线程写 / HTTP 线程读的那次引用替换、以及组件停止时端口确实被释放。
  * 折叠与投影本身在 [[BoardSnapshotSpec]] / [[BoardViewSpec]] 里单测。
  */
class DashboardActorSpec extends munit.FunSuite:
  private val ex = Exchange.Binance
  private val sym = "BTCUSDT"
  private val t0 = 1_700_000_000_000L

  /** 借一个当下空闲的端口。写死端口会让并行跑测试时互相撞。 */
  private def freePort(): Int =
    val s = ServerSocket(0)
    try s.getLocalPort
    finally s.close()

  private def get(port: Int, path: String): (Int, String) =
    val backend = DefaultSyncBackend()
    try
      val url = Uri.unsafeParse(s"http://127.0.0.1:$port$path")
      val r = basicRequest.get(url).response(asStringAlways).send(backend)
      (r.code.code, r.body)
    finally backend.close()

  /** 往总线上发事件的最小插件 —— 引擎自己不发事件, 事件都来自插件。 */
  private final class Injector extends Actor:
    @volatile private var ctx: ActorContext = scala.compiletime.uninitialized
    override def name: String = "test-injector"
    override def onStart(c: ActorContext): Unit = ctx = c
    def publish(ev: AnyEvent): Unit = ctx.publish(ev)

  private def eventually(what: String)(cond: => Boolean): Unit =
    val deadline = System.currentTimeMillis() + 3000
    while !cond && System.currentTimeMillis() < deadline do Thread.sleep(20)
    assert(cond, s"等待超时: $what")

  test("装进引擎后, 总线上的行情与仓位经 HTTP 读得到"):
    val port = freePort()
    supervised:
      Engine.run(plugins = Vector.empty) { engine =>
        val board = DashboardActor(port)
        engine.install(board)
        val injector = Injector()
        engine.install(injector)

        val (code, body) = get(port, "/api/board")
        assertEquals(code, 200)
        assert(body.contains("\"eventsApplied\":0"), s"还没发事件, 应是 0: $body")

        injector.publish(Event.stamped(Topics.Bbo, BBO(ex, sym, 49999.0, Coin(1.0), 50001.0, Coin(2.0), t0), t0, t0))
        injector.publish(Event.stamped(Topics.Position, Position(AccountId.Live, ex, sym, Coin(0.5)), t0, t0))

        eventually("看板收到两条事件")(board.view().eventsApplied >= 2)

        val (code2, json) = get(port, "/api/board")
        assertEquals(code2, 200)
        assert(json.contains("\"symbol\":\"BTCUSDT\""), json)
        assert(json.contains("\"mid\":50000.0"), json)
        assert(json.contains("\"account\":\"live\""), json)

        engine.requestShutdown("测试结束")
      }

  test("页面自包含, 不引任何外部域名 —— 交易机可能没有外网"):
    // 页面依赖 CDN 意味着"行情断了想看看板"的时候看板自己也打不开。
    val port = freePort()
    supervised:
      Engine.run(plugins = Vector.empty) { engine =>
        engine.install(DashboardActor(port))
        val (code, html) = get(port, "/")
        assertEquals(code, 200)
        assert(html.contains("squant 看板"), html.take(200))
        assert(!html.contains("http://") && !html.contains("https://"), "页面不该引用任何外部地址")
        assert(html.contains("EventSource('/api/stream')"), "页面应走 SSE 推送而不是轮询")
        engine.requestShutdown("测试结束")
      }

  test("组件停止后端口被释放 —— 资源随生命周期走, 不靠 JVM 退出兜底"):
    val port = freePort()
    supervised:
      Engine.run(plugins = Vector.empty) { engine =>
        engine.install(DashboardActor(port))
        assertEquals(get(port, "/api/board")._1, 200)
        engine.requestShutdown("测试结束")
      }
    // 端口能重新绑上 = 上一个真的关了
    val reopened = ServerSocket(port)
    reopened.close()

  test("端口被占 -> 装配期抛错, 而不是报告一个绑在 -1 上的'看板已启动'"):
    // Helidon 的 start() 在绑定失败时**不抛**: 它只在自己的 logger 上打一条 ERROR、把服务器
    // 停掉, 然后正常返回, port 变成 -1。照单全收的话, 引擎会带着一个根本不存在的看板跑起来,
    // 日志里还印着 "看板已启动: http://127.0.0.1:-1" —— 上游的失败被翻译成一个看着正常的值。
    // 占位者必须绑**同一个地址**: ServerSocket(0) 绑的是通配 0.0.0.0, 而看板绑 127.0.0.1,
    // 在 macOS 上这两者并不冲突 —— 用它做占位, 测的就不是想测的东西了。
    val taken = freePort()
    val squatter = ServerSocket()
    try
      squatter.bind(java.net.InetSocketAddress("127.0.0.1", taken))
      val e = intercept[Exception] {
        supervised:
          Engine.run(plugins = Vector.empty) { engine =>
            engine.install(DashboardActor(taken))
            engine.requestShutdown("不该走到这里")
          }
      }
      val msg = Iterator.iterate[Throwable](e)(_.getCause).takeWhile(_ != null).map(_.getMessage).mkString(" | ")
      assert(msg.contains("未能绑定"), msg)
    finally squatter.close()

  test("非本机 Host 被拒 —— 挡 DNS rebinding"):
    // 看板无鉴权、只绑回环, "只有本机能访问"是它唯一的防线。但操作者打开的任意网页只要把
    // 自己的域名解析到 127.0.0.1, 就能以同源读走仓位/挂单/净值。Host 白名单挡住这条路,
    // 且不影响 SSH 端口转发 (转发后 Host 仍是 localhost)。
    val port = freePort()
    supervised:
      Engine.run(plugins = Vector.empty) { engine =>
        engine.install(DashboardActor(port))
        // 裸 socket 手写请求: JDK 的 HttpClient 不允许覆盖 Host 头, 而这里要测的恰恰是
        // "别人用什么 Host 打过来" —— 那正是浏览器在 DNS rebinding 里会做的事。
        def statusWithHost(h: String): Int =
          val sock = java.net.Socket("127.0.0.1", port)
          try
            sock.getOutputStream.write(s"GET /api/board HTTP/1.1\r\nHost: $h\r\nConnection: close\r\n\r\n".getBytes("UTF-8"))
            sock.getOutputStream.flush()
            val line = java.io.BufferedReader(java.io.InputStreamReader(sock.getInputStream)).readLine()
            line.split(" ")(1).toInt
          finally sock.close()

        assertEquals(statusWithHost(s"127.0.0.1:$port"), 200, "本机 Host 放行")
        assertEquals(statusWithHost(s"localhost:$port"), 200, "localhost 放行 (SSH 转发后就是它)")
        assertEquals(statusWithHost("evil.example.com"), 403, "指向本机的恶意域名必须被拒")
        engine.requestShutdown("测试结束")
      }

  test("突发事件不积压 —— 折叠跟得上总线"):
    // 看板是总线上扇出最大的订阅者 (全量订阅)。邮箱无界是总线契约, 所以"跟不上"的表现不是
    // 丢事件而是积压; 这里确认一次 5 万条的突发之后邮箱能排空。
    val port = freePort()
    supervised:
      Engine.run(plugins = Vector.empty) { engine =>
        val board = DashboardActor(port)
        engine.install(board)
        val injector = Injector()
        engine.install(injector)
        val n = 50_000
        for i <- 0 until n do
          injector.publish(Event.stamped(Topics.Bbo, BBO(ex, sym, 100.0 + i % 10, Coin(1.0), 101.0 + i % 10, Coin(1.0), t0 + i), t0 + i, t0 + i))
        eventually(s"折叠完 $n 条")(board.view().eventsApplied >= n)
        engine.requestShutdown("测试结束")
      }

  test("SSE: 有新事件就推, 没有就不推 —— 事件驱动而不是把轮询调快"):
    val port = freePort()
    supervised:
      Engine.run(plugins = Vector.empty) { engine =>
        val board = DashboardActor(port)
        engine.install(board)
        val injector = Injector()
        engine.install(injector)

        val sock = java.net.Socket("127.0.0.1", port)
        try
          sock.getOutputStream.write(
            s"GET /api/stream HTTP/1.1\r\nHost: localhost:$port\r\nAccept: text/event-stream\r\nConnection: keep-alive\r\n\r\n".getBytes("UTF-8"))
          sock.getOutputStream.flush()
          sock.setSoTimeout(5000)
          val in = java.io.BufferedReader(java.io.InputStreamReader(sock.getInputStream, "UTF-8"))
          assert(in.readLine().contains("200"), "SSE 应返回 200")

          injector.publish(Event.stamped(Topics.Bbo, BBO(ex, sym, 49999.0, Coin(1.0), 50001.0, Coin(2.0), t0), t0, t0))

          // 连上就先推一份当前状态 (否则页面在第一条事件到来前是空白), 所以这里要一直读到
          // 带上那条 BBO 的那一帧。
          var payload = ""
          val deadline = System.currentTimeMillis() + 5000
          while !payload.contains("BTCUSDT") && System.currentTimeMillis() < deadline do
            val line = in.readLine()
            if line != null && line.startsWith("data:") then payload = line.drop(5).trim
          assert(payload.contains("\"symbol\":\"BTCUSDT\""), s"推送的应是完整视图: ${payload.take(200)}")
          assert(payload.contains("\"mid\":50000.0"), payload.take(300))
        finally sock.close()
        engine.requestShutdown("测试结束")
      }

  test("SSE 也受 Host 白名单管 —— 它走原生路由, 不经过 tapir 那层"):
    // 两处各写一遍判据的结果是其中一条路被忘掉, 而那条路照样能读走全部仓位与净值。
    val port = freePort()
    supervised:
      Engine.run(plugins = Vector.empty) { engine =>
        engine.install(DashboardActor(port))
        val sock = java.net.Socket("127.0.0.1", port)
        try
          sock.getOutputStream.write(
            s"GET /api/stream HTTP/1.1\r\nHost: evil.example.com\r\nConnection: close\r\n\r\n".getBytes("UTF-8"))
          sock.getOutputStream.flush()
          sock.setSoTimeout(5000)
          val status = java.io.BufferedReader(java.io.InputStreamReader(sock.getInputStream)).readLine()
          assert(status.contains("403"), s"非本机 Host 应被拒: $status")
        finally sock.close()
        engine.requestShutdown("测试结束")
      }
