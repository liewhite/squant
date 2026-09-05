package hft.dashboard

import sttp.model.StatusCode
import sttp.shared.Identity
import sttp.tapir.*
import sttp.tapir.json.jsoniter.*
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.swagger.bundle.SwaggerInterpreter

import BoardView.given

/** 看板的 HTTP 面 —— endpoint 定义与页面, 不含服务器与线程。
  *
  * 只依赖一个 `() => BoardView`: 这一层不知道数据是从总线折叠出来的还是测试造的。
  *
  * ## 只读
  *
  * 没有任何写接口。看板是观察者, 不是操作台 —— 一个能下单/停机的 HTTP 面意味着交易系统多了
  * 一条不经过总线、不经过 [[hft.actor.ActorSystem]] 生命周期的旁路控制通道。真要做运维接口,
  * 那是另一个组件的事, 且必须走命令总线。
  */
final class DashboardApi(view: () => BoardView):

  /** 只接受指向本机的 `Host` —— 挡 DNS rebinding。
    *
    * 看板无鉴权且只绑回环, 于是"只有本机能访问"是它唯一的防线。但浏览器会替攻击者破掉它:
    * 操作者打开的任意网页, 只要把自己的域名解析到 127.0.0.1, 就能以**同源**读取
    * `/api/board`, 把仓位、挂单、净值整份拿走 —— 无鉴权本机服务的经典泄露路径。
    * 一个 Host 白名单就挡住, 且不影响 SSH 端口转发 (转发之后 Host 仍是 localhost)。
    */
  private def localHost(host: Option[String]): Boolean = DashboardApi.isLocalHost(host)

  private val guarded = endpoint
    .securityIn(header[Option[String]]("Host"))
    .errorOut(statusCode(StatusCode.Forbidden).and(stringBody))
    .serverSecurityLogicPure[Unit, Identity](h =>
      if localHost(h) then Right(()) else Left("看板只接受来自本机的请求 (远程访问请用 SSH 端口转发)")
    )

  private val boardEndpoint: ServerEndpoint[Any, Identity] =
    guarded.get
      .in("api" / "board")
      .out(jsonBody[BoardView])
      .description("各标的的行情、仓位、挂单, 以及各账户的净值与余额。每个读数都带 ageMs")
      .serverLogicSuccess(_ => _ => view())

  private val pageEndpoint: ServerEndpoint[Any, Identity] =
    guarded.get
      .in("")
      .out(htmlBodyUtf8)
      .serverLogicSuccess(_ => _ => DashboardPage.html)

  /** 页面显式给出 favicon 的 204, 免得浏览器每次刷新都在日志里打一条 404。 */
  private val faviconEndpoint: ServerEndpoint[Any, Identity] =
    endpoint.get.in("favicon.ico").out(statusCode(StatusCode.NoContent)).serverLogicSuccess[Identity](_ => ())

  val apiEndpoints: List[ServerEndpoint[Any, Identity]] = List(boardEndpoint)

  val all: List[ServerEndpoint[Any, Identity]] =
    apiEndpoints ++ List(pageEndpoint, faviconEndpoint) ++
      SwaggerInterpreter().fromServerEndpoints[Identity](apiEndpoints, "squant-dashboard", "1.0.0")

object DashboardApi:
  /** Host 是否指向本机。**判据只有这一份** —— SSE 端点走原生 Helidon 路由、不经过 tapir 那层,
    * 两处各写一遍的结果是其中一条路被忘掉, 而那条路照样能读走全部仓位与净值。
    *
    * 缺 Host 头的请求不放行: HTTP/1.1 要求必须带, 缺了说明不是浏览器发的正常请求。 */
  def isLocalHost(host: Option[String]): Boolean =
    host
      .map(_.takeWhile(_ != ':').toLowerCase)
      .exists(h => h == "127.0.0.1" || h == "localhost" || h == "[::1]" || h == "::1")
