package demo

import java.net.InetSocketAddress

import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec
import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker
import io.helidon.webserver.WebServer
import sttp.model.StatusCode
import sttp.shared.Identity
import sttp.tapir.*
import sttp.tapir.json.jsoniter.*
import sttp.tapir.server.{PartialServerEndpoint, ServerEndpoint}
import sttp.tapir.server.nima.NimaServerInterpreter
import sttp.tapir.swagger.bundle.SwaggerInterpreter

/** tapir + Helidon Nima 演示：三层树状 endpoint 结构 + 模块化 security middleware。
  *
  * {{{
  * apiRoot (/api)                      ← 第一层：纯 Endpoint 树根
  * ├── adminBranch (/api/admin)        ← 第二层：挂 ApiTokenAuth + ClientIp 两个 middleware
  * │   ├── GET /api/admin/v1..v3       ← 第三层：叶子，serverLogic 入参是 RequestContext
  * └── userBranch (/api/user)          ← 第二层：公开分支，不挂 security
  *     └── GET /api/user/v1..v3        ← 第三层：叶子，无鉴权
  * }}}
  *
  * endpoint 描述是不可变值，每个中间 val 都是可继续分叉的树节点；
  * serverSecurityLogic 在每条根→叶路径上只能出现一次，security 由派生源头静态决定。
  *
  * 运行：sbt "runMain demo.TapirAuthDemo"
  * 文档：http://localhost:8080/docs
  * 验证：curl http://localhost:8080/api/user/v1
  *      curl -H "X-Api-Token: demo-secret" http://localhost:8080/api/admin/v1
  */
object TapirAuthDemo:

  case class AuthedUser(name: String)

  /** security 阶段产出的请求上下文：身份 + 客户端 IP，业务 endpoint 的入参类型 */
  case class RequestContext(user: AuthedUser, clientIp: String)

  case class ApiError(code: Int, message: String) derives Schema
  case class Greeting(version: String, user: String, clientIp: String, message: String) derives Schema

  // jsoniter-scala-macros 是 Provided（仅编译期），所以用 make 显式生成 codec，不能用 derives
  private given JsonValueCodec[ApiError] = JsonCodecMaker.make
  private given JsonValueCodec[Greeting] = JsonCodecMaker.make

  // ── middleware 1：API token 鉴权。input 声明（含文档）+ 校验逻辑自包含，可单独复用 ──
  private object ApiTokenAuth:
    private val ApiToken = "demo-secret"

    val input: EndpointInput[String] =
      auth.apiKey(header[String]("X-Api-Token")).description("访问令牌，demo 固定为 demo-secret")

    def check(token: String): Either[ApiError, AuthedUser] =
      if token == ApiToken then Right(AuthedUser("demo-user"))
      else Left(ApiError(401, "invalid X-Api-Token"))

  // ── middleware 2：client IP 提取。mapTo[Source] 把内部输入结构封在模块内，对外只暴露单一类型 ──
  private object ClientIp:
    case class Source(xff: Option[String], remote: Option[InetSocketAddress])

    // X-Forwarded-For 是普通 header input，进 OpenAPI 文档；
    // extractFromRequest 是 server-only input，文档解释器自动忽略，直连时兜底
    val input: EndpointInput[Source] =
      header[Option[String]]("X-Forwarded-For")
        .description("代理传递的客户端 IP 链，取第一个")
        .and(extractFromRequest(_.connectionInfo.remote))
        .mapTo[Source]

    def resolve(s: Source): String =
      s.xff
        .flatMap(_.split(',').headOption.map(_.trim).filter(_.nonEmpty))
        .orElse(s.remote.map(_.getAddress.getHostAddress))
        .getOrElse("unknown")

  // ── 第一层：公共前缀 /api，纯 Endpoint 值，整棵树的根 ──
  private val apiRoot = endpoint.in("api")

  // ── 第二层 admin 分支：组装两个 middleware（serverSecurityLogic 每条路径只能调一次），再长出 /admin ──
  private val adminBranch: PartialServerEndpoint[(String, ClientIp.Source), RequestContext, Unit, ApiError, Unit, Any, Identity] =
    apiRoot
      .securityIn(ApiTokenAuth.input)
      .securityIn(ClientIp.input)
      .errorOut(statusCode(StatusCode.Unauthorized).and(jsonBody[ApiError]))
      .serverSecurityLogic[RequestContext, Identity] { (token, ipSource) =>
        ApiTokenAuth.check(token).map(RequestContext(_, ClientIp.resolve(ipSource)))
      }
      .in("admin")

  // ── 第二层 user 分支：公开，不挂 security，文档中也没有 security 标记 ──
  private val userBranch = apiRoot.in("user")

  private val versions = List("v1", "v2", "v3")

  // ── 第三层：每个分支各长出 v1/v2/v3 三个叶子 ──
  private val adminEndpoints: List[ServerEndpoint[Any, Identity]] = versions.map { v =>
    adminBranch.get
      .in(v)
      .out(jsonBody[Greeting])
      .serverLogicSuccess(ctx => _ => Greeting(v, ctx.user.name, ctx.clientIp, s"hello from admin $v"))
  }

  private val userEndpoints: List[ServerEndpoint[Any, Identity]] = versions.map { v =>
    userBranch.get
      .in(v)
      .out(stringBody)
      .serverLogicSuccess[Identity](_ => s"hello from user $v")
  }

  private val apiEndpoints: List[ServerEndpoint[Any, Identity]] = adminEndpoints ++ userEndpoints

  private val docsEndpoints =
    SwaggerInterpreter().fromServerEndpoints[Identity](apiEndpoints, "tapir-nima-auth-demo", "1.0.0")

  def main(args: Array[String]): Unit =
    val handler = NimaServerInterpreter().toHandler(apiEndpoints ++ docsEndpoints)
    val server = WebServer
      .builder()
      .routing { builder =>
        builder.any(handler)
        ()
      }
      .port(8080)
      .build()
      .start()
    println(s"server started at http://localhost:${server.port}")
    println(s"swagger docs at http://localhost:${server.port}/docs")
