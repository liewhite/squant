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

/** tapir + Helidon Nima 演示：header 鉴权逻辑只写一次，v1/v2/v3 三个 endpoint 复用。
  *
  * 关键点是 [[secureEndpoint]]：securityIn 声明了鉴权 header（会自动出现在 OpenAPI 文档里），
  * serverSecurityLogic 完成校验并产出 AuthedUser。业务 endpoint 从它派生，
  * serverLogic 的入参类型就是 AuthedUser —— 不鉴权拿不到 user，编译期保证不会漏。
  *
  * 运行：sbt "runMain demo.TapirAuthDemo"
  * 文档：http://localhost:8080/docs
  * 验证：curl -H "X-Api-Token: demo-secret" http://localhost:8080/api/v1
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

  // ── 组装点：serverSecurityLogic 每条派生路径只能调一次，两个 middleware 在此拼成 RequestContext ──
  private val secureEndpoint: PartialServerEndpoint[(String, ClientIp.Source), RequestContext, Unit, ApiError, Unit, Any, Identity] =
    endpoint
      .securityIn(ApiTokenAuth.input)
      .securityIn(ClientIp.input)
      .errorOut(statusCode(StatusCode.Unauthorized).and(jsonBody[ApiError]))
      .serverSecurityLogic[RequestContext, Identity] { (token, ipSource) =>
        ApiTokenAuth.check(token).map(RequestContext(_, ClientIp.resolve(ipSource)))
      }

  // v1 是公开 endpoint：不从 secureEndpoint 派生就不带鉴权，文档中也没有 security 标记
  private val v1 = endpoint.get
    .in("api" / "v1")
    .out(stringBody)
    .serverLogicSuccess[Identity](_ => "hello")

  // v2/v3 只写业务逻辑，鉴权和上下文提取由 secureEndpoint 统一提供
  private val v2 = secureEndpoint.get
    .in("api" / "v2")
    .out(jsonBody[Greeting])
    .serverLogicSuccess(ctx => _ => Greeting("v2", ctx.user.name, ctx.clientIp, "hello from v2"))

  private val v3 = secureEndpoint.get
    .in("api" / "v3")
    .out(jsonBody[Greeting])
    .serverLogicSuccess(ctx => _ => Greeting("v3", ctx.user.name, ctx.clientIp, "hello from v3"))

  private val apiEndpoints: List[ServerEndpoint[Any, Identity]] = List(v1, v2, v3)

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
