package demo

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

  private val ApiToken = "demo-secret"

  case class AuthedUser(name: String)

  case class ApiError(code: Int, message: String) derives Schema
  case class Greeting(version: String, user: String, message: String) derives Schema

  // jsoniter-scala-macros 是 Provided（仅编译期），所以用 make 显式生成 codec，不能用 derives
  private given JsonValueCodec[ApiError] = JsonCodecMaker.make
  private given JsonValueCodec[Greeting] = JsonCodecMaker.make

  // 鉴权的唯一出处：所有需要鉴权的 endpoint 都从这里派生
  private val secureEndpoint: PartialServerEndpoint[String, AuthedUser, Unit, ApiError, Unit, Any, Identity] =
    endpoint
      .securityIn(auth.apiKey(header[String]("X-Api-Token")).description("访问令牌，demo 固定为 demo-secret"))
      .errorOut(statusCode(StatusCode.Unauthorized).and(jsonBody[ApiError]))
      .serverSecurityLogic[AuthedUser, Identity] { token =>
        if token == ApiToken then Right(AuthedUser("demo-user"))
        else Left(ApiError(401, "invalid X-Api-Token"))
      }

  // 三个业务 endpoint 只写业务逻辑，鉴权由 secureEndpoint 统一提供
  private val v1 = secureEndpoint.get
    .in("api" / "v1")
    .out(jsonBody[Greeting])
    .serverLogicSuccess(user => _ => Greeting("v1", user.name, "hello from v1"))

  private val v2 = secureEndpoint.get
    .in("api" / "v2")
    .out(jsonBody[Greeting])
    .serverLogicSuccess(user => _ => Greeting("v2", user.name, "hello from v2"))

  private val v3 = secureEndpoint.get
    .in("api" / "v3")
    .out(jsonBody[Greeting])
    .serverLogicSuccess(user => _ => Greeting("v3", user.name, "hello from v3"))

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
