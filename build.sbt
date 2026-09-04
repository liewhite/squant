ThisBuild / scalaVersion := "3.7.4"

// 不穷尽的 match 一律当错误。它的失效方式最恶劣: 编译通过、启动通过、契约校验通过,
// 然后在真正走到那个分支的瞬间抛 MatchError —— 例如 Exchange 加一个成员而
// newClientOrderId 忘了跟上, 症状是实盘下第一单时炸, 位置恰好在所有启动校验之后。
// 只提这一类: 全局 -Werror 会被几百条 feature/deprecation 警告淹掉, 反而没人看。
ThisBuild / scalacOptions += "-Wconf:id=E029:e"

// ox 基于虚拟线程(Project Loom)，fork 出独立 JVM 运行，输出更干净
ThisBuild / fork := true

lazy val jsoniter = Seq(
  "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-core" % "2.35.3",
  "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-macros" % "2.35.3" % Provided,
)

/** 交易框架本体。**不含 demo, 也不带 JMH** —— 两者都是旁枝, 见 `demo` 子工程。 */
lazy val root = (project in file("."))
  .settings(
    name := "squant",
    libraryDependencies += "com.softwaremill.ox" %% "core" % "1.0.4",
    libraryDependencies ++= jsoniter,
    // sttp 同步 backend (REST + WebSocket, 虚拟线程友好) + 日志
    libraryDependencies ++= Seq(
      "com.softwaremill.sttp.client4" %% "core" % "4.0.25",
      "org.slf4j" % "slf4j-api" % "2.0.17",
      "org.slf4j" % "slf4j-simple" % "2.0.17",
      "org.scalameta" %% "munit" % "1.1.1" % Test,
    ),
    // tapir + Helidon Nima: 实时看板的 HTTP 面
    libraryDependencies ++= Seq(
      "com.softwaremill.sttp.tapir" %% "tapir-nima-server" % "1.13.20",
      "com.softwaremill.sttp.tapir" %% "tapir-jsoniter-scala" % "1.13.20",
      "com.softwaremill.sttp.tapir" %% "tapir-swagger-ui-bundle" % "1.13.20",
    ),
  )

/** 语言/库特性的演示与 JMH 基准 —— 与交易逻辑无关。
  *
  * 分出来的理由: 从前它们与框架同处一个工程, 于是 `Compile / mainClass` 指着
  * `demo.BankDemo` (一个银行转账演示), 而这是个实盘交易框架; JMH 插件与 tapir 依赖也因此
  * 进了主工程的编译路径。演示代码不该出现在跑实盘的那个 artifact 里。
  */
lazy val demo = (project in file("demo"))
  .enablePlugins(JmhPlugin)
  .dependsOn(root)
  .settings(
    name := "squant-demo",
    publish / skip := true,
    libraryDependencies ++= jsoniter,
    libraryDependencies ++= Seq(
      "com.softwaremill.sttp.tapir" %% "tapir-nima-server" % "1.13.20",
      "com.softwaremill.sttp.tapir" %% "tapir-jsoniter-scala" % "1.13.20",
      "com.softwaremill.sttp.tapir" %% "tapir-swagger-ui-bundle" % "1.13.20",
    ),
  )
