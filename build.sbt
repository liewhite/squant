ThisBuild / scalaVersion := "3.7.4"

lazy val root = (project in file("."))
  .enablePlugins(JmhPlugin)
  .settings(
    name := "ox-actor-demo",
    // 不穷尽的 match 一律当错误。它的失效方式最恶劣: 编译通过、启动通过、契约校验通过,
    // 然后在真正走到那个分支的瞬间抛 MatchError —— 例如 Exchange 加一个成员而
    // newClientOrderId 忘了跟上, 症状是实盘下第一单时炸, 位置恰好在所有启动校验之后。
    // 只提这一类: 全局 -Werror 会被几百条 feature/deprecation 警告淹掉, 反而没人看。
    scalacOptions += "-Wconf:id=E029:e",
    libraryDependencies += "com.softwaremill.ox" %% "core" % "1.0.4",
    libraryDependencies += "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-core" % "2.35.3",
    libraryDependencies += "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-macros" % "2.35.3" % Provided,
    libraryDependencies ++= Seq(
      "com.softwaremill.sttp.tapir" %% "tapir-nima-server" % "1.13.20",
      "com.softwaremill.sttp.tapir" %% "tapir-jsoniter-scala" % "1.13.20",
      "com.softwaremill.sttp.tapir" %% "tapir-swagger-ui-bundle" % "1.13.20"
    ),
    // hft 框架: sttp 同步 backend (REST + WebSocket, 虚拟线程友好) + 日志
    libraryDependencies ++= Seq(
      "com.softwaremill.sttp.client4" %% "core" % "4.0.25",
      "org.slf4j" % "slf4j-api" % "2.0.17",
      "org.slf4j" % "slf4j-simple" % "2.0.17",
      "org.scalameta" %% "munit" % "1.1.1" % Test
    ),
    // ox 基于虚拟线程(Project Loom)，fork 出独立 JVM 运行，输出更干净
    fork := true,
    Compile / mainClass := Some("demo.BankDemo")
  )
