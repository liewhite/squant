ThisBuild / scalaVersion := "3.7.4"

lazy val root = (project in file("."))
  .enablePlugins(JmhPlugin)
  .settings(
    name := "ox-actor-demo",
    libraryDependencies += "com.softwaremill.ox" %% "core" % "1.0.4",
    libraryDependencies += "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-core" % "2.35.3",
    libraryDependencies += "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-macros" % "2.35.3" % Provided,
    libraryDependencies ++= Seq(
      "com.softwaremill.sttp.tapir" %% "tapir-nima-server" % "1.13.20",
      "com.softwaremill.sttp.tapir" %% "tapir-jsoniter-scala" % "1.13.20",
      "com.softwaremill.sttp.tapir" %% "tapir-swagger-ui-bundle" % "1.13.20"
    ),
    // ox 基于虚拟线程(Project Loom)，fork 出独立 JVM 运行，输出更干净
    fork := true,
    Compile / mainClass := Some("demo.BankDemo")
  )
