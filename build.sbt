ThisBuild / scalaVersion := "3.7.4"

lazy val root = (project in file("."))
  .enablePlugins(JmhPlugin)
  .settings(
    name := "ox-actor-demo",
    libraryDependencies += "com.softwaremill.ox" %% "core" % "1.0.4",
    // ox 基于虚拟线程(Project Loom)，fork 出独立 JVM 运行，输出更干净
    fork := true,
    Compile / mainClass := Some("demo.BankDemo")
  )
