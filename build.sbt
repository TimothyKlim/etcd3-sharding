import com.typesafe.sbt.packager.docker.{Cmd, ExecCmd}
import com.typesafe.sbt.packager.docker.DockerPlugin.autoImport.dockerCommands

name := "etcd3-sharding"

scalaVersion := "2.12.6"

val akkaVersion = "2.5.12"

resolvers := Seq(
  Resolver.jcenterRepo,
  Resolver.sonatypeRepo("releases")
)

libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.coreos" % "jetcd-core" % "0.0.2",
  "com.github.pureconfig" %% "pureconfig" % "0.9.1",
  "com.github.pureconfig" %% "pureconfig" % "0.9.1",
  "com.lihaoyi" %% "upickle" % "0.6.5",
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream-kafka" % "0.20",
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
  "org.scala-lang.modules" %% "scala-java8-compat" % "0.9.0",
  "org.typelevel" %% "cats-core" % "1.1.0",
)

scalafmtOnCompile in ThisBuild := true

scalacOptions ++= Seq(
  "-deprecation",
  "-encoding",
  "utf-8",
  "-explaintypes",
  "-feature",
  "-language:_",
  "-opt-warnings:_",
  "-opt:l:inline",
  "-unchecked",
  "-Xfuture",
  "-Xlint:_",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-extra-implicit",
  "-Ywarn-inaccessible",
  "-Ywarn-infer-any",
  "-Ywarn-nullary-override",
  "-Ywarn-nullary-unit",
  "-Ywarn-numeric-widen",
  "-Ywarn-unused:_",
  "-Ywarn-value-discard"
)

enablePlugins(DockerPlugin, JavaAppPackaging)

val javaRunOptions = Seq(
  "-server",
  "-Xms1g",
  "-Xmx1g",
  "-Xss4m",
  "-XX:+AlwaysPreTouch",
  "-XX:+DisableExplicitGC",
  "-XX:+TieredCompilation",
  "-XX:+UnlockDiagnosticVMOptions",
  "-XX:+UnlockExperimentalVMOptions",
  "-XX:+EnableJVMCI",
  "-XX:+UseJVMCICompiler",
  "-XX:+UseG1GC",
  "-XX:+UseStringDeduplication",
  "-XX:-UseBiasedLocking",
  "-XX:InitiatingHeapOccupancyPercent=15",
  "-XX:MaxGCPauseMillis=100",
  "-XX:NewSize=256m",
  "-XX:ReservedCodeCacheSize=256m",
)

executableScriptName := "app"
packageName in Universal := "app"
scriptClasspath ~= (cp => "../config" +: cp)
javaOptions in (Test, run) ++= javaRunOptions
dockerCommands := {
  val targetDir = "/app"
  val dockerUser = "app"
  val entrypoint = s"$targetDir/bin/${executableScriptName.value}"
  Seq(
    Cmd("FROM", "openjdk:11-jre-slim"),
    Cmd("COPY", "opt/docker", targetDir),
    ExecCmd("RUN", "chmod", "+x", entrypoint)
  )
}
