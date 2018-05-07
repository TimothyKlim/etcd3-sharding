name := "etcd3-sharding-ring"

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
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream-kafka" % "0.20",
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "org.scala-lang.modules" %% "scala-java8-compat" % "0.9.0",
)

scalafmtOnCompile in ThisBuild := true
