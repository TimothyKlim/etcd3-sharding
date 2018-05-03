name := "etcd3-ring"

scalaVersion := "2.12.6"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.5.12",
  "com.typesafe.akka" %% "akka-stream" % "2.5.12",
  "com.typesafe.akka" %% "akka-slf4j" % "2.5.12",
  "com.typesafe.akka" %% "akka-stream-kafka" % "0.20",
  "com.coreos" % "jetcd-core" % "0.0.2",
  "ch.qos.logback" % "logback-classic" % "1.2.3"
)
