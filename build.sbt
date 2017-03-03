name := """activator-kafka-scala-producer-consumer"""

version := "1.0"

scalaVersion := "2.12.1"

val kafkaStreamsVersion = "0.10.1.1"

libraryDependencies ++= Seq(
  "org.scala-lang.modules" %% "scala-xml" % "1.0.6",
  "org.apache.kafka" %% "kafka" % "0.10.1.1",
//  "org.apache.commons" % "commons-lang3" % "3.5",
  "com.typesafe.akka" %% "akka-actor" % "2.4.17",
  "com.typesafe.akka" %% "akka-stream" % "2.4.17",
  "com.typesafe.akka" %% "akka-stream-kafka" % "0.13",
//  "com.typesafe.akka" %% "akka-http-core" % "10.0.3",
//  "com.typesafe.akka" %% "akka-http" % "10.0.3",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.8.7",
  "org.apache.kafka" % "kafka-streams" % kafkaStreamsVersion
)


