name := """activator-kafka-scala-producer-consumer"""

version := "0.1"

organization := "nl.trivento.kkessels"

scalaVersion in ThisBuild := "2.12.1"

val scalaXmlVersion = "1.0.6"

val jacksonModuleScalaVersion = "2.8.7"

val akkaActorVersion = "2.4.16"
val akkaStreamKafka = "0.13"
val akkaHttpCoreVersion = "10.0.3"
val akkaHttpVersion = "10.0.3"

val kafkaVersion = "0.10.1.1"
val kafkaStreamsVersion = "0.10.1.1"

val sparkVersion = "2.1.0"
val sparkCoreVersion = "2.1.0"
val sparkSqlVersion = "2.1.0"
val sparkStreamingVersion = "2.1.0"
val sparkStreamingKafkaVersion = "2.1.0"

val dispatchVersion = "0.12.0"

lazy val commonSettings = Seq(
  libraryDependencies ++= Seq(
    "org.scala-lang.modules" %% "scala-xml" % scalaXmlVersion,
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonModuleScalaVersion
  )
)

lazy val root = (project in file("."))
  .enablePlugins(ScalaxbPlugin)
  .settings(
    commonSettings,
    libraryDependencies ++= Seq(
      "org.apache.kafka" %% "kafka" % kafkaVersion,
      "com.typesafe.akka" %% "akka-actor" % akkaActorVersion,
      "com.typesafe.akka" %% "akka-stream-kafka" % akkaStreamKafka,
      "com.typesafe.akka" %% "akka-http-core" % akkaHttpCoreVersion,
      "com.typesafe.akka" %% "akka-http" % akkaHttpVersion
  ))

lazy val kafkaStreaming = (project in file("kafka-streaming"))
  .dependsOn(root)
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.kafka" % "kafka-streams" % kafkaStreamsVersion
  ))

lazy val sparkStreaming = (project in file("spark-streaming"))
  .enablePlugins(ScalaxbPlugin)
  .settings(
    commonSettings,
    scalaVersion := "2.11.8",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.spark" %% "spark-streaming" % sparkVersion,
      "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion
//      "net.databinder.dispatch" %% "dispatch-core" % dispatchVersion
  ))
//  .settings(
//    scalaxbPackageName in (Compile, scalaxb) := "generated"
//  )