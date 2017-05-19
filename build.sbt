import sbt.Keys.mainClass
import sbtdocker.DockerPlugin.autoImport.{ImageName, imageNames}

name := """scala-kafka-ndw-xml-producer"""

version := "0.1"

organization := "nl.trivento.kkessels"

scalaVersion in ThisBuild := "2.12.1"

scalacOptions ++= Seq(
  "-unchecked",
  "-deprecation",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard"
)

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

//lazy val dockerSettings = Seq(
//  dockerfile in docker := {
//    // The assembly task generates a fat JAR file
//    val artifact: File = assembly.value
//    val artifactTargetPath = s"/app/${artifact.name}"
//
//    new Dockerfile {
//      from("java:openjdk-8-jre")
//      add(artifact, artifactTargetPath)
//      entryPoint("java", "-cp", artifactTargetPath, "nl.trivento.fastdata.ndw.ingestion.NdwSource")
//    }
//  },
//
//  imageNames in docker := Seq(
//    ImageName(s"kkessels/ndw-test:latest")
//  )
//)

lazy val dockerSettings = Seq(
  dockerfile in docker := {
    // The assembly task generates a fat JAR file
    val artifact: File = assembly.value
    val artifactTargetPath = s"/app/${artifact.name}"

    new Dockerfile {
      from("java:openjdk-8-jre")
      add(artifact, artifactTargetPath)
      entryPoint("java", "-jar", artifactTargetPath, "-Xmx 3G")
    }
  },

  imageNames in docker := Seq(
    ImageName(s"kkessels/ndw-test-${assembly.value.getName.toLowerCase}:latest")
  )
)


lazy val commonSettings = Seq(
  libraryDependencies ++= Seq(
    "org.scala-lang.modules" %% "scala-xml" % scalaXmlVersion,
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonModuleScalaVersion
  ))

lazy val root = (project in file("."))
  .enablePlugins(DockerPlugin)
  .enablePlugins(ScalaxbPlugin)
  .settings(
    commonSettings,
    dockerSettings,
    libraryDependencies ++= Seq(
      "org.apache.kafka" %% "kafka" % kafkaVersion,
      "com.typesafe.akka" %% "akka-actor" % akkaActorVersion,
      "com.typesafe.akka" %% "akka-stream-kafka" % akkaStreamKafka,
      "com.typesafe.akka" %% "akka-http-core" % akkaHttpCoreVersion,
      "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
      "net.databinder.dispatch" %% "dispatch-core" % dispatchVersion
  ))
  .settings(
    scalaxbPackageName in (Compile, scalaxb) := "generated",
    mainClass in assembly := Some("nl.trivento.fastdata.ndw.ingestion.NdwSource")
  )

lazy val kafkaStreaming = (project in file("kafka-streaming"))
  .enablePlugins(DockerPlugin)
  .enablePlugins(ScalaxbPlugin)
  .dependsOn(root)
  .settings(
    dockerSettings,
    libraryDependencies ++= Seq(
      "org.apache.kafka" % "kafka-streams" % kafkaStreamsVersion
    ),
    mainClass in assembly := Some("nl.trivento.fastdata.ndw.processing.SiteInfoProcessor")
  )


lazy val sparkStreaming = (project in file("spark-streaming"))
  .enablePlugins(DockerPlugin)
  .enablePlugins(ScalaxbPlugin)
  .settings(
    commonSettings,
    dockerSettings,
    scalaVersion := "2.11.8",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.spark" %% "spark-streaming" % sparkVersion,
      "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
      "net.databinder.dispatch" %% "dispatch-core" % dispatchVersion
  ))
  .settings(
    scalaxbPackageName in (Compile, scalaxb) := "generated"
  )