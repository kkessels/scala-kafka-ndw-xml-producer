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

val sparkVersion = "2.1.1"
val sparkCoreVersion = "2.1.1"
val sparkSqlVersion = "2.1.1"
val sparkStreamingVersion = "2.1.1"
val sparkStreamingKafkaVersion = "2.1.1"
val sparkMlVersion = "2.1.1"

val dispatchVersion = "0.12.0"

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

resolvers +=
  "Geotools snapshots" at "http://download.osgeo.org/webdav/geotools/"

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
      "net.databinder.dispatch" %% "dispatch-core" % dispatchVersion,
      "org.scala-lang.modules" %% "scala-xml" % "1.0.6",
      "org.apache.kafka" % "kafka_2.12" % "0.10.1.1",
      //  "org.apache.commons" % "commons-lang3" % "3.5",
      "com.typesafe.akka" %% "akka-actor" % "2.4.16",
      "com.typesafe.akka" %% "akka-stream-kafka" % "0.13",
      "com.typesafe.akka" %% "akka-http-core" % "10.0.3",
      "com.typesafe.akka" %% "akka-http" % "10.0.3",
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.8.7",
      "org.geotools" % "gt-shapefile" % "17.0"
  ))
  .settings(
    scalaxbPackageName in (Compile, scalaxb) := "generated",
    mainClass in assembly := Some("nl.trivento.fastdata.ndw.ingestion.NdwSource")
  )

lazy val kafkaStreaming = (project in file("kafka-streaming"))
  .enablePlugins(DockerPlugin)
//  .enablePlugins(ScalaxbPlugin)
  .dependsOn(root)
  .settings(
    commonSettings,
    dockerSettings,
    libraryDependencies ++= Seq(
      "org.apache.kafka" % "kafka-streams" % kafkaStreamsVersion
    ),
    mainClass in assembly := Some("nl.trivento.fastdata.ndw.processing.SiteInfoProcessor")
  )


lazy val sparkStreaming = (project in file("spark-streaming"))
  .enablePlugins(DockerPlugin)
//    .enablePlugins(ScalaxbPlugin)
  .settings(
    commonSettings,
    dockerSettings,
    mainClass in (Compile, run) := Some("nl.trivento.fastdata.ndw.processing.SparkStreaming"),
    scalaVersion := "2.11.8",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.spark" %% "spark-streaming" % sparkVersion,
      "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
      "org.apache.spark" %% "spark-mllib" % sparkMlVersion,
      "net.databinder.dispatch" %% "dispatch-core" % dispatchVersion
  ))