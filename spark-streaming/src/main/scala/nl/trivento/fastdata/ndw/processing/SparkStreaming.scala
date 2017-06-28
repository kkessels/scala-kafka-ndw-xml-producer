package nl.trivento.fastdata.ndw.processing

import nl.trivento.fastdata.ndw.shared.serialization.MeasurementExtraJsonSerde
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{Milliseconds, Duration, StreamingContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

import scala.concurrent.duration.Duration

/**
  * Created by kkessels on 07/03/17.
  */
object SparkStreaming extends App {

  case class MeasurementExtra(id: NdwSensorId,
                              time: Long,
                              speed: Option[Float],
                              intensity: Option[BigInt],
                              travelTimes: Option[(Float, Float, Float)]
                             ) extends Message

  case class NdwSensorId(id: String, index: Int)

  trait Message {
    val id: NdwSensorId
  }

  val bootstrapServers = "localhost:9092" //"broker-0.kafka.mesos:9671"

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> bootstrapServers,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[MeasurementExtraJsonSerde],
    "group.id" -> "spark-stream-test",
    "auto.offset.reset" -> "earliest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val sparkSession = SparkSession
    .builder
    .master("local[2]")
    .appName("spark-streaming-ml")
    .getOrCreate()
  sparkSession.sparkContext.setLogLevel("WARN")

  val numClusters = 4
  val decayFactor = 1.0
  val randomCentersDim = 2
  val randomCentersWeight = 0.0

  val microbatchDuration = Duration(500)
  val streamingContext = new StreamingContext(sparkSession.sparkContext, microbatchDuration)

  val topics = List("measurementsExtra")

  val stream: DStream[ConsumerRecord[String, MeasurementExtra]] = KafkaUtils.createDirectStream[String, MeasurementExtra](
    streamingContext,
    PreferConsistent,
    Subscribe[String, MeasurementExtra](topics, kafkaParams)
  )

  val vectorStream: DStream[Vector] = stream.map{ record =>
    val measurement = record.value

    val intensity = measurement.intensity match { case Some(b: BigInt) => b.toDouble case _ => 0.0 }
    val speed = measurement.speed match { case Some(f: Float) => f.toDouble case _ => 0.0 }
//    val (tt1, tt2, tt3) = measurement.travelTimes match {
//      case Some((f1: Float, f2: Float, f3: Float)) => (f1.toDouble, f2.toDouble, f3.toDouble) case _ => (0.0f, 0.0f, 0.0f)
//    }

    Vectors.dense(0, intensity)//, speed)//, tt1, tt2, tt3)
  }

//  val trafficData: DStream[Vector] = vectorStream.transform(normalize)

  val model = new StreamingKMeans()
    .setK(numClusters)
    .setDecayFactor(decayFactor)
    .setRandomCenters(randomCentersDim, randomCentersWeight)

  model.trainOn(vectorStream)

  val skmodel = model.latestModel()

  println("Cluster centers: ")
  skmodel.clusterCenters.foreach(println)
  println("Cluster weights: ")
  skmodel.clusterWeights.foreach(println)

  val predictClusters: DStream[Int] = model.predictOn(vectorStream)

  predictClusters.countByValue().print()

  predictClusters.foreachRDD{ rdd => println("Samples in batch: " + rdd.count())}

  predictClusters.foreachRDD{ rdd => if(!rdd.isEmpty()){ println(s"first = ${rdd.first()}") }}

  sys.ShutdownHookThread {
    streamingContext.stop(stopSparkContext = true, stopGracefully =  true)
  }

  streamingContext.start()
  streamingContext.awaitTermination()

  private def normalize: RDD[Vector] => RDD[Vector] = { rdd =>
    if (rdd.isEmpty()) rdd else {
      new StandardScaler().fit(rdd).transform(rdd)
    }
  }
}
