package nl.trivento.fastdata.ndw.processing

import generated.{DurationValue, SiteMeasurements, TrafficFlowType, TrafficSpeed, TravelTimeData}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.sql.SparkSession

import scala.xml.XML

/**
  * Created by kkessels on 07/03/17.
  */
object SparkStreaming extends App {

  case class SiteInfo(id: String, speeds: List[Float],
                      intensities: List[BigInt],
                      travelTimes: List[Float])

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "broker-0.kafka.mesos:9671",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "spark-stream-test",
    "auto.offset.reset" -> "earliest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val sparkSession = SparkSession
    .builder
    .master("local[2]")
    .appName("spark-streaming-yay")
    .getOrCreate()

  val streamingContext = new StreamingContext(sparkSession.sparkContext, Duration(1*1000))

  val topics = List("measurements")
  val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
    streamingContext,
    PreferConsistent,
    Subscribe[String, String](topics, kafkaParams)
  )

  val processedData: DStream[(Float, BigInt, Float)] = stream.map(record => {
    val d = scalaxb.fromXML[SiteMeasurements](
      XML.loadString(
        record.value()
      )
    )

    val id = d.measurementSiteReference.id
    val data = d.measuredValue.flatMap(_.measuredValue.basicData)

    val speeds = data.flatMap { _ match {
      case speed: TrafficSpeed => speed.averageVehicleSpeed
        .filterNot(_.dataError.getOrElse(false))
        .map(_.speed)
      case _ => None
    }}.toList

    val intensities = data.flatMap { _ match {
      case intensity: TrafficFlowType => intensity.vehicleFlow
        .filterNot(_.dataError.getOrElse(true))
        .map(_.vehicleFlowRate)
      case _ => None
    }}.toList

    val travelTimes = (for {
      ttd : TravelTimeData <- data.collect({case t: TravelTimeData => t})
      fftt: DurationValue <- ttd.freeFlowTravelTime
      nett: DurationValue <- ttd.normallyExpectedTravelTime
      tt: DurationValue <- ttd.travelTime
    } yield /*(fftt.duration, nett.duration, */tt.duration).toList

//    val travelTimes = data.flatMap { _ match {
//      case travelTimes: TravelTimeData => travelTimes.travelTime
//    }}

    //      val test = data.flatMap{
    //        case t: TrafficFlowType => Option(t)
    //        case _ => None
    //      }
    //      test
    SiteInfo(id, speeds, intensities, travelTimes)
  }).reduceByWindow((current, next) =>
    SiteInfo("averages", current.speeds ::: next.speeds, current.intensities ::: next.intensities, current.travelTimes ::: next.travelTimes)
    , Duration(1*1000), Duration(1*1000)
  ).map(record =>
    (if(record.speeds.nonEmpty) record.speeds.sum / record.speeds.length else 0,
      if(record.intensities.nonEmpty) record.intensities.sum / record.intensities.length else 0,
      if(record.travelTimes.nonEmpty) record.travelTimes.sum / record.travelTimes.length else 0)
    )

  processedData.print()

  streamingContext.start()

  streamingContext.awaitTermination()
}
