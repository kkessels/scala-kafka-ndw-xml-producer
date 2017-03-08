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

import scala.xml.{Elem, XML}

/**
  * Created by kkessels on 07/03/17.
  */
object SparkStreaming extends App {

  case class SiteInfo(id: String, speeds: List[(Float, Boolean)],
                      intensities: List[(BigInt, Boolean)],
                      travelTimes: List[(Float, Float, Float)])

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
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

  case class TestThing(info: String)

  val result = stream.map(record => {
    val d = scalaxb.fromXML[SiteMeasurements](
      XML.loadString(
        record.value()
      )
    )

    val id = d.measurementSiteReference.id
    val data = d.measuredValue.flatMap(_.measuredValue.basicData)

    val speeds = data.flatMap{ _ match {
      case speed: TrafficSpeed => speed.averageVehicleSpeed
      case _ => None
    }}.toList.map(s => (s.speed, s.dataError match {
      case Some(b: Boolean) => b
      case _ => true
    }))

    val intensities = data.flatMap{ _ match {
      case intensity: TrafficFlowType => intensity.vehicleFlow
      case _ => None
    }}.toList.map(f => (f.vehicleFlowRate, f.dataError match {
      case Some(b: Boolean) => b
      case _ => true
    }))

    val travelTimes = (for {
      TravelTimeData(_, _, _, _, _, _, _, freeFlow, normalFlow, currentFlow, _, _, _) <- data
      DurationValue(_, _, _, freeFlowTime, _, _) <- freeFlow
      DurationValue(_, _, _, normalFlowTime, _, _) <- normalFlow
      DurationValue(_, _, _, currentFlowTime, _, _) <- currentFlow
    } yield (freeFlowTime, normalFlowTime, currentFlowTime)).toList

    //      val test = data.flatMap{
    //        case t: TrafficFlowType => Option(t)
    //        case _ => None
    //      }
    //      test
    SiteInfo(id, speeds, intensities, travelTimes)
  })

  result.print()

  streamingContext.start()

  streamingContext.awaitTermination()
}
