package nl.trivento.fastdata.ndw.processing

import java.util.{Properties, UUID}

import nu.ndw.{DurationValue, SiteMeasurements, TrafficFlowType, TrafficSpeed, TravelTimeData}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.kstream._

import scala.xml.{Elem, XML}

/**
  * Created by kkessels on 03/03/17.
  */

object SiteInfoProcessor extends App {
  case class SiteInfo(id: String, speeds: List[(Float, Boolean)],
                      intensities: List[(Int, Boolean)],
                      travelTimes: List[(Float, Float, Float)])

  val sitesTopic = "sites"
  val measurementsTopic = "measurements"

  val brokers = "localhost:9092"
  val zookeeper = "localhost:2181"

  val streamingConfig: Properties = {
    val settings = new Properties
    settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    settings.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString)
    settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, zookeeper)
    settings.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass.getName)
    settings.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass.getName)
    settings.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp")
    settings
  }

  val builder = new KStreamBuilder

  val measurements = builder.stream[String, String](Serdes.String(), Serdes.String(), measurementsTopic)
    .mapValues[Elem](XML.loadString(_))
    .mapValues[SiteMeasurements](scalaxb.fromXML[SiteMeasurements](_))
    .mapValues[SiteInfo]{ d =>
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
  }
    .mapValues[String](_.toString)

  measurements.print()

  measurements.to(Serdes.String(), Serdes.String(), "siteInfo")

  val stream: KafkaStreams = new KafkaStreams(builder, streamingConfig)
  stream.start()
}
