package nl.trivento.fastdata.ndw.processing

import java.util.{Properties, UUID}

import generated.{DurationValue, SiteMeasurements, TrafficFlowType, TrafficSpeed, TravelTimeData}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.kstream._

import scala.xml.{Elem, XML}

/**
  * Created by kkessels on 03/03/17.
  */

object SiteInfoProcessor {
  def main(args: Array[String]): Unit = {
    try {
      startStreaming
    } catch {
      case e: Exception => e.printStackTrace
    }
  }

  def startStreaming {
    val measurementsTopic = "measurements"

    val brokers = "broker-0.kafka.mesos:9671"   // bootstrap-servers
    val zookeeper = "master.mesos:2181/dcos-service-kafka"

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
      .mapValues[SiteInfo] { d =>
      val id = d.measurementSiteReference.id
      val data = d.measuredValue.flatMap(_.measuredValue.basicData)

      val speeds = data.flatMap {
        _ match {
          case speed: TrafficSpeed => speed.averageVehicleSpeed
            .filter(_.dataError.getOrElse(false))
            .map(_.speed)
          case _ => None
        }
      }.toList

      val intensities = data.flatMap {
        _ match {
          case intensity: TrafficFlowType => intensity.vehicleFlow
            .filterNot(_.dataError.getOrElse(false))
            .map(_.vehicleFlowRate)
          case _ => None
        }
      }.toList

      val travelTimes = (for {
        ttd: TravelTimeData <- data.collect({ case t: TravelTimeData => t })
        fftt: DurationValue <- ttd.freeFlowTravelTime
        nett: DurationValue <- ttd.normallyExpectedTravelTime
        tt: DurationValue <- ttd.travelTime
      } yield (fftt.duration, nett.duration, tt.duration)).toList

      //    val test = data.flatMap {
      //      case t: TravelTimeData => Option(t)
      //      case _ => None
      //    }
      //    test

      SiteInfo(id, speeds, intensities, travelTimes)
    }
      .mapValues[String](_.toString)

    //  measurements.foreach { (_, v) =>
    //    if(v.nonEmpty) println(s"There are intensities: $v")
    //  }

    measurements.print()

    measurements.mapValues[String](_.toString)
      .to(Serdes.String(), Serdes.String(), "siteInfo")

    val stream: KafkaStreams = new KafkaStreams(builder, streamingConfig)
    stream.start()
  }

  case class SiteInfo(id: String, speeds: List[Float],
                      intensities: List[BigInt],
                      travelTimes: List[(Float, Float, Float)])
}
