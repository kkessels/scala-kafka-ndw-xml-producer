package nl.trivento.fastdata.ndw.processing

import java.util.{Properties, UUID}

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonInclude}
import nu.ndw.{MeasuredValue, MeasurementSiteRecord, SiteMeasurements}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.kstream._

/**
  * Created by kkessels on 03/03/17.
  */

object SiteInfoProcessor extends App {
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonInclude(Include.NON_NULL)
  case class SiteInfo(id: String, values: Seq[MeasuredValue])

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
//    settings.put(StreamsConfig.STATE_DIR_CONFIG, stateDir)
    settings
  }

  val builder = new KStreamBuilder

//  val sites: KStream[String, MeasurementSiteRecord] = builder.stream[String, MeasurementSiteRecord](sitesTopic)

  val measurements = builder.stream[String, SiteMeasurements](new JSONSerde[String], new JSONSerde[SiteMeasurements], measurementsTopic)
    .mapValues[SiteInfo](m => SiteInfo(m.measurementSiteReference.id, m.measuredValue.map(_.measuredValue)))

  val joiner = new ValueJoiner[MeasurementSiteRecord, SiteMeasurements, (MeasurementSiteRecord, SiteMeasurements)] {
    override def apply(v1: MeasurementSiteRecord, v2: SiteMeasurements): (MeasurementSiteRecord, SiteMeasurements) = {
      (v1, v2)
    }
  }

//  sites.join(measurements, joiner, JoinWindows.of(10*60*1000))

  measurements.print()

  measurements.to("siteInfo")

  val stream: KafkaStreams = new KafkaStreams(builder, streamingConfig)
  stream.start()
}
