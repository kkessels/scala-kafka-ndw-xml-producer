package nl.trivento.fastdata.ndw.ingestion

import java.net.URL
import java.util.zip.GZIPInputStream
import java.util.{Properties, UUID}

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.databind.ObjectMapper.DefaultTypeResolverBuilder
import com.fasterxml.jackson.databind.{JavaType, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import kafka.message.SnappyCompressionCodec
import nl.trivento.fastdata.api.{ScalaKafkaProducer, XmlIngestor}
import nu.ndw.{MeasurementSiteRecord, SiteMeasurements, SituationRecord, _SiteMeasurementsIndexMeasuredValue}
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer

import scala.concurrent.Future

/**
  * Created by koen on 13/02/2017.5
  */
class NdwProducer[T](config: Properties) {
  private val producer = new ScalaKafkaProducer[String, String](config, new StringSerializer, new StringSerializer)
  private val mapper = new ObjectMapper()
  mapper.setDefaultTyping(new DefaultTypeResolverBuilder(null) {
    init(JsonTypeInfo.Id.CLASS, null)
    inclusion(JsonTypeInfo.As.PROPERTY)

    override def useForType(t: JavaType): Boolean =
      !t.isPrimitive && t.getRawClass.getPackage.getName == "nu.ndw" && !t.isFinal
  })
  mapper.setSerializationInclusion(Include.NON_EMPTY)
  mapper.registerModule(DefaultScalaModule)

  def send(topic: String, key: String, record: T): Future[RecordMetadata] = {
    val json: String = mapper.writeValueAsString(record)
    println(json)
    producer.send(new ProducerRecord(topic, key, json))
  }
}

object NdwSource {
  def main(args: Array[String]) {
    val properties = new Properties()

    properties.put("compression.codec", SnappyCompressionCodec.codec.toString)
    properties.put("producer.type", "sync")
    properties.put("metadata.broker.list", "localhost:9092")
    properties.put("bootstrap.servers", "localhost:9092")
    properties.put("message.send.max.retries", "5")
    properties.put("request.required.acks", "-1")
    properties.put("client.id", UUID.randomUUID().toString)

    loadSituationRecords()
    loadMeasurementSiteRecords()
    loadSiteMeasurements()

    def loadSituationRecords(): Unit = {
      val producer = new NdwProducer[SituationRecord](properties)

      XmlIngestor(
        Map(
          "/SOAP:Envelope/SOAP:Body/d2LogicalModel/payloadPublication/situation/situationRecord/groupOfLocations/linearExtension/linearByCoordinatesExtension/" ->
            (e => None),
          "/SOAP:Envelope/SOAP:Body/d2LogicalModel/payloadPublication/situation/situationRecord/groupOfLocations/pointExtension/pointExtension/" ->
            (e => None),
          "/SOAP:Envelope/SOAP:Body/d2LogicalModel/payloadPublication/situation/situationRecord/" ->
            (element => {
              val situationRecord: SituationRecord = scalaxb.fromXML[SituationRecord](element)
              producer.send("incidents", situationRecord.id, situationRecord)
              Option(element)
            }
          )
        )
      ).fromInputStream(new GZIPInputStream(new URL("http://opendata.ndw.nu/incidents.xml.gz").openStream()))
    }

    def loadMeasurementSiteRecords(): Unit = {
      val producer = new NdwProducer[MeasurementSiteRecord](properties)

      XmlIngestor(
        Map(
          "/SOAP:Envelope/SOAP:Body/d2LogicalModel/payloadPublication/measurementSiteTable/measurementSiteRecord/measurementSiteLocation/locationContainedInItinerary/location/linearExtension/" ->
            (e => None),
          "/SOAP:Envelope/SOAP:Body/d2LogicalModel/payloadPublication/measurementSiteTable/measurementSiteRecord/" ->
            (element => {
              val measurementSite: MeasurementSiteRecord = scalaxb.fromXML[MeasurementSiteRecord](element)
              producer.send("sites", measurementSite.id, measurementSite)
              Option(element)
            }
          )
        )
      ).fromInputStream(new GZIPInputStream(new URL("http://opendata.ndw.nu/measurement.xml.gz").openStream()))
    }

    def loadSiteMeasurements(): Unit = {
      val producer = new NdwProducer[_SiteMeasurementsIndexMeasuredValue](properties)

      XmlIngestor(
        Map(
          ("/SOAP:Envelope/SOAP:Body/d2LogicalModel/payloadPublication/siteMeasurements/",
            element => {
              val siteMeasurements: SiteMeasurements = scalaxb.fromXML[SiteMeasurements](element)
              siteMeasurements.measuredValue.foreach(
                value => producer.send("measurements", siteMeasurements.measurementSiteReference.id, value)
              )
              Option(element)
            }
          )
        )
      ).fromInputStream(new GZIPInputStream(new URL("http://opendata.ndw.nu/trafficspeed.xml.gz").openStream()))
    }
  }
}
