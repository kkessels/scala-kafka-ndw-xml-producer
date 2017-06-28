package nl.trivento.fastdata.ndw.ingestion

import java.net.URL
import java.util.zip.GZIPInputStream
import java.util.{Properties, UUID}

import nl.trivento.fastdata.api.{ScalaKafkaProducer, XmlIngestor}
import nl.trivento.fastdata.ndw.shared.serialization.{MeasurementExtraJsonSerde, TypedJsonSerializer}
import nl.trivento.fastdata.ndw.{Measurement, MeasurementExtra, Sensor}
import generated.{MeasurementSiteRecord, SiteMeasurements}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{BytesSerializer, StringSerializer}
import org.apache.kafka.common.utils.Bytes

import scala.concurrent.ExecutionContext
import scala.xml.Elem

/**
  * Created by koen on 13/02/2017.5
  */
class NdwProducer(config: Properties) {
  private val producer = new ScalaKafkaProducer[String, Bytes](config, new StringSerializer, new BytesSerializer)
  private implicit val ec: ExecutionContext = ExecutionContext.global

  def send(topic: String, key: String, data: Bytes): Unit = {
    producer.send(new ProducerRecord(topic, key, data))
  }
}

object NdwSource {
  def main(args: Array[String]) {
    val properties = new Properties()

    //properties.put("compression.type", CompressionType.SNAPPY.name)
    properties.put("bootstrap.servers", "localhost:9092")
//    properties.put("bootstrap.servers", "broker-0.kafka.mesos:9671")
//    properties.put("bootstrap.servers", "master:9092")
    properties.put("acks", "0")
    properties.put("batch.size", "1048576")
    properties.put("linger.ms", "500")
    properties.put("buffer.memory", "33554432")
    properties.put("client.id", UUID.randomUUID().toString)

    val sensorSerializer = new TypedJsonSerializer[Sensor](classOf[Sensor])
    val measurementSerializer = new TypedJsonSerializer[Measurement](classOf[Measurement])
    val measurementExtraSerializer = new MeasurementExtraJsonSerde

    //    loadSituationRecords()
    loadMeasurementSiteRecords()
    loadSiteMeasurements()

    def getAttr(elem: Elem, name: String): String = {
      elem.attribute(name).map(nodes => nodes.map(_.toString)).getOrElse(Seq.empty).fold("")(_ + _)
    }

//    def loadSituationRecords(): Unit = {
//      properties.put("client.id", UUID.randomUUID().toString)
//      lazy val producer = new NdwProducer(properties)
//
//      XmlIngestor(
//        Map(
//          "/SOAP:Envelope/SOAP:Body/d2LogicalModel/payloadPublication/situation/situationRecord/groupOfLocations/linearExtension/linearByCoordinatesExtension/" ->
//            (e => None),
//          "/SOAP:Envelope/SOAP:Body/d2LogicalModel/payloadPublication/situation/situationRecord/groupOfLocations/pointExtension/pointExtension/" ->
//            (e => None),
//          "/SOAP:Envelope/SOAP:Body/d2LogicalModel/payloadPublication/situation/situationRecord/" ->
//            (element => {
////              val situationRecord: SituationRecord = scalaxb.fromXML[SituationRecord](element)
//              producer.send("incidents", getAttr(element, "id"), element.toString)
//              Option(element)
//            }
//          )
//        )
//      ).fromInputStream(new GZIPInputStream(new URL("http://opendata.ndw.nu/incidents.xml.gz").openStream()))
//    }

    def loadMeasurementSiteRecords(): Unit = {
      properties.put("client.id", UUID.randomUUID().toString)
      lazy val producer = new NdwProducer(properties)

      XmlIngestor(
        Map(
          "/SOAP:Envelope/SOAP:Body/d2LogicalModel/payloadPublication/measurementSiteTable/measurementSiteRecord/measurementSiteLocation/locationContainedInItinerary/location/linearExtension/" ->
            (e => None),
          "/SOAP:Envelope/SOAP:Body/d2LogicalModel/payloadPublication/measurementSiteTable/measurementSiteRecord/" ->
            (element => {
              Sensor.fromSiteMeasurement(scalaxb.fromXML[MeasurementSiteRecord](element)).foreach(
                sensor => producer.send("sites", getAttr(element, "id"), new Bytes(sensorSerializer.serialize(null, sensor)))
              )
              None
            }
          )
        )
      ).fromInputStream(new GZIPInputStream(new URL("http://opendata.ndw.nu/measurement.xml.gz").openStream()))
    }

    def loadSiteMeasurements(): Unit = {
      properties.put("client.id", UUID.randomUUID().toString)
      lazy val producer = new NdwProducer(properties)

      XmlIngestor(
        Map(
          ("/SOAP:Envelope/SOAP:Body/d2LogicalModel/payloadPublication/siteMeasurements/",
            element => {
              val siteMeasurements: SiteMeasurements = scalaxb.fromXML[SiteMeasurements](element)
              Measurement.fromSiteMeasurements(siteMeasurements).foreach(
                measurement => producer.send("measurements", siteMeasurements.measurementSiteReference.id,
                  new Bytes(measurementSerializer.serialize(null, measurement)))
              )
              MeasurementExtra.fromSiteMeasurements(siteMeasurements).foreach(
                measurement => producer.send("measurementsExtra", siteMeasurements.measurementSiteReference.id,
                  new Bytes(measurementExtraSerializer.serialize(null, measurement)))
              )
              None
            }
          )
        )
      ).fromInputStream(new GZIPInputStream(new URL("http://opendata.ndw.nu/trafficspeed.xml.gz").openStream()))
    }
  }
}
