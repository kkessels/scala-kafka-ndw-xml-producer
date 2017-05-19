package nl.trivento.fastdata.ndw.ingestion

import java.net.URL
import java.util.zip.GZIPInputStream
import java.util.{Properties, UUID}

import nl.trivento.fastdata.api.{ScalaKafkaProducer, XmlIngestor}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer

import scala.concurrent.ExecutionContext
import scala.xml.Elem

/**
  * Created by koen on 13/02/2017.5
  */
class NdwProducer(config: Properties) {
  private val producer = new ScalaKafkaProducer[String, String](config, new StringSerializer, new StringSerializer)
  private implicit val ec: ExecutionContext = ExecutionContext.global

  def send(topic: String, key: String, xml: String): Unit = {
    producer.send(new ProducerRecord(topic, key, xml))
      .onComplete(f => if (f.isFailure) f.failed.get.printStackTrace() else println(f.get.offset()))
  }
}

object NdwSource {
  def main(args: Array[String]) {
    val properties = new Properties()

    //properties.put("compression.type", CompressionType.SNAPPY.name)
//    properties.put("bootstrap.servers", "localhost:9092")
    properties.put("bootstrap.servers", "broker-0.kafka.mesos:9671")
    properties.put("acks", "-1")
    properties.put("client.id", UUID.randomUUID().toString)

    loadSituationRecords()
    loadMeasurementSiteRecords()
    loadSiteMeasurements()

    def getAttr(elem: Elem, name: String): String = {
      elem.attribute(name).map(nodes => nodes.map(_.toString)).getOrElse(Seq.empty).fold("")(_ + _)
    }

    def loadSituationRecords(): Unit = {
      lazy val producer = new NdwProducer(properties)

      XmlIngestor(
        Map(
          "/SOAP:Envelope/SOAP:Body/d2LogicalModel/payloadPublication/situation/situationRecord/groupOfLocations/linearExtension/linearByCoordinatesExtension/" ->
            (e => None),
          "/SOAP:Envelope/SOAP:Body/d2LogicalModel/payloadPublication/situation/situationRecord/groupOfLocations/pointExtension/pointExtension/" ->
            (e => None),
          "/SOAP:Envelope/SOAP:Body/d2LogicalModel/payloadPublication/situation/situationRecord/" ->
            (element => {
//              val situationRecord: SituationRecord = scalaxb.fromXML[SituationRecord](element)
              producer.send("incidents", getAttr(element, "id"), element.toString)
              Option(element)
            }
          )
        )
      ).fromInputStream(new GZIPInputStream(new URL("http://opendata.ndw.nu/incidents.xml.gz").openStream()))
    }

    def loadMeasurementSiteRecords(): Unit = {
      lazy val producer = new NdwProducer(properties)

      XmlIngestor(
        Map(
          "/SOAP:Envelope/SOAP:Body/d2LogicalModel/payloadPublication/measurementSiteTable/measurementSiteRecord/measurementSiteLocation/locationContainedInItinerary/location/linearExtension/" ->
            (e => None),
          "/SOAP:Envelope/SOAP:Body/d2LogicalModel/payloadPublication/measurementSiteTable/measurementSiteRecord/" ->
            (element => {
//              val measurementSite: MeasurementSiteRecord = scalaxb.fromXML[MeasurementSiteRecord](element)
              producer.send("sites", getAttr(element, "id"), element.toString)
              Option(element)
            }
          )
        )
      ).fromInputStream(new GZIPInputStream(new URL("http://opendata.ndw.nu/measurement.xml.gz").openStream()))
    }

    def loadSiteMeasurements(): Unit = {
      lazy val producer = new NdwProducer(properties)

      XmlIngestor(
        Map(
          ("/SOAP:Envelope/SOAP:Body/d2LogicalModel/payloadPublication/siteMeasurements/",
            element => {
//              val siteMeasurements: SiteMeasurements = scalaxb.fromXML[SiteMeasurements](element)
//              producer.send("measurements", siteMeasurements.measurementSiteReference.id, element.toString)
              producer.send("measurements", getAttr(element, "id"), element.toString)
              Option(element)
            }
          )
        )
      ).fromInputStream(new GZIPInputStream(new URL("http://opendata.ndw.nu/trafficspeed.xml.gz").openStream()))
    }
  }
}
