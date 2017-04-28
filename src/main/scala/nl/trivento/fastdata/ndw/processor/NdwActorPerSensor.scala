package nl.trivento.fastdata.ndw.processor

import java.io.ByteArrayInputStream
import java.util
import java.util.UUID
import java.util.regex.Pattern

import akka.actor.{Actor, ActorRef, ActorSystem, Inbox, Props}
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, ProducerSettings, Subscriptions}
import akka.stream.ActorMaterializer
import nl.trivento.fastdata.ndw.shared.serialization.JsonSerializer
import nu.ndw.{DirectionEnum, GroupOfLocations, LaneEnum, Linear, LinearElementByPoints, MeasuredOrDerivedDataTypeEnum, MeasurementSiteRecord, Point, PointCoordinates, SiteMeasurements, TrafficFlow, TrafficFlowType, TrafficSpeed, TrafficSpeedValue, TrafficStatus, TrafficStatusInformation, TravelTimeInformation, VehicleCharacteristics}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{Deserializer, StringDeserializer, StringSerializer}

import scala.xml.XML
import scalaxb.XMLFormat

/**
  * Created by koen on 24/02/2017.
  */
case class NdwSensorId(id: String, index: Int)

trait Message {
  val id: NdwSensorId
}

case class Sensor(id: NdwSensorId, time: Long, direction: Option[DirectionEnum], location: GroupOfLocations,
                  measurementType: MeasuredOrDerivedDataTypeEnum, vehicle: Option[VehicleCharacteristics],
                  numberOfLanes: Option[Int], specificLane: Option[LaneEnum]) extends Message

case class Measurement(id: NdwSensorId, time: Long, value: Double) extends Message

case class OnMap()

object LatLong {
  def apply(pt: PointCoordinates): LatLong = LatLong(pt.latitude, pt.longitude)
}

case class LatLong(lat: Double, long: Double)

case class Heat(heat: Double, location: LatLong, to: LatLong*)

class SensorActor extends Actor {
  private var heatFunc: Option[Double => Heat] = None
  private var sensor: Option[Sensor] = None
  private var measurements: List[Double] = List.empty

  override def receive = {
    case sensor: Sensor => becomeSensor(sensor)
    case _ =>
  }

  def flow: Receive = {
    case sensor: Sensor => becomeSensor(sensor)
    case Measurement =>
  }

  def speed: Receive = {
    case sensor: Sensor => becomeSensor(sensor)
    case m: Measurement =>
      heatFunc.foreach(sender ! _(dev(m.value)))
      measurements = (if (measurements.length == 10) measurements.tail else measurements) :+ m.value
  }

  def status: Receive = {
    case sensor: Sensor => becomeSensor(sensor)
    case Measurement =>
  }

  def traveltime: Receive = {
    case sensor: Sensor => becomeSensor(sensor)
    case m: Measurement =>
      heatFunc.foreach(sender ! _(-dev(m.value)))
      measurements = (if (measurements.length == 10) measurements.tail else measurements) :+ m.value
  }

  def useless: Receive = {
    case sensor: Sensor => becomeSensor(sensor)
    case m: Measurement =>
  }

  def dev(measurement: Double): Double = {
    val all = measurements :+ measurement
    val avg = all.sum / all.length
    val stddev = Math.sqrt(all.map(d => Math.pow(d - avg, 2)).sum / all.length)
    Math.pow((measurement - avg) / stddev, 2)
  }

  def becomeSensor(sensor: Sensor): Unit = {
    if (!this.sensor.contains(sensor)) {
      measurements = List.empty
      this.sensor = Option(sensor)
      heatFunc = sensor.location match {
        case l: Linear => for {
            pt <- l.linearWithinLinearElement.map(_.linearElement).collect { case pt: LinearElementByPoints => pt }
            start <- pt.startPointOfLinearElement.pointCoordinates
            end <- pt.endPointOfLinearElement.pointCoordinates
          } yield (f: Double) => Heat(f, LatLong(start), LatLong(end))
        case p: Point =>
          p.locationForDisplay.map(point => f => Heat(f, LatLong(point)))
        case _ => None
      }
      sensor.measurementType match {
        case TrafficFlow => context.become(flow)
        case TrafficSpeedValue => context.become(speed)
        case TrafficStatusInformation => context.become(status)
        case TravelTimeInformation => context.become(traveltime)
        case _ => context.become(useless)
      }
    }
  }
}

class SensorNetworkActor extends Actor {
  private val pattern = Pattern.compile("[^a-zA-Z0-9\\-_\\.\\*\\$\\+\\:\\@\\&\\=,\\!\\~\\';]")
  private var heatProducer: KafkaProducer[String, Heat] = null

  override def preStart(): Unit = {
    super.preStart()
    heatProducer = ProducerSettings[String, Heat](
      context.system,
      new StringSerializer(),
      new JsonSerializer[Heat](classOf[Heat]))
      .withBootstrapServers("master:9092")
      .createKafkaProducer()
  }

  override def receive: Receive = {
    case measurements: SiteMeasurements =>
      val id = measurements.measurementSiteReference.id
      measurements
        .measuredValue
        .flatMap(value => {
          val time = measurements.measurementTimeDefault.toGregorianCalendar.getTimeInMillis
          val sensorId = NdwSensorId(id, value.index)

          value.measuredValue.basicData match {
          case Some(speed: TrafficSpeed) => speed.averageVehicleSpeed.map(m => Measurement(sensorId, time, m.speed))
          case Some(flow: TrafficFlowType) => flow.vehicleFlow.filter(_.dataError.getOrElse(true)).map(m => Measurement(sensorId, time, m.vehicleFlowRate))
          case Some(status: TrafficStatus) => None
          case _ => None}})
        .foreach(send)

    case site: MeasurementSiteRecord =>
      val id = site.id
      val location = site.measurementSiteLocation
      val numberOfLanes = site.measurementSiteNumberOfLanes
      val time: Long = site.measurementSiteRecordVersionTime.map(c => c.toGregorianCalendar.getTimeInMillis).getOrElse(0)
      val direction = site.measurementSide

      site.measurementSpecificCharacteristics
        .map(e => {
          val measurementType = e.measurementSpecificCharacteristics.specificMeasurementValueType
          val vehicleCharacteristics = e.measurementSpecificCharacteristics.specificVehicleCharacteristics
          val lane = e.measurementSpecificCharacteristics.specificLane

          Sensor(NdwSensorId(id, e.index), time, direction, location, measurementType, vehicleCharacteristics, numberOfLanes, lane)})
        .foreach(send)

    case heat: Heat => heatProducer.send(new ProducerRecord("heat", "NL", heat))
  }

  def actorName(ndwSensorId: NdwSensorId): String = {
    pattern.matcher(ndwSensorId.id).replaceAll("_") + "_" + ndwSensorId.index
  }

  def send(message: Message): Unit = {
    val name = message.id.id.replaceAll("/| ", "_") + "_" + message.id.index.toString
    context.child(name).getOrElse(context.actorOf(Props[SensorActor], name)) ! message
  }
}

class XmlDeserializer[T : XMLFormat] extends Deserializer[T] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}

  override def deserialize(topic: String, data: Array[Byte]): T = {
    scalaxb.fromXML[T](XML.load(new ByteArrayInputStream(data)))
  }
}

class NdwActorPerSensor() {
  private implicit val system = ActorSystem.create("ndw")
  private implicit val executionContext = system.dispatchers.lookup("default-dispatcher")
  private implicit val materializer = ActorMaterializer.create(system)
  private val inbox = Inbox.create(system)
  private val network = system.actorOf(Props[SensorNetworkActor])

  private val siteConsumer = ConsumerSettings[String, MeasurementSiteRecord](
    system,
    new StringDeserializer(),
    new XmlDeserializer[MeasurementSiteRecord])
    .withProperty("auto.offset.reset", "earliest")
//    .withProperty("compression.type", CompressionType.SNAPPY.name)
    .withBootstrapServers("master:9092")
    .withClientId(UUID.randomUUID().toString)
    .withGroupId("ndw_sites_" + UUID.randomUUID.toString)

  Consumer.committableSource(siteConsumer, Subscriptions.topics("sites"))
    .runForeach(msg => {
      process(msg.record.value)
      msg.committableOffset.commitScaladsl()
    })

  private val measurementConsumer = ConsumerSettings[String, SiteMeasurements](
    system,
    new StringDeserializer(),
    new XmlDeserializer[SiteMeasurements])
    .withProperty("auto.offset.reset", "earliest")
//    .withProperty("compression.type", CompressionType.SNAPPY.name)
    .withBootstrapServers("master:9092")
    .withClientId(UUID.randomUUID().toString)
    .withGroupId("ndw_measurements_" + UUID.randomUUID.toString)

  Consumer.committableSource(measurementConsumer, Subscriptions.topics("measurements"))
    .runForeach(msg => {
      process(msg.record.value)
      msg.committableOffset.commitScaladsl()
    })

  def process(message: SiteMeasurements): Unit = {
    //inbox.send(network, message)
    network.tell(message, ActorRef.noSender)
  }

  def process(message: MeasurementSiteRecord): Unit = {
    //inbox.send(network, message)
    network.tell(message, ActorRef.noSender)
  }
}

object NdwActorPerSensor {
  def main(args: Array[String]): Unit = {
    new NdwActorPerSensor()
  }
}
