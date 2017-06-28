package nl.trivento.fastdata.ndw.processor

import java.util.UUID
import java.util.regex.Pattern

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Inbox, Props}
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, ProducerSettings, Subscriptions}
import akka.stream.ActorMaterializer
import nl.trivento.fastdata.ndw.shared.serialization.{TypedJsonDeserializer, TypedJsonSerializer}
import nl.trivento.fastdata.ndw.{Measurement, Message, Sensor}
import generated.PointCoordinates
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

object LatLong {
  def apply(pt: PointCoordinates): LatLong = LatLong(pt.latitude, pt.longitude)
}

case class LatLong(lat: Double, long: Double)

case class Heat(heat: Double, location: LatLong, to: LatLong*)

class SensorActor extends Actor {
  private var heatFunc: Option[Double => Heat] = None
  private var sensor: Sensor = _
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
    val dev = all.map(d => Math.pow(d - avg, 2)).sum / all.length
    if (dev == 0) return 0
    val stddev = Math.sqrt(dev)
    if (stddev != 0) (measurement - avg) / stddev else 0
  }

  def becomeSensor(sensor: Sensor): Unit = {
    if (this.sensor == null || sensor.measurementType.toString != this.sensor.measurementType.toString) {
      measurements = List.empty
      sensor.measurementType.toString match {
        case "trafficFlow" => context.become(flow)
        case "trafficSpeed" => context.become(speed)
        case "trafficStatusInformation" => context.become(status)
        case "travelTimeInformation" => context.become(traveltime)
        case _ => context.become(useless)
      }
    }
    this.sensor = sensor
    heatFunc = Option((f: Double) => Heat(f, sensor.location.head))
  }
}

class SensorNetworkActor extends Actor with ActorLogging {
  private val pattern = Pattern.compile("[^a-zA-Z0-9\\-_\\.\\*\\$\\+\\:\\@\\&\\=,\\!\\~\\';]")
  private var heatProducer: KafkaProducer[String, Heat] = _

  override def preStart(): Unit = {
    super.preStart()
    heatProducer = ProducerSettings[String, Heat](
      context.system,
      new StringSerializer(),
      new TypedJsonSerializer[Heat](classOf[Heat]))
      .withBootstrapServers("master:9092")
      .withParallelism(8)
      .withProperty("batch.size", "1048576")
      .withProperty("linger.ms", "5000")
      .withProperty("buffer.memory", "33554432")
      .withProperty("acks", "0")
      .createKafkaProducer()
  }

  override def receive: Receive = {
    case message: Message =>
      send(message)

    case heat: Heat =>
      heatProducer.send(new ProducerRecord("heat", "NL", heat))
  }

  def send(message: Message): Unit = {
    val name = message.id.id.replaceAll("/| ", "_") + "_" + message.id.index.toString
    context.child(name).getOrElse(context.actorOf(Props[SensorActor], name)) ! message
  }
}

class NdwActorPerSensor() {
  private implicit val system = ActorSystem.create("ndw")
  private implicit val executionContext = system.dispatchers.lookup("default-dispatcher")
  private implicit val materializer = ActorMaterializer.create(system)
  private val inbox = Inbox.create(system)
  private val network = system.actorOf(Props[SensorNetworkActor])

  private val siteConsumer = ConsumerSettings[String, Sensor](
    system,
    new StringDeserializer(),
    new TypedJsonDeserializer[Sensor](classOf[Sensor]))
    .withProperty("auto.offset.reset", "earliest")
    .withBootstrapServers("master:9092")
    .withClientId(UUID.randomUUID().toString)
    .withGroupId("ndw_sites_" + UUID.randomUUID.toString)
    .withProperty("fetch.max.wait.ms", "500")
    .withProperty("fetch.min.bytes", "1048576")
    .withProperty("enable.auto.commit", "true")
    .withProperty("auto.commit.interval.ms", "500")

  Consumer.committableSource(siteConsumer, Subscriptions.topics("sites"))
    .runForeach(msg => {
      process(msg.record.value)
      //msg.committableOffset.commitScaladsl()
    })

  private val measurementConsumer = ConsumerSettings[String, Measurement](
    system,
    new StringDeserializer(),
    new TypedJsonDeserializer[Measurement](classOf[Measurement]))
    .withProperty("auto.offset.reset", "earliest")
    .withBootstrapServers("master:9092")
    .withClientId(UUID.randomUUID().toString)
    .withGroupId("ndw_measurements_" + UUID.randomUUID.toString)
    .withProperty("fetch.max.wait.ms", "500")
    .withProperty("fetch.min.bytes", "1048576")
    .withProperty("enable.auto.commit", "true")
    .withProperty("auto.commit.interval.ms", "500")

  Consumer.committableSource(measurementConsumer, Subscriptions.topics("measurements"))
    .runForeach(msg => {
      process(msg.record.value)
      //msg.committableOffset.commitScaladsl()
    })

  def process(message: Sensor): Unit = {
    //inbox.send(network, message)
    network.tell(message, ActorRef.noSender)
  }

  def process(message: Measurement): Unit = {
    //inbox.send(network, message)
    network.tell(message, ActorRef.noSender)
  }
}

object NdwActorPerSensor {
  def main(args: Array[String]): Unit = {
    new NdwActorPerSensor()
  }
}
