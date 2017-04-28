package nl.trivento.fastdata.ndw.presentation

import java.util.UUID

import akka.NotUsed
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props, Terminated}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ws.TextMessage
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import nl.trivento.fastdata.ndw.processor.{Heat, LatLong}
import nl.trivento.fastdata.ndw.shared.serialization.JsonSerializer
import nl.trivento.fastdata.ndw.shared.serialization.TypedJsonDeserializer
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.mutable

case class AddSubscription()

case class Subscribe(listener: ActorRef, topic: ActorRef)

case class RemoveSubscription()

case class Mutation(heat: Heat)

class CachingTopic extends Actor with ActorLogging {
  private val cache = mutable.HashMap.empty[LatLong, Heat]
  private val subscriptions = mutable.HashSet.empty[ActorRef]

  override def preStart(): Unit = {
    super.preStart()
  }

  override def receive: Receive = {
    case AddSubscription =>
      context.watch(sender())
      log.info("New subscription, send " + cache.values.size + " heat points in cache");
      sender ! cache.values.toArray[Heat]
      subscriptions.add(sender())
    case RemoveSubscription =>
      subscriptions.remove(sender())
    case m: Mutation =>
      cache.put(m.heat.location, m.heat)
      subscriptions.foreach(_ ! Array(m.heat))
    case Terminated => subscriptions.remove(sender())
  }
}

class Subscription extends Actor with ActorLogging {
  private var listener: Option[ActorRef] = None

  override def receive: Receive = {
    case s: Subscribe =>
      log.info("Subscribing " + s.listener + " to " + s.topic)
      context.become(subscription)
      listener = Option(s.listener)
      s.topic ! AddSubscription
  }

  def subscription: Receive = {
    case h: Array[Heat] =>
      log.info("Forwarding " + h.length + " items from topic")
      listener.foreach(_ ! h)
  }
}

object WebsocketsHeatmapServer {
  private implicit val actorSystem = ActorSystem.create()
  private implicit val materializer = ActorMaterializer.create(actorSystem)
  private val objectMapper = new ObjectMapper()
  objectMapper.registerModule(DefaultScalaModule)
  private val hub = actorSystem.actorOf(Props[CachingTopic])

  def start(): Unit = {
    val consumerSettings = ConsumerSettings[String, Heat](actorSystem, new StringDeserializer(),
      new TypedJsonDeserializer[Heat](classOf[Heat]))
      .withProperty("auto.offset.reset", "earliest")
      .withBootstrapServers("master:9092")
      .withClientId(UUID.randomUUID().toString)
      .withGroupId("ndw_heatmaps_" + UUID.randomUUID.toString)

    Consumer.plainSource(consumerSettings, Subscriptions.topics("heat"))
      .map(message => Mutation(message.value()))
      .to(Sink.actorRef(hub, RemoveSubscription))
      .run()

    val route: Flow[HttpRequest, HttpResponse, NotUsed] = Route.handlerFlow(
      get {
        pathSingleSlash {
          getFromFile("src/main/resources/index.html")
        } ~
        pathPrefix("static") {
          path(Remaining) {
            tail => getFromResource("static/" + tail)
          }
        } ~
        path("listen") {
          val subscriptionActor = actorSystem.actorOf(Props[Subscription])

          val outgoingMessages: Source[TextMessage, NotUsed] =
            Source.actorRef[Array[Heat]](10, OverflowStrategy.fail)
              .mapMaterializedValue { outActor =>
                // give the user actor a way to send messages out
                subscriptionActor ! Subscribe(outActor, hub)
                NotUsed
              }.map(outMsg => TextMessage(objectMapper.writeValueAsString(outMsg)))

          handleWebSocketMessages(Flow.fromSinkAndSource(Sink.ignore, outgoingMessages))
        }
      }
    )
    Http().bindAndHandle(route, "localhost", 8888)
  }

  def main(args: Array[String]): Unit = {
    start()
  }

}

