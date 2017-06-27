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
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import nl.trivento.fastdata.ndw.processor.{Heat, LatLong}
import nl.trivento.fastdata.ndw.shared.serialization.TypedJsonDeserializer
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.mutable

object WebsocketsHeatmapServer {
  private implicit val actorSystem = ActorSystem.create()
  private implicit val materializer = ActorMaterializer.create(actorSystem)
  private val objectMapper = new ObjectMapper()
  objectMapper.registerModule(DefaultScalaModule)
  private val hub = actorSystem.actorOf(
    Props(
      new BufferingCachingTopic[Heat, LatLong](
        (heat: Heat) => heat.location,
        (heat: Heat) => objectMapper.writeValueAsString(heat),
        30000,
        25000
      )
    )
  )

  def start(): Unit = {
    val BUFFER_SIZE = 65536 * 16

    val consumerSettings = ConsumerSettings[String, Heat](actorSystem, new StringDeserializer(),
      new TypedJsonDeserializer[Heat](classOf[Heat]))
      .withProperty("auto.offset.reset", "earliest")
      .withBootstrapServers("master:9092")
      .withClientId(UUID.randomUUID().toString)
      .withGroupId("ndw_heatmaps_" + UUID.randomUUID.toString)
      .withProperty("fetch.max.wait.ms", "500")
      .withProperty("fetch.min.bytes", "1048576")
      .withProperty("enable.auto.commit", "true")
      .withProperty("auto.commit.interval.ms", "500")

    Consumer.plainSource(consumerSettings, Subscriptions.topics("heat"))
      .map(message => message.value())
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
          val subscriptionActor = actorSystem.actorOf(Props[Subscription[TextMessage]])

          val outgoingMessages: Source[TextMessage, NotUsed] =
            Source.actorRef[Array[String]](BUFFER_SIZE * 4, OverflowStrategy.dropTail)
              .mapMaterializedValue { outActor =>
                // give the user actor a way to send messages out
                subscriptionActor ! Subscribe(outActor, hub)
                NotUsed
              }
            .batch(BUFFER_SIZE, strings => strings)((result, append) => result ++ append)
            .map((batch) => {
              System.out.println("Sending batch of " + batch.length)
              TextMessage(batch.addString(StringBuilder.newBuilder, "[", ",", "]").toString())
            })

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

